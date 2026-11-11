// Package worker attempts deliveries under the tenants' limits, retries them, and keeps the queues honest
// (sweeper, parking lots, push receipts, retention). Routing happens in the API, when an event is stored.
//
// Any number of worker processes can run against the same PostgreSQL and Redis: a Redis consumer group splits
// the stream between them, a delivery is attempted only by the process holding its slot, and the limits live in
// Redis.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/cache"
	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/channels/email"
	"github.com/guzmanloeracode/relay/internal/channels/push"
	"github.com/guzmanloeracode/relay/internal/channels/webhook"
	"github.com/guzmanloeracode/relay/internal/config"
	"github.com/guzmanloeracode/relay/internal/crypto"
	"github.com/guzmanloeracode/relay/internal/observability"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/store"
)

// Worker is one worker process.
type Worker struct {
	cfg      config.Config
	store    *store.Store
	q        *queue.Queue
	log      *slog.Logger
	m        *observability.Metrics
	box      *crypto.Box
	senders  map[string]channels.Sender
	expo     *push.Expo
	consumer string

	limits *cache.Cache[[2]uuid.UUID, queue.Limits]
	// Webhook endpoints whose failure streak is known to be zero (saves a write per delivered webhook).
	healthy *cache.Cache[uuid.UUID, bool]

	recorder *recorder

	inflight sync.WaitGroup
}

// The recorder's flushers write attempt results in parallel batches of up to recorderBatch.
const (
	recorderFlushers = 2
	recorderBatch    = 256
	recorderLinger   = 5 * time.Millisecond
)

// Options lets tests replace the channel senders.
type Options struct {
	Senders map[string]channels.Sender
}

// New builds a worker from the configuration.
func New(cfg config.Config, st *store.Store, q *queue.Queue, log *slog.Logger, m *observability.Metrics, opts Options) (*Worker, error) {
	box, err := crypto.NewBox(cfg.SecretKey)
	if err != nil {
		return nil, err
	}
	host, _ := os.Hostname()
	w := &Worker{
		cfg: cfg, store: st, q: q, log: log, m: m, box: box,
		consumer: fmt.Sprintf("%s-%d", host, os.Getpid()),
		limits:   cache.New[[2]uuid.UUID, queue.Limits](15*time.Second, 100_000),
		healthy:  cache.New[uuid.UUID, bool](time.Minute, 100_000),
	}
	w.expo = &push.Expo{
		SendURL: cfg.ExpoPushURL, ReceiptsURL: cfg.ExpoReceiptURL, AccessToken: cfg.ExpoToken,
		Client: &http.Client{Timeout: 15 * time.Second},
	}
	mailer, err := newMailer(cfg, log)
	if err != nil {
		return nil, err
	}
	w.senders = map[string]channels.Sender{
		channels.Webhook: webhook.New(cfg.WebhookTimeout, cfg.AllowPrivateNetworks, "Relay/"+cfg.Version+" (+https://github.com/guzmanloeracode/relay)"),
		channels.Email:   email.NewSender(mailer, cfg.EmailFrom),
		channels.Push: &push.Sender{Store: st.Queries, Expo: w.expo, ReceiptDelay: cfg.PushReceiptAge,
			OnPruned: func(n int) { m.PushTokensPruned.Add(float64(n)) }},
	}
	w.recorder = newRecorder(st, log, recorderFlushers, recorderBatch)
	for ch, s := range opts.Senders {
		w.senders[ch] = s
	}
	return w, nil
}

func newMailer(cfg config.Config, log *slog.Logger) (email.Mailer, error) {
	switch cfg.EmailProvider {
	case "resend":
		return email.Resend{APIKey: cfg.ResendAPIKey, BaseURL: cfg.ResendURL}, nil
	case "smtp":
		return email.NewSMTP(cfg.SMTPURL)
	default:
		return email.LogMailer{Log: log}, nil
	}
}

// Run works until ctx is cancelled, then stops taking new work and waits up to ShutdownTimeout for the
// attempts in progress to finish.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.q.Setup(ctx); err != nil {
		if ctx.Err() != nil {
			return nil // stopped before it started
		}
		return err
	}
	// Attempts keep their own context: cancelling ctx stops new work, not the requests already on the wire.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	w.recorder.run(workCtx, recorderFlushers)

	var loops sync.WaitGroup
	start := func(name string, fn func(context.Context)) {
		loops.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					w.log.Error("worker loop panicked", "loop", name, "panic", r)
				}
			}()
			fn(ctx)
		})
	}
	go w.listenInvalidations(ctx)
	start("deliver", func(ctx context.Context) { w.deliverLoop(ctx, workCtx) })
	start("promote", w.promoteLoop)
	start("sweep", w.sweepLoop)
	start("unpark", w.unparkLoop)
	start("receipts", w.receiptLoop)
	start("cleanup", w.cleanupLoop)
	start("depth", w.depthLoop)
	w.log.Info("worker started", "consumer", w.consumer, "concurrency", w.cfg.WorkerConcurrency)

	<-ctx.Done()
	loops.Wait()
	done := make(chan struct{})
	go func() { w.inflight.Wait(); close(done) }()
	select {
	case <-done:
		w.log.Info("worker drained")
	case <-time.After(w.cfg.ShutdownTimeout):
		w.log.Warn("shutdown timeout: abandoning attempts in progress (their leases expire and they run again)")
		cancelWork()
	}
	return nil
}

func (w *Worker) listenInvalidations(ctx context.Context) {
	for msg := range w.q.Invalidations(ctx) {
		kind, id, _ := strings.Cut(msg, ":")
		uid, err := uuid.Parse(id)
		if err != nil {
			continue
		}
		switch kind {
		case "tenant":
			w.limits.DeleteFunc(func(k [2]uuid.UUID, _ queue.Limits) bool { return k[0] == uid })
		case "endpoint":
			w.limits.DeleteFunc(func(k [2]uuid.UUID, _ queue.Limits) bool { return k[1] == uid })
			w.healthy.Delete(uid)
		}
	}
}

// every runs fn every d until ctx ends.
func every(ctx context.Context, d time.Duration, fn func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn()
		}
	}
}

// pause waits d or until ctx ends.
func pause(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
