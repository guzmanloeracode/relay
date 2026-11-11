package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/batch"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/observability"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/router"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// eventWriter is the API's group commit. Each request hands its event (with the deliveries routing planned for
// it) to one of a few flushers; a flusher takes every event waiting at that moment and stores them and their
// deliveries in one transaction, so one commit (and one WAL flush) covers many requests, then queues the
// deliveries. It never waits on purpose: with little traffic each batch holds one event, and under load batches
// grow by themselves, which keeps ingestion latency flat as the rate rises. A flusher that found only a few events
// waits up to writerLinger for more, so steady traffic is written in fewer, larger statements.
type eventWriter struct {
	st          *store.Store
	q           *queue.Queue
	log         *slog.Logger
	m           *observability.Metrics
	maxAttempts int32
	in          chan *pendingEvent
	max         int
	linger      time.Duration
}

type pendingEvent struct {
	p    newEvent
	plan []router.Planned
	done chan writeResult
}

type writeResult struct {
	id        uuid.UUID
	createdAt time.Time
	conflict  bool // the Idempotency-Key exists already: nothing was stored
	err       error
}

func newEventWriter(s *Server, flushers, maxBatch int) *eventWriter {
	return &eventWriter{st: s.store, q: s.q, log: s.log, m: s.m, maxAttempts: int32(s.cfg.RetryMaxAttempts),
		in: make(chan *pendingEvent, flushers*maxBatch), max: maxBatch, linger: writerLinger}
}

// run starts the flushers; they stop when ctx ends.
func (w *eventWriter) run(ctx context.Context, flushers int) {
	for range flushers {
		go func() {
			b := make([]*pendingEvent, 0, w.max)
			for {
				select {
				case <-ctx.Done():
					return
				case first := <-w.in:
					b = append(b[:0], first)
				}
				b = batch.Collect(w.in, b, w.max, w.linger)
				w.flush(context.WithoutCancel(ctx), b)
			}
		}()
	}
}

// write stores one event with its deliveries and waits for its batch to commit.
func (w *eventWriter) write(ctx context.Context, p newEvent, plan []router.Planned) writeResult {
	pe := &pendingEvent{p: p, plan: plan, done: make(chan writeResult, 1)}
	select {
	case w.in <- pe:
	case <-ctx.Done():
		return writeResult{err: ctx.Err()}
	}
	select {
	case r := <-pe.done:
		return r
	case <-ctx.Done():
		// The batch may still commit: a retry with the same Idempotency-Key then finds the event.
		return writeResult{err: ctx.Err()}
	}
}

func (w *eventWriter) flush(ctx context.Context, batch []*pendingEvent) {
	n := len(batch)
	ev := db.InsertEventsParams{
		Ids: make([]uuid.UUID, n), TenantIds: make([]uuid.UUID, n), Types: make([]string, n), Payloads: make([]string, n),
		Messages: make([]string, n), Targets: make([]string, n), IdemKeys: make([]string, n), RequestHashes: make([][]byte, n),
		DeliveryCounts: make([]int32, n),
	}
	for i, pe := range batch {
		p := pe.p
		ev.Ids[i], ev.TenantIds[i], ev.Types[i], ev.Payloads[i] = p.ID, p.TenantID, p.Type, string(p.Payload)
		ev.Messages[i], ev.Targets[i], ev.RequestHashes[i] = string(p.Message), string(p.Targets), p.RequestHash
		ev.DeliveryCounts[i] = int32(len(pe.plan))
		if p.IdempotencyKey != nil {
			ev.IdemKeys[i] = *p.IdempotencyKey
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	stored := make(map[uuid.UUID]time.Time, n)
	var created []db.InsertDeliveriesRow
	channelOf := map[uuid.UUID]string{}
	err := w.st.Tx(ctx, func(q *db.Queries) error {
		rows, err := q.InsertEvents(ctx, ev)
		if err != nil {
			return err
		}
		for _, r := range rows {
			stored[r.ID] = r.CreatedAt
		}
		d := db.InsertDeliveriesParams{MaxAttempts: w.maxAttempts}
		for _, pe := range batch {
			if _, ok := stored[pe.p.ID]; !ok {
				continue // an Idempotency-Key seen before: its event was routed when it was stored
			}
			for _, pl := range pe.plan {
				id := ids.New()
				d.Ids = append(d.Ids, id)
				d.TenantIds = append(d.TenantIds, pe.p.TenantID)
				d.EventIds = append(d.EventIds, pe.p.ID)
				d.EndpointIds = append(d.EndpointIds, pl.EndpointID)
				d.Channels = append(d.Channels, pl.Channel)
				d.Recipients = append(d.Recipients, pl.Recipient)
				channelOf[id] = pl.Channel
			}
		}
		if len(d.Ids) > 0 {
			created, err = q.InsertDeliveries(ctx, d)
		}
		return err
	})
	if err != nil {
		w.log.Error("storing events", "err", err, "count", n)
		clear(stored)
	} else {
		items := make([]queue.Item, len(created))
		for i, c := range created {
			items[i] = queue.Item{Delivery: c.ID, Tenant: c.TenantID, Endpoint: c.EndpointID}
			w.m.DeliveriesRouted.WithLabelValues(channelOf[c.ID], "route").Inc()
		}
		w.m.EventsRouted.Add(float64(len(stored)))
		if err := w.q.EnqueueDeliveries(ctx, items...); err != nil {
			// Stored but not queued: the worker's sweeper finds them once they are overdue.
			w.log.Warn("queueing deliveries failed; the sweeper will pick them up", "err", err, "count", len(items))
		}
	}
	for _, pe := range batch {
		res := writeResult{id: pe.p.ID, err: err}
		if err == nil {
			created, ok := stored[pe.p.ID]
			res.createdAt, res.conflict = created, !ok
		}
		pe.done <- res
	}
}
