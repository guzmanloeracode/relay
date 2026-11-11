package worker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/observability"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

const maxReadBatch = 100

// deliverLoop reads the delivery stream only as fast as it has free attempt slots, so a slow receiver
// never makes this process pull more work than it can hold.
func (w *Worker) deliverLoop(ctx, workCtx context.Context) {
	slots := make(chan struct{}, w.cfg.WorkerConcurrency)
	for ctx.Err() == nil {
		// Wait for one free slot, then take any others that are free right now.
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		n := 1
	fill:
		for n < maxReadBatch {
			select {
			case slots <- struct{}{}:
				n++
			default:
				break fill
			}
		}
		msgs, err := w.q.Read(ctx, queue.DeliveriesStream, queue.WorkerGroup, w.consumer, n, time.Second)
		if err != nil && ctx.Err() == nil {
			w.log.Error("reading deliveries stream", "err", err)
			pause(ctx, time.Second)
		}
		for range n - len(msgs) {
			<-slots
		}
		for _, m := range msgs {
			w.inflight.Add(1)
			go func() {
				defer func() { <-slots; w.inflight.Done() }()
				w.handle(workCtx, m)
			}()
		}
	}
}

// handle runs one stream message: take the limits, claim the delivery, attempt it, record the result.
func (w *Worker) handle(ctx context.Context, m queue.Message) {
	ack := func() {
		if err := w.q.Ack(ctx, queue.DeliveriesStream, queue.WorkerGroup, m.ID); err != nil {
			w.log.Warn("ack failed", "err", err)
		}
	}
	it, err := queue.ParseItem(m.Value)
	if err != nil {
		w.log.Warn("dropping malformed delivery message", "value", m.Value)
		ack()
		return
	}
	limits, err := w.limitsFor(ctx, it)
	if store.IsNotFound(err) { // the endpoint (and its deliveries) was deleted
		ack()
		return
	}
	if err != nil {
		w.log.Error("loading limits", "err", err)
		return // not acknowledged: retried after the idle timeout
	}
	verdict, err := w.q.Acquire(ctx, it, limits, w.cfg.LeaseDuration)
	if err != nil {
		w.log.Error("acquiring delivery slot", "err", err)
		return
	}
	if verdict != queue.Acquired {
		// Parked or delayed inside Redis already (or a duplicate of a running attempt).
		w.m.Deferred.WithLabelValues(verdict.String()).Inc()
		if verdict == queue.RateLimited {
			w.m.RateLimited.WithLabelValues("delivery").Inc()
		}
		ack()
		return
	}
	defer func() {
		if err := w.q.Release(context.WithoutCancel(ctx), it); err != nil {
			w.log.Warn("releasing delivery slot", "err", err)
		}
	}()

	row, err := w.store.ClaimDelivery(ctx, db.ClaimDeliveryParams{ID: it.Delivery, LeaseSeconds: int32(w.cfg.LeaseDuration.Seconds())})
	if store.IsNotFound(err) {
		// Done already, leased elsewhere, not due yet, or its endpoint was disabled meanwhile.
		if n, err := w.store.DeadLetterIfEndpointDisabled(ctx, it.Delivery); err == nil && n > 0 {
			w.m.DeadLettered.WithLabelValues("endpoint_disabled").Inc()
		}
		ack()
		return
	}
	if err != nil {
		w.log.Error("claiming delivery", "err", err)
		return
	}
	w.attempt(ctx, it, row)
	ack()
}

func (w *Worker) limitsFor(ctx context.Context, it queue.Item) (queue.Limits, error) {
	return w.limits.GetOrLoad([2]uuid.UUID{it.Tenant, it.Endpoint}, func() (queue.Limits, error) {
		r, err := w.store.GetDeliveryLimits(ctx, db.GetDeliveryLimitsParams{TenantID: it.Tenant, EndpointID: it.Endpoint})
		if err != nil {
			return queue.Limits{}, err
		}
		rate := int(r.DeliveryRate)
		return queue.Limits{
			EndpointInFlight: int(r.EndpointMaxInFlight),
			TenantInFlight:   int(r.TenantMaxInFlight),
			Rate:             queue.Bucket{Rate: rate, Burst: max(rate, 1)},
		}, nil
	})
}

// attempt sends a claimed delivery and records the outcome.
func (w *Worker) attempt(ctx context.Context, it queue.Item, row db.ClaimDeliveryRow) {
	ctx, span := observability.Tracer.Start(ctx, "relay.deliver")
	defer span.End()
	span.SetAttributes(
		attribute.String("relay.delivery_id", ids.Encode(ids.Delivery, row.ID)),
		attribute.String("relay.event_id", ids.Encode(ids.Event, row.EventID)),
		attribute.String("relay.channel", row.Channel),
		attribute.Int("relay.attempt", int(row.Attempts)),
	)
	w.m.InFlight.Inc()
	defer w.m.InFlight.Dec()

	job, res := w.job(row)
	started := time.Now()
	if res == nil {
		sender := w.senders[row.Channel]
		attemptCtx, cancel := context.WithTimeout(ctx, w.cfg.LeaseDuration-5*time.Second)
		r := sender.Send(attemptCtx, job)
		cancel()
		res = &r
	}
	latency := time.Since(started)
	w.m.AttemptDuration.WithLabelValues(row.Channel).Observe(latency.Seconds())
	if res.Outcome != channels.Delivered {
		span.SetStatus(codes.Error, res.Error)
	}
	w.record(ctx, it, row, *res, started, latency)
}

// job builds the channel's input. A non-nil result means the delivery can't be attempted at all.
func (w *Worker) job(row db.ClaimDeliveryRow) (*channels.Job, *channels.Result) {
	j := &channels.Job{
		DeliveryID: row.ID, EventID: row.EventID, TenantID: row.TenantID, EndpointID: row.EndpointID,
		TenantSlug: row.TenantSlug, EventType: row.EventType, EventCreatedAt: row.EventCreatedAt,
		Payload: row.Payload, Attempt: int(row.Attempts), Recipient: row.Recipient,
	}
	if row.Attempts > row.MaxAttempts { // a lease expired on the last attempt
		return j, &channels.Result{Outcome: channels.Dead, Reason: "max_attempts", Error: "no attempts left"}
	}
	if len(row.Message) > 0 && string(row.Message) != "null" {
		var msg channels.Message
		if err := json.Unmarshal(row.Message, &msg); err == nil {
			j.Message = &msg
		}
	}
	if row.Channel == channels.Webhook {
		if row.Url != nil {
			j.URL = *row.Url
		}
		secret, err := w.box.Open(row.SecretEnc)
		if err != nil {
			return j, &channels.Result{Outcome: channels.Retry, Error: "cannot decrypt the signing secret (RELAY_SECRET_KEY changed?)"}
		}
		j.Secrets = []string{string(secret)}
		if row.PreviousSecretExpiresAt != nil && time.Now().Before(*row.PreviousSecretExpiresAt) {
			if prev, err := w.box.Open(row.PreviousSecretEnc); err == nil {
				j.Secrets = append(j.Secrets, string(prev))
			}
		}
	}
	return j, nil
}

// record stores the attempt and decides what happens next: done, retry later, or the dead-letter queue.
func (w *Worker) record(ctx context.Context, it queue.Item, row db.ClaimDeliveryRow, res channels.Result, started time.Time, latency time.Duration) {
	outcome := res.Outcome
	reason := res.Reason
	next := time.Now()
	if outcome == channels.Retry {
		if int(row.Attempts) >= int(row.MaxAttempts) {
			outcome, reason = channels.Dead, "max_attempts"
		} else {
			wait := Backoff(int(row.Attempts), w.cfg.RetryBase, w.cfg.RetryMaxDelay)
			if res.RetryAfter > wait {
				wait = min(res.RetryAfter, w.cfg.RetryMaxDelay)
			}
			next = next.Add(wait)
		}
	}
	p := db.RecordAttemptParams{
		ID: row.ID, Outcome: outcome.String(), NextAttemptAt: next, StartedAt: started,
		LatencyMs: int32(latency.Milliseconds()), StatusCode: nonZero(res.StatusCode),
		Error: nonEmpty(res.Error), ResponseBody: nonEmpty(res.ResponseBody),
	}
	if outcome == channels.Dead || outcome == channels.Skipped {
		p.DeadReason = nonEmpty(reason)
	}
	if _, err := w.store.RecordAttempt(ctx, p); err != nil {
		// The lease expires and the sweeper makes the delivery due again: at least once, never lost.
		w.log.Error("recording attempt", "err", err, "delivery_id", ids.Encode(ids.Delivery, row.ID))
		return
	}
	w.m.Attempts.WithLabelValues(row.Channel, outcome.String()).Inc()

	switch outcome {
	case channels.Delivered:
		w.m.DeliveryLag.WithLabelValues(row.Channel).Observe(time.Since(row.EventCreatedAt).Seconds())
		if row.Channel == channels.Webhook {
			_ = w.store.RecordEndpointSuccess(ctx, row.EndpointID)
		}
	case channels.Retry:
		if err := w.q.Schedule(ctx, it, next); err != nil {
			w.log.Warn("scheduling retry failed; the sweeper will pick it up", "err", err)
		}
	case channels.Dead:
		w.m.DeadLettered.WithLabelValues(reason).Inc()
	}
	if res.DisableEndpoint != "" {
		w.disableEndpoint(ctx, row, res.DisableEndpoint, "gone")
	} else if res.EndpointFailure && row.Channel == channels.Webhook {
		w.countFailure(ctx, row)
	}
}

// countFailure extends the endpoint's failure streak and disables it once the streak is long enough.
func (w *Worker) countFailure(ctx context.Context, row db.ClaimDeliveryRow) {
	f, err := w.store.RecordEndpointFailure(ctx, row.EndpointID)
	if err != nil || f.Status != "active" || f.FailingSince == nil {
		return
	}
	if int(f.FailureStreak) >= w.cfg.EndpointDisableAfter && time.Since(*f.FailingSince) >= w.cfg.EndpointDisableMinAge {
		w.disableEndpoint(ctx, row, "every attempt failed since "+f.FailingSince.UTC().Format(time.RFC3339), "failing")
	}
}

func (w *Worker) disableEndpoint(ctx context.Context, row db.ClaimDeliveryRow, why, metric string) {
	ep, err := w.store.DisableEndpoint(ctx, db.DisableEndpointParams{ID: row.EndpointID, Reason: &why})
	if store.IsNotFound(err) {
		return // already disabled
	}
	if err != nil {
		w.log.Error("disabling endpoint", "err", err)
		return
	}
	n, _ := w.store.DeadLetterEndpointBacklog(ctx, db.DeadLetterEndpointBacklogParams{EndpointID: ep.ID, Reason: new("endpoint_disabled")})
	w.m.EndpointDisabled.WithLabelValues(metric).Inc()
	w.m.DeadLettered.WithLabelValues("endpoint_disabled").Add(float64(n))
	_ = w.q.Invalidate(ctx, "routes:"+row.TenantID.String())
	w.log.Warn("endpoint disabled", "endpoint_id", ids.Encode(ids.Endpoint, ep.ID), "tenant", row.TenantSlug,
		"reason", why, "dead_lettered", n)
}

func nonZero(n int) *int32 {
	if n == 0 {
		return nil
	}
	v := int32(n)
	return &v
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	if len(s) > 2000 {
		s = s[:2000]
	}
	return &s
}
