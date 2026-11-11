package worker

import (
	"context"
	"encoding/json"
	"strings"
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

const (
	maxReadBatch = 100
	smallRead    = 32
	readLinger   = 10 * time.Millisecond
)

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
		if len(msgs) > 0 {
			w.inflight.Add(1)
			go func() {
				defer w.inflight.Done()
				w.dispatch(workCtx, msgs, slots)
			}()
		}
		if len(msgs) > 0 && len(msgs) < smallRead {
			pause(ctx, readLinger) // steady traffic: let the next read gather a bigger batch
		}
	}
}

// dispatch prepares a batch of stream messages together: one pipelined round trip to Redis for the limits,
// one query for the deliveries. Each delivery that may go now is attempted on its own goroutine; every message
// frees its worker slot when it is done.
func (w *Worker) dispatch(ctx context.Context, msgs []queue.Message, slots chan struct{}) {
	done := func(m queue.Message, ack bool) {
		if ack {
			if err := w.q.Ack(ctx, queue.DeliveriesStream, queue.WorkerGroup, m.ID); err != nil {
				w.log.Warn("ack failed", "err", err)
			}
		}
		<-slots
	}
	type entry struct {
		msg queue.Message
		it  queue.Item
	}
	var ready []entry
	var limits []queue.Limits
	for _, m := range msgs {
		it, err := queue.ParseItem(m.Value)
		if err != nil {
			w.log.Warn("dropping malformed delivery message", "value", m.Value)
			done(m, true)
			continue
		}
		l, err := w.limitsFor(ctx, it)
		if store.IsNotFound(err) { // the endpoint (and its deliveries) was deleted
			done(m, true)
			continue
		}
		if err != nil {
			w.log.Error("loading limits", "err", err)
			done(m, false) // not acknowledged: taken over again after the lease
			continue
		}
		ready = append(ready, entry{m, it})
		limits = append(limits, l)
	}
	if len(ready) == 0 {
		return
	}
	items := make([]queue.Item, len(ready))
	for i, e := range ready {
		items[i] = e.it
	}
	verdicts, err := w.q.AcquireMany(ctx, items, limits, w.cfg.LeaseDuration)
	if err != nil {
		w.log.Error("acquiring delivery slots", "err", err)
		for _, e := range ready {
			done(e.msg, false)
		}
		return
	}
	var acquired []entry
	for i, v := range verdicts {
		if v == queue.Acquired {
			acquired = append(acquired, ready[i])
			continue
		}
		// Parked or delayed inside Redis already (or a duplicate of a running attempt).
		w.m.Deferred.WithLabelValues(v.String()).Inc()
		if v == queue.RateLimited {
			w.m.RateLimited.WithLabelValues("delivery").Inc()
		}
		done(ready[i].msg, true)
	}
	if len(acquired) == 0 {
		return
	}
	release := func(it queue.Item) {
		if err := w.q.Release(context.WithoutCancel(ctx), it); err != nil {
			w.log.Warn("releasing delivery slot", "err", err)
		}
	}
	idList := make([]uuid.UUID, len(acquired))
	for i, e := range acquired {
		idList[i] = e.it.Delivery
	}
	rows, err := w.store.LoadDeliveries(ctx, idList)
	if err != nil {
		w.log.Error("loading deliveries", "err", err)
		for _, e := range acquired {
			release(e.it)
			done(e.msg, false)
		}
		return
	}
	byID := make(map[uuid.UUID]db.LoadDeliveriesRow, len(rows))
	for _, r := range rows {
		byID[r.ID] = r
	}
	for _, e := range acquired {
		row, ok := byID[e.it.Delivery]
		if !ok {
			// Done already, not due yet (a stale duplicate), or its endpoint was disabled meanwhile.
			if n, err := w.store.DeadLetterIfEndpointDisabled(ctx, e.it.Delivery); err == nil && n > 0 {
				w.m.DeadLettered.WithLabelValues("endpoint_disabled").Inc()
			}
			release(e.it)
			done(e.msg, true)
			continue
		}
		w.inflight.Add(1)
		go func() {
			defer w.inflight.Done()
			recorded := w.attempt(ctx, e.it, row)
			if err := w.q.Finish(context.WithoutCancel(ctx), e.it, e.msg.ID, recorded); err != nil {
				w.log.Warn("releasing delivery slot", "err", err)
			}
			<-slots
		}()
	}
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

// attempt sends a loaded delivery and records the outcome. It reports whether the outcome is stored (or
// another worker stored this attempt first); if not, the message stays unacknowledged and is retried.
func (w *Worker) attempt(ctx context.Context, it queue.Item, row db.LoadDeliveriesRow) bool {
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
		// The attempt must end before the slot's lease does, or another worker could start a second one.
		attemptCtx, cancel := context.WithTimeout(ctx, w.cfg.LeaseDuration*9/10)
		r := sender.Send(attemptCtx, job)
		cancel()
		res = &r
	}
	latency := time.Since(started)
	w.m.AttemptDuration.WithLabelValues(row.Channel).Observe(latency.Seconds())
	if res.Outcome != channels.Delivered {
		span.SetStatus(codes.Error, res.Error)
	}
	return w.record(ctx, it, row, *res, started, latency)
}

// job builds the channel's input. A non-nil result means the delivery can't be attempted at all.
func (w *Worker) job(row db.LoadDeliveriesRow) (*channels.Job, *channels.Result) {
	j := &channels.Job{
		DeliveryID: row.ID, EventID: row.EventID, TenantID: row.TenantID, EndpointID: row.EndpointID,
		TenantSlug: row.TenantSlug, EventType: row.EventType, EventCreatedAt: row.EventCreatedAt,
		Payload: row.Payload, Attempt: int(row.Attempts) + 1, Recipient: row.Recipient,
	}
	if row.Attempts >= row.MaxAttempts {
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
func (w *Worker) record(ctx context.Context, it queue.Item, row db.LoadDeliveriesRow, res channels.Result, started time.Time, latency time.Duration) bool {
	outcome := res.Outcome
	reason := res.Reason
	next := time.Now()
	attempt := int(row.Attempts) + 1
	if outcome == channels.Retry {
		if attempt >= int(row.MaxAttempts) {
			outcome, reason = channels.Dead, "max_attempts"
		} else {
			wait := Backoff(attempt, w.cfg.RetryBase, w.cfg.RetryMaxDelay)
			if res.RetryAfter > wait {
				wait = min(res.RetryAfter, w.cfg.RetryMaxDelay)
			}
			next = next.Add(wait)
		}
	}
	p := &pendingRecord{
		id: row.ID, seen: row.Attempts, outcome: outcome.String(), next: next, started: started,
		latencyMs: int32(latency.Milliseconds()), status: int32(res.StatusCode),
		err: truncate(res.Error), body: truncate(res.ResponseBody),
	}
	if outcome == channels.Dead || outcome == channels.Skipped {
		p.reason = reason
	}
	r := w.recorder.record(p)
	if r.err != nil {
		// Not recorded: the delivery is still pending and its message unacknowledged, so it is attempted
		// again (at least once, never lost).
		return false
	}
	if !r.applied {
		return true // another worker recorded this attempt first (a duplicate after a slot's lease expired)
	}
	w.m.Attempts.WithLabelValues(row.Channel, outcome.String()).Inc()

	switch outcome {
	case channels.Delivered:
		w.m.DeliveryLag.WithLabelValues(row.Channel).Observe(time.Since(row.EventCreatedAt).Seconds())
		if _, healthy := w.healthy.Get(row.EndpointID); !healthy && row.Channel == channels.Webhook {
			if w.store.RecordEndpointSuccess(ctx, row.EndpointID) == nil {
				w.healthy.Set(row.EndpointID, true) // skip the write while the endpoint keeps succeeding
			}
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
	return true
}

// countFailure extends the endpoint's failure streak and disables it once the streak is long enough.
func (w *Worker) countFailure(ctx context.Context, row db.LoadDeliveriesRow) {
	w.healthy.Delete(row.EndpointID)
	f, err := w.store.RecordEndpointFailure(ctx, row.EndpointID)
	if err != nil || f.Status != "active" || f.FailingSince == nil {
		return
	}
	if int(f.FailureStreak) >= w.cfg.EndpointDisableAfter && time.Since(*f.FailingSince) >= w.cfg.EndpointDisableMinAge {
		w.disableEndpoint(ctx, row, "every attempt failed since "+f.FailingSince.UTC().Format(time.RFC3339), "failing")
	}
}

func (w *Worker) disableEndpoint(ctx context.Context, row db.LoadDeliveriesRow, why, metric string) {
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

// truncate keeps the stored error or response text short.
func truncate(s string) string {
	if len(s) > 2000 {
		return strings.ToValidUTF8(s[:2000], "")
	}
	return s
}
