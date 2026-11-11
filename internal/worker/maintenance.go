package worker

import (
	"context"
	"time"

	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// promoteLoop moves due retries from the delayed set into the stream.
func (w *Worker) promoteLoop(ctx context.Context) {
	every(ctx, 200*time.Millisecond, func() {
		for ctx.Err() == nil {
			n, err := w.q.PromoteDue(ctx, 500)
			if err != nil {
				if ctx.Err() == nil {
					w.log.Warn("promoting delayed deliveries", "err", err)
				}
				return
			}
			if n < 500 {
				return
			}
		}
	})
}

// sweepLoop is the safety net that makes Redis a cache of due work rather than its only record.
func (w *Worker) sweepLoop(ctx context.Context) {
	every(ctx, w.cfg.SweepInterval, func() { w.Sweep(ctx) })
}

// Sweep requeues work whose queue entry was lost: expired leases (a worker died mid-attempt), deliveries long
// overdue, events never routed, and stream entries a dead consumer never acknowledged.
func (w *Worker) Sweep(ctx context.Context) {
	if rows, err := w.store.ExpireLeases(ctx, 500); err == nil && len(rows) > 0 {
		items := make([]queue.Item, len(rows))
		for i, r := range rows {
			items[i] = queue.Item{Delivery: r.ID, Tenant: r.TenantID, Endpoint: r.EndpointID}
		}
		w.m.Requeued.WithLabelValues("expired_lease").Add(float64(len(items)))
		_ = w.q.EnqueueDeliveries(ctx, items...)
	}
	if rows, err := w.store.RequeueOverdue(ctx, db.RequeueOverdueParams{
		OlderThanSeconds: int32(w.cfg.SweepOverdue.Seconds()), Lim: 1000,
	}); err == nil && len(rows) > 0 {
		items := make([]queue.Item, len(rows))
		for i, r := range rows {
			items[i] = queue.Item{Delivery: r.ID, Tenant: r.TenantID, Endpoint: r.EndpointID}
		}
		w.m.Requeued.WithLabelValues("overdue").Add(float64(len(items)))
		_ = w.q.EnqueueDeliveries(ctx, items...)
	}
	if rows, err := w.store.ListUnroutedEvents(ctx, db.ListUnroutedEventsParams{OlderThanSeconds: int32(w.cfg.SweepUnrouted.Seconds()), Lim: 1000}); err == nil && len(rows) > 0 {
		refs := make([]queue.EventRef, len(rows))
		for i, r := range rows {
			refs[i] = queue.EventRef{Event: r.ID, Tenant: r.TenantID}
		}
		w.m.Requeued.WithLabelValues("unrouted").Add(float64(len(refs)))
		_ = w.q.EnqueueEvents(ctx, refs...)
	}
	minIdle := 2 * w.cfg.LeaseDuration
	if msgs, err := w.q.Reclaim(ctx, queue.EventsStream, queue.RouterGroup, w.consumer, minIdle, routeBatch); err == nil && len(msgs) > 0 {
		w.m.Requeued.WithLabelValues("reclaimed").Add(float64(len(msgs)))
		if err := w.route(ctx, msgs); err != nil {
			w.log.Warn("routing reclaimed events", "err", err)
		}
	}
	if msgs, err := w.q.Reclaim(ctx, queue.DeliveriesStream, queue.WorkerGroup, w.consumer, minIdle, 100); err == nil && len(msgs) > 0 {
		w.m.Requeued.WithLabelValues("reclaimed").Add(float64(len(msgs)))
		for _, m := range msgs {
			w.inflight.Add(1)
			go func() {
				defer w.inflight.Done()
				w.handle(context.WithoutCancel(ctx), m)
			}()
		}
	}
}

// unparkLoop wakes parked deliveries whose slots were freed without a wake-up (a worker died holding one).
func (w *Worker) unparkLoop(ctx context.Context) {
	every(ctx, 5*time.Second, func() {
		lots, err := w.q.ParkedLots(ctx)
		if err != nil {
			return
		}
		for _, p := range lots {
			l, err := w.limitsFor(ctx, p.First)
			if err != nil {
				continue
			}
			limit := l.EndpointInFlight
			if p.Kind == "t" {
				limit = l.TenantInFlight
			}
			if _, err := w.q.Unpark(ctx, p, limit); err != nil {
				w.log.Warn("unparking", "err", err)
			}
		}
	})
}

// receiptLoop asks Expo for the receipts of push tickets and deletes tokens of uninstalled apps.
func (w *Worker) receiptLoop(ctx context.Context) {
	every(ctx, time.Minute, func() { w.CheckReceipts(ctx) })
}

// CheckReceipts processes the push tickets that are due for a receipt check.
func (w *Worker) CheckReceipts(ctx context.Context) {
	due, err := w.store.DuePushTickets(ctx, 1000)
	if err != nil || len(due) == 0 {
		return
	}
	idsList := make([]string, len(due))
	for i, d := range due {
		idsList[i] = d.TicketID
	}
	receipts, err := w.expo.Receipts(ctx, idsList)
	if err != nil {
		w.log.Warn("fetching push receipts", "err", err)
		return
	}
	var mark db.MarkPushTicketsParams
	for _, d := range due {
		r, ok := receipts[d.TicketID]
		if !ok {
			if time.Since(d.CreatedAt) < 24*time.Hour {
				continue // not ready yet
			}
			mark.TicketIds, mark.Statuses, mark.Errors = append(mark.TicketIds, d.TicketID), append(mark.Statuses, "unknown"), append(mark.Errors, "")
			continue
		}
		mark.TicketIds = append(mark.TicketIds, d.TicketID)
		mark.Statuses = append(mark.Statuses, r.Status)
		mark.Errors = append(mark.Errors, r.Details.Error)
		if r.Details.Error == "DeviceNotRegistered" {
			n, err := w.store.DeletePushTokensByValue(ctx, db.DeletePushTokensByValueParams{TenantID: d.TenantID, Tokens: []string{d.Token}})
			if err == nil {
				w.m.PushTokensPruned.Add(float64(n))
			}
		}
	}
	if len(mark.TicketIds) > 0 {
		if err := w.store.MarkPushTickets(ctx, mark); err != nil {
			w.log.Warn("marking push receipts", "err", err)
		}
	}
}

// cleanupLoop releases idempotency keys after their window and deletes events past the retention period.
func (w *Worker) cleanupLoop(ctx context.Context) {
	every(ctx, 10*time.Minute, func() { w.Cleanup(ctx) })
}

// Cleanup runs one retention pass in small batches.
func (w *Worker) Cleanup(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := w.store.ReleaseIdempotencyKeys(ctx, db.ReleaseIdempotencyKeysParams{Before: time.Now().Add(-w.cfg.IdempotencyTTL), Lim: 5000})
		if err != nil || n < 5000 {
			break
		}
	}
	for ctx.Err() == nil {
		n, err := w.store.DeleteEventsBefore(ctx, db.DeleteEventsBeforeParams{Before: time.Now().Add(-w.cfg.Retention), Lim: 1000})
		if err != nil || n < 1000 {
			break
		}
	}
	_, _ = w.store.DeletePushTicketsBefore(ctx, time.Now().Add(-48*time.Hour))
}

// depthLoop publishes the Redis backlog sizes.
func (w *Worker) depthLoop(ctx context.Context) {
	every(ctx, 5*time.Second, func() {
		d, err := w.q.Depths(ctx)
		if err != nil {
			return
		}
		w.m.QueueDepth.WithLabelValues("events").Set(float64(d.Events))
		w.m.QueueDepth.WithLabelValues("deliveries").Set(float64(d.Deliveries))
		w.m.QueueDepth.WithLabelValues("delayed").Set(float64(d.Delayed))
		w.m.QueueDepth.WithLabelValues("parked").Set(float64(d.Parked))
	})
}
