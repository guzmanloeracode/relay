package worker

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/observability"
	"github.com/guzmanloeracode/relay/internal/queue"
	"github.com/guzmanloeracode/relay/internal/router"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

const routeBatch = 200

func (w *Worker) routeLoop(ctx context.Context) {
	for ctx.Err() == nil {
		msgs, err := w.q.Read(ctx, queue.EventsStream, queue.RouterGroup, w.consumer, routeBatch, time.Second)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("reading events stream", "err", err)
				pause(ctx, time.Second)
			}
			continue
		}
		if len(msgs) == 0 {
			continue
		}
		if err := w.route(context.WithoutCancel(ctx), msgs); err != nil {
			// Not acknowledged: the entries are claimed again after the idle timeout (and the sweeper finds
			// unrouted events in PostgreSQL anyway).
			w.log.Error("routing events", "err", err, "count", len(msgs))
			pause(ctx, time.Second)
		}
	}
}

// route turns a batch of events into deliveries in one transaction, then queues the deliveries.
func (w *Worker) route(ctx context.Context, msgs []queue.Message) error {
	ctx, span := observability.Tracer.Start(ctx, "relay.route")
	defer span.End()
	span.SetAttributes(attribute.Int("relay.events", len(msgs)))

	msgIDs := make([]string, 0, len(msgs))
	eventIDs := make([]uuid.UUID, 0, len(msgs))
	for _, m := range msgs {
		msgIDs = append(msgIDs, m.ID)
		ref, err := queue.ParseEventRef(m.Value)
		if err != nil {
			w.log.Warn("dropping malformed event message", "value", m.Value)
			continue
		}
		eventIDs = append(eventIDs, ref.Event)
	}
	events, err := w.store.GetEventsToRoute(ctx, eventIDs)
	if err != nil {
		return err
	}

	var p db.InsertDeliveriesParams
	p.MaxAttempts = int32(w.cfg.RetryMaxAttempts)
	routed := make([]uuid.UUID, 0, len(events))
	counts := make([]int32, 0, len(events))
	for _, ev := range events {
		routes, err := w.tenantRoutes(ctx, ev.TenantID)
		if err != nil {
			return err
		}
		var targets router.Targets
		if len(ev.Targets) > 0 {
			_ = json.Unmarshal(ev.Targets, &targets)
		}
		var only map[uuid.UUID]bool
		if len(targets.Endpoints) > 0 {
			only = map[uuid.UUID]bool{}
			for _, s := range targets.Endpoints {
				if id, err := ids.Decode(ids.Endpoint, s); err == nil {
					only[id] = true
				}
			}
		}
		hasMessage := len(ev.Message) > 0 && string(ev.Message) != "null"
		plan, skips := router.Plan(ev.Type, hasMessage, targets, only, routes)
		for _, s := range skips {
			w.m.RoutingSkipped.WithLabelValues(s.Channel, s.Reason).Inc()
		}
		for _, d := range plan {
			p.Ids = append(p.Ids, ids.New())
			p.TenantIds = append(p.TenantIds, ev.TenantID)
			p.EventIds = append(p.EventIds, ev.ID)
			p.EndpointIds = append(p.EndpointIds, d.EndpointID)
			p.Channels = append(p.Channels, d.Channel)
			p.Recipients = append(p.Recipients, d.Recipient)
		}
		routed = append(routed, ev.ID)
		counts = append(counts, int32(len(plan)))
	}

	var created []db.InsertDeliveriesRow
	if len(routed) > 0 {
		err = w.store.Tx(ctx, func(q *db.Queries) error {
			if len(p.Ids) > 0 {
				if created, err = q.InsertDeliveries(ctx, p); err != nil {
					return err
				}
			}
			return q.MarkEventsRouted(ctx, db.MarkEventsRoutedParams{Ids: routed, Counts: counts})
		})
		if err != nil {
			return err
		}
	}
	items := make([]queue.Item, len(created))
	for i, c := range created {
		items[i] = queue.Item{Delivery: c.ID, Tenant: c.TenantID, Endpoint: c.EndpointID}
	}
	channelOf := make(map[uuid.UUID]string, len(p.Ids))
	for i, id := range p.Ids {
		channelOf[id] = p.Channels[i]
	}
	for _, c := range created {
		w.m.DeliveriesRouted.WithLabelValues(channelOf[c.ID], "route").Inc()
	}
	if err := w.q.EnqueueDeliveries(ctx, items...); err != nil {
		w.log.Warn("queueing deliveries failed; the sweeper will pick them up", "err", err, "count", len(items))
	}
	w.m.EventsRouted.Add(float64(len(routed)))
	return w.q.Ack(ctx, queue.EventsStream, queue.RouterGroup, msgIDs...)
}

func (w *Worker) tenantRoutes(ctx context.Context, tenant uuid.UUID) ([]router.Route, error) {
	return w.routes.GetOrLoad(tenant, func() ([]router.Route, error) {
		rows, err := w.store.ListRoutes(ctx, tenant)
		if err != nil {
			return nil, err
		}
		routes := make([]router.Route, len(rows))
		for i, r := range rows {
			routes[i] = router.Route{EndpointID: r.EndpointID, Channel: r.Channel, EmailTo: r.EmailTo, Patterns: r.EventTypes}
		}
		return router.Merge(routes), nil
	})
}
