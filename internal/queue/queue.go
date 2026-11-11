// Package queue holds Relay's Redis side: a stream with a consumer group (deliveries due now), a sorted set of
// delayed retries, per-tenant token buckets, and per-endpoint and per-tenant concurrency slots with parking
// lots.
//
// PostgreSQL stays the source of truth. Redis only carries IDs of work that is due, so anything lost here
// (a crash, a flushed Redis) is found again by the worker's sweeper. See docs/adr/001-redis-streams.md.
package queue

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Redis keys. Everything lives under "relay:" so the instance can share a Redis server.
const (
	DeliveriesStream = "relay:deliveries"
	WorkerGroup      = "workers"
	delayedKey       = "relay:delayed"
	parkedIndexKey   = "relay:parked"
	invalidateChan   = "relay:invalidate"
	itemField        = "i"
)

// Item identifies one delivery and what its limits depend on.
type Item struct {
	Delivery, Tenant, Endpoint uuid.UUID
}

func (it Item) String() string {
	return it.Delivery.String() + " " + it.Tenant.String() + " " + it.Endpoint.String()
}

// ParseItem reads Item.String.
func ParseItem(s string) (Item, error) {
	parts := strings.Fields(s)
	if len(parts) != 3 {
		return Item{}, fmt.Errorf("queue: bad item %q", s)
	}
	var it Item
	var err error
	for i, dst := range []*uuid.UUID{&it.Delivery, &it.Tenant, &it.Endpoint} {
		if *dst, err = uuid.Parse(parts[i]); err != nil {
			return Item{}, fmt.Errorf("queue: bad item %q", s)
		}
	}
	return it, nil
}

func semKey(kind string, id uuid.UUID) string  { return "relay:sem:" + kind + ":" + id.String() }
func parkKey(kind string, id uuid.UUID) string { return "relay:park:" + kind + ":" + id.String() }
func bucketKey(name string, id uuid.UUID) string {
	return "relay:tb:" + name + ":" + id.String()
}

var (
	//go:embed scripts/bucket.lua
	bucketSrc string
	//go:embed scripts/acquire.lua
	acquireSrc string
	//go:embed scripts/release.lua
	releaseSrc string
	//go:embed scripts/promote.lua
	promoteSrc string
	//go:embed scripts/unpark.lua
	unparkSrc string

	bucketScript  = redis.NewScript(bucketSrc)
	acquireScript = redis.NewScript(acquireSrc)
	releaseScript = redis.NewScript(releaseSrc)
	promoteScript = redis.NewScript(promoteSrc)
	unparkScript  = redis.NewScript(unparkSrc)
)

// Queue wraps the Redis client.
type Queue struct{ rdb redis.UniversalClient }

// New returns a Queue on rdb.
func New(rdb redis.UniversalClient) *Queue { return &Queue{rdb: rdb} }

// Client exposes the Redis client (health checks).
func (q *Queue) Client() redis.UniversalClient { return q.rdb }

// Setup creates the consumer group (and the stream) if it doesn't exist.
func (q *Queue) Setup(ctx context.Context) error {
	err := q.rdb.XGroupCreateMkStream(ctx, DeliveriesStream, WorkerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("queue: create group: %w", err)
	}
	return nil
}

// EnqueueDeliveries adds deliveries that are due now to the delivery stream.
func (q *Queue) EnqueueDeliveries(ctx context.Context, items ...Item) error {
	if len(items) == 0 {
		return nil
	}
	_, err := q.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, it := range items {
			p.XAdd(ctx, &redis.XAddArgs{Stream: DeliveriesStream, Values: []any{itemField, it.String()}})
		}
		return nil
	})
	return err
}

// Schedule queues a delivery for a later time (a retry). Scheduling the same delivery again moves it.
func (q *Queue) Schedule(ctx context.Context, it Item, at time.Time) error {
	return q.rdb.ZAdd(ctx, delayedKey, redis.Z{Score: float64(at.UnixMilli()), Member: it.String()}).Err()
}

// PromoteDue moves up to max delayed deliveries that are due into the delivery stream.
func (q *Queue) PromoteDue(ctx context.Context, max int) (int, error) {
	return promoteScript.Run(ctx, q.rdb, []string{delayedKey, DeliveriesStream}, max).Int()
}

// Bucket is a token bucket's settings: rate tokens per second, up to burst.
type Bucket struct{ Rate, Burst int }

// Allow takes one token from the tenant's ingest bucket. When it is empty, it returns how long until the next
// token.
func (q *Queue) Allow(ctx context.Context, name string, id uuid.UUID, b Bucket) (bool, time.Duration, error) {
	res, err := bucketScript.Run(ctx, q.rdb, []string{bucketKey(name, id)}, b.Rate, b.Burst, 1).Int64Slice()
	if err != nil {
		return false, 0, err
	}
	return res[0] == 1, time.Duration(res[1]) * time.Millisecond, nil
}

// Limits apply to one delivery attempt.
type Limits struct {
	EndpointInFlight int    // concurrent attempts to the endpoint
	TenantInFlight   int    // concurrent attempts across the tenant's endpoints
	Rate             Bucket // tenant's deliveries per second
}

// Verdict is the result of Acquire.
type Verdict int

// Acquire results.
const (
	Acquired      Verdict = iota // slots taken: attempt now, then Release
	EndpointBusy                 // parked until one of the endpoint's attempts finishes
	TenantBusy                   // parked until one of the tenant's attempts finishes
	RateLimited                  // scheduled again when the tenant's bucket has a token
	AlreadyActive                // an attempt for this delivery is running (a duplicate message)
)

func (v Verdict) String() string {
	return [...]string{"acquired", "endpoint_busy", "tenant_busy", "rate_limited", "already_active"}[v]
}

// Acquire takes a slot at the endpoint and at the tenant plus one token from the tenant's delivery bucket, all
// or nothing. When a slot is not free the delivery is parked atomically (no lost wake-up); when the bucket is
// empty it is scheduled for when a token will be there. Either way the caller just acknowledges the message.
func (q *Queue) Acquire(ctx context.Context, it Item, l Limits, lease time.Duration) (Verdict, error) {
	keys := []string{
		semKey("e", it.Endpoint), semKey("t", it.Tenant), parkKey("e", it.Endpoint), parkKey("t", it.Tenant),
		parkedIndexKey, bucketKey("deliver", it.Tenant), delayedKey,
	}
	res, err := acquireScript.Run(ctx, q.rdb, keys, it.Delivery.String(), it.String(), lease.Milliseconds(),
		l.EndpointInFlight, l.TenantInFlight, l.Rate.Rate, l.Rate.Burst).Int64Slice()
	if err != nil {
		return 0, err
	}
	return Verdict(res[0]), nil
}

// AcquireMany runs Acquire for several deliveries in one round trip to Redis.
func (q *Queue) AcquireMany(ctx context.Context, items []Item, limits []Limits, lease time.Duration) ([]Verdict, error) {
	run := func() ([]*redis.Cmd, error) {
		cmds := make([]*redis.Cmd, len(items))
		_, err := q.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i, it := range items {
				l := limits[i]
				keys := []string{
					semKey("e", it.Endpoint), semKey("t", it.Tenant), parkKey("e", it.Endpoint), parkKey("t", it.Tenant),
					parkedIndexKey, bucketKey("deliver", it.Tenant), delayedKey,
				}
				cmds[i] = acquireScript.EvalSha(ctx, p, keys, it.Delivery.String(), it.String(), lease.Milliseconds(),
					l.EndpointInFlight, l.TenantInFlight, l.Rate.Rate, l.Rate.Burst)
			}
			return nil
		})
		return cmds, err
	}
	cmds, err := run()
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") { // Redis restarted: load the script once
		if err := acquireScript.Load(ctx, q.rdb).Err(); err != nil {
			return nil, err
		}
		cmds, err = run()
	}
	if err != nil {
		return nil, err
	}
	out := make([]Verdict, len(cmds))
	for i, c := range cmds {
		res, err := c.Int64Slice()
		if err != nil {
			return nil, err
		}
		out[i] = Verdict(res[0])
	}
	return out, nil
}

// Release frees the slots taken by Acquire and wakes one parked delivery of the endpoint and one of the tenant.
func (q *Queue) Release(ctx context.Context, it Item) error {
	keys := []string{
		semKey("e", it.Endpoint), semKey("t", it.Tenant), parkKey("e", it.Endpoint), parkKey("t", it.Tenant),
		parkedIndexKey, DeliveriesStream,
	}
	return releaseScript.Run(ctx, q.rdb, keys, it.Delivery.String()).Err()
}

// Parked is one parking lot with deliveries waiting.
type Parked struct {
	Key   string
	Kind  string // "e" (endpoint) or "t" (tenant)
	First Item   // the oldest waiting delivery, to look up the limit
}

// ParkedLots lists the parking lots that have deliveries waiting.
func (q *Queue) ParkedLots(ctx context.Context) ([]Parked, error) {
	keys, err := q.rdb.SMembers(ctx, parkedIndexKey).Result()
	if err != nil {
		return nil, err
	}
	var out []Parked
	for _, k := range keys {
		kind := strings.TrimPrefix(k, "relay:park:")
		if len(kind) < 2 || (kind[0] != 'e' && kind[0] != 't') {
			continue
		}
		first, err := q.rdb.ZRange(ctx, k, 0, 0).Result()
		if err != nil {
			return nil, err
		}
		if len(first) == 0 {
			q.rdb.SRem(ctx, parkedIndexKey, k)
			continue
		}
		it, err := ParseItem(first[0])
		if err != nil {
			q.rdb.ZRem(ctx, k, first[0])
			continue
		}
		out = append(out, Parked{Key: k, Kind: kind[:1], First: it})
	}
	return out, nil
}

// Unpark moves as many deliveries out of a parking lot as its semaphore has free slots. It is the safety net for
// wake-ups lost when a worker died holding a slot (the slot's lease expires).
func (q *Queue) Unpark(ctx context.Context, p Parked, limit int) (int, error) {
	sem := strings.Replace(p.Key, "relay:park:", "relay:sem:", 1)
	return unparkScript.Run(ctx, q.rdb, []string{p.Key, sem, parkedIndexKey, DeliveriesStream}, limit).Int()
}

// Message is one stream entry handed to a consumer.
type Message struct {
	ID    string
	Value string
}

// Read blocks up to block for new entries of a stream for this consumer.
func (q *Queue) Read(ctx context.Context, stream, group, consumer string, count int, block time.Duration) ([]Message, error) {
	res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: consumer, Streams: []string{stream, ">"}, Count: int64(count), Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toMessages(res), nil
}

func toMessages(res []redis.XStream) []Message {
	var out []Message
	for _, s := range res {
		for _, m := range s.Messages {
			v, _ := m.Values[itemField].(string)
			out = append(out, Message{ID: m.ID, Value: v})
		}
	}
	return out
}

// Ack acknowledges and deletes entries, so a stream's length is its backlog.
func (q *Queue) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := q.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.XAck(ctx, stream, group, ids...)
		p.XDel(ctx, stream, ids...)
		return nil
	})
	return err
}

// Reclaim takes over entries another consumer read but never acknowledged for longer than minIdle (that
// consumer died), and drops consumers that have been idle for a day with nothing pending.
func (q *Queue) Reclaim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int) ([]Message, error) {
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: stream, Group: group, Consumer: consumer, MinIdle: minIdle, Start: "0-0", Count: int64(count),
	}).Result()
	if err != nil {
		return nil, err
	}
	if consumers, err := q.rdb.XInfoConsumers(ctx, stream, group).Result(); err == nil {
		for _, c := range consumers {
			if c.Name != consumer && c.Pending == 0 && c.Idle > 24*time.Hour {
				q.rdb.XGroupDelConsumer(ctx, stream, group, c.Name)
			}
		}
	}
	return toMessages([]redis.XStream{{Stream: stream, Messages: msgs}}), nil
}

// Depth reports the work waiting in Redis, for the metrics.
type Depth struct {
	Deliveries, Delayed, Parked int64
}

// Depths reads the current backlog sizes.
func (q *Queue) Depths(ctx context.Context) (Depth, error) {
	var d Depth
	var err error
	if d.Deliveries, err = q.rdb.XLen(ctx, DeliveriesStream).Result(); err != nil {
		return d, err
	}
	if d.Delayed, err = q.rdb.ZCard(ctx, delayedKey).Result(); err != nil {
		return d, err
	}
	keys, err := q.rdb.SMembers(ctx, parkedIndexKey).Result()
	if err != nil {
		return d, err
	}
	for _, k := range keys {
		d.Parked += q.rdb.ZCard(ctx, k).Val()
	}
	return d, nil
}

// Invalidate tells every API and worker process to drop a cached entry ("key:<hash>", "tenant:<id>",
// "routes:<tenant id>", "endpoint:<id>").
func (q *Queue) Invalidate(ctx context.Context, what string) error {
	return q.rdb.Publish(ctx, invalidateChan, what).Err()
}

// Invalidations streams the messages sent by Invalidate until ctx ends.
func (q *Queue) Invalidations(ctx context.Context) <-chan string {
	out := make(chan string, 64)
	sub := q.rdb.Subscribe(ctx, invalidateChan)
	go func() {
		defer close(out)
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- m.Payload:
				default: // a slow reader only loses cache freshness, never correctness (entries also expire)
				}
			}
		}
	}()
	return out
}
