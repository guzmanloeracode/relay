package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/batch"
	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// recorder writes attempt results in batches, like the API's event writer: every result waiting when a flusher
// is free goes into one statement, so a busy worker spends one round trip and one commit on many attempts.
type recorder struct {
	st     *store.Store
	log    *slog.Logger
	in     chan *pendingRecord
	max    int
	linger time.Duration
}

type pendingRecord struct {
	id        uuid.UUID
	seen      int32
	outcome   string
	next      time.Time
	started   time.Time
	latencyMs int32
	status    int32
	err       string
	body      string
	reason    string
	done      chan recordResult
}

type recordResult struct {
	applied bool // false: another worker recorded this attempt first
	err     error
}

func newRecorder(st *store.Store, log *slog.Logger, flushers, maxBatch int) *recorder {
	return &recorder{st: st, log: log, in: make(chan *pendingRecord, flushers*maxBatch), max: maxBatch, linger: recorderLinger}
}

func (r *recorder) run(ctx context.Context, flushers int) {
	for range flushers {
		go func() {
			b := make([]*pendingRecord, 0, r.max)
			for {
				select {
				case <-ctx.Done():
					return
				case first := <-r.in:
					b = append(b[:0], first)
				}
				b = batch.Collect(r.in, b, r.max, r.linger)
				r.flush(ctx, b)
			}
		}()
	}
}

// record queues one result and waits for its batch.
func (r *recorder) record(p *pendingRecord) recordResult {
	p.done = make(chan recordResult, 1)
	r.in <- p
	return <-p.done
}

func (r *recorder) flush(ctx context.Context, batch []*pendingRecord) {
	n := len(batch)
	a := db.RecordAttemptsParams{
		Ids: make([]uuid.UUID, n), Seen: make([]int32, n), Outcomes: make([]string, n), NextAt: make([]time.Time, n),
		StartedAt: make([]time.Time, n), Latencies: make([]int32, n), StatusCodes: make([]int32, n),
		Errors: make([]string, n), Bodies: make([]string, n), DeadReasons: make([]string, n),
	}
	for i, p := range batch {
		a.Ids[i], a.Seen[i], a.Outcomes[i], a.NextAt[i] = p.id, p.seen, p.outcome, p.next
		a.StartedAt[i], a.Latencies[i], a.StatusCodes[i] = p.started, p.latencyMs, p.status
		a.Errors[i], a.Bodies[i], a.DeadReasons[i] = p.err, p.body, p.reason
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	applied, err := r.st.RecordAttempts(ctx, a)
	if err != nil {
		r.log.Error("recording attempts", "err", err, "count", n)
	}
	ok := make(map[uuid.UUID]bool, len(applied))
	for _, id := range applied {
		ok[id] = true
	}
	for _, p := range batch {
		p.done <- recordResult{applied: ok[p.id], err: err}
	}
}
