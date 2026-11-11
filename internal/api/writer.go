package api

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/store"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// eventWriter is the API's group commit. Each request hands its event to one of a few flushers; a flusher takes
// every event waiting at that moment and writes them with one statement, so one commit (and one WAL flush)
// covers many requests. It never waits on purpose: with little traffic each batch holds one event, and under
// load batches grow by themselves, which keeps ingestion latency flat as the rate rises.
type eventWriter struct {
	st  *store.Store
	in  chan *pendingEvent
	max int
}

type pendingEvent struct {
	p    db.InsertEventParams
	done chan writeResult
}

type writeResult struct {
	id        uuid.UUID
	createdAt time.Time
	conflict  bool // the Idempotency-Key exists already
	err       error
}

func newEventWriter(st *store.Store, flushers, maxBatch int) *eventWriter {
	return &eventWriter{st: st, in: make(chan *pendingEvent, flushers*maxBatch), max: maxBatch}
}

// run starts the flushers; they stop when ctx ends.
func (w *eventWriter) run(ctx context.Context, flushers int) {
	for range flushers {
		go func() {
			batch := make([]*pendingEvent, 0, w.max)
			for {
				select {
				case <-ctx.Done():
					return
				case first := <-w.in:
					batch = append(batch[:0], first)
				}
			fill:
				for len(batch) < w.max {
					select {
					case p := <-w.in:
						batch = append(batch, p)
					default:
						break fill
					}
				}
				w.flush(context.WithoutCancel(ctx), batch)
			}
		}()
	}
}

// write stores one event and waits for its batch to commit.
func (w *eventWriter) write(ctx context.Context, p db.InsertEventParams) writeResult {
	pe := &pendingEvent{p: p, done: make(chan writeResult, 1)}
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
	a := db.InsertEventsParams{
		Ids: make([]uuid.UUID, n), TenantIds: make([]uuid.UUID, n), Types: make([]string, n), Payloads: make([]string, n),
		Messages: make([]string, n), Targets: make([]string, n), IdemKeys: make([]string, n), RequestHashes: make([][]byte, n),
	}
	for i, pe := range batch {
		p := pe.p
		a.Ids[i], a.TenantIds[i], a.Types[i], a.Payloads[i] = p.ID, p.TenantID, p.Type, string(p.Payload)
		a.Messages[i], a.Targets[i], a.RequestHashes[i] = string(p.Message), string(p.Targets), p.RequestHash
		if p.IdempotencyKey != nil {
			a.IdemKeys[i] = *p.IdempotencyKey
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := w.st.InsertEvents(ctx, a)
	stored := make(map[uuid.UUID]time.Time, len(rows))
	for _, r := range rows {
		stored[r.ID] = r.CreatedAt
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
