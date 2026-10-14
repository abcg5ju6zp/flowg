package log

import (
	"context"
	"sync"

	storage "link-society.com/flowg/internal/storage/interfaces"
)

// retirementGate is the in-memory first line of the purge fence. It works hand
// in hand with the persisted generation tombstone:
//
//   - writes enter the gate before touching storage; once a stream is fenced
//     every new write is rejected immediately, while writes admitted before
//     the fence (an in-flight syslog/HTTP batch) run to completion,
//   - the purge owner waits for those admitted writers to drain before any key
//     is deleted,
//   - duplicate purges join the in-flight owner instead of starting a second
//     cleanup that could fight it.
//
// The gate is a fast path only. It never decides correctness by itself: the
// persisted tombstone fences transactions directly, so an operation that
// slipped past the gate (or that comes from another process) is still
// rejected inside the storage transaction.
type retirementGate struct {
	mu sync.Mutex

	// cond wakes up waiters when admitted writers finish or when a fence is
	// lifted.
	cond *sync.Cond

	// retiring lists the streams currently fenced by a local purge owner.
	retiring map[string]struct{}
	// inflight counts admitted, not-yet-finished write operations per stream.
	inflight map[string]int
}

func newRetirementGate() *retirementGate {
	g := &retirementGate{
		retiring: map[string]struct{}{},
		inflight: map[string]int{},
	}
	g.cond = sync.NewCond(&g.mu)

	return g
}

// enter admits a write to a stream. It returns storage.ErrStreamRetiring when
// the stream is fenced; otherwise the write is counted as in-flight until the
// matching leave.
func (g *retirementGate) enter(stream string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.retiring[stream]; ok {
		return storage.ErrStreamRetiring
	}

	g.inflight[stream]++

	return nil
}

// leave marks an admitted write as finished, waking the fence owner that is
// waiting for in-flight batches to drain.
func (g *retirementGate) leave(stream string) {
	g.mu.Lock()

	g.inflight[stream]--
	if g.inflight[stream] <= 0 {
		delete(g.inflight, stream)
	}

	g.cond.Broadcast()
	g.mu.Unlock()
}

// tryBecomeOwner fences the stream for a purge on behalf of the caller. It
// returns false when a local purge already owns the stream, in which case
// callers join it with waitIdle.
func (g *retirementGate) tryBecomeOwner(stream string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.retiring[stream]; ok {
		return false
	}

	g.retiring[stream] = struct{}{}

	return true
}

// drainWriters blocks until every write admitted before the fence has
// completed, so admitted batches either finish before deletion starts or have
// been rejected by the in-transaction tombstone check. Only the fence owner
// calls it. Context cancellation unblocks the wait with the context error.
func (g *retirementGate) drainWriters(ctx context.Context, stream string) error {
	return g.waitWhile(ctx, stream, func(inflight int, fenced bool) bool {
		return inflight > 0
	})
}

// waitIdle blocks while a local purge owner is fenced on the stream, letting a
// duplicate purge join the in-flight retirement and observe its outcome.
func (g *retirementGate) waitIdle(ctx context.Context, stream string) error {
	return g.waitWhile(ctx, stream, func(_ int, fenced bool) bool {
		return fenced
	})
}

// release lifts the local fence. The persisted tombstone stays the
// authoritative fence, so releasing after a failure simply hands the stream
// back to the recovery machinery without risking a write.
func (g *retirementGate) release(stream string) {
	g.mu.Lock()

	delete(g.retiring, stream)
	g.cond.Broadcast()

	g.mu.Unlock()
}

// waitWhile cond-waits until predicate returns false (holding the lock while
// evaluating) or ctx is canceled.
func (g *retirementGate) waitWhile(
	ctx context.Context,
	stream string,
	predicate func(inflight int, fenced bool) bool,
) error {
	stop := context.AfterFunc(ctx, g.cond.Broadcast)
	defer stop()

	g.mu.Lock()
	defer g.mu.Unlock()

	for predicate(g.inflight[stream], g.isFenced(stream)) {
		if err := ctx.Err(); err != nil {
			return err
		}

		g.cond.Wait()
	}

	return nil
}

// isFenced reports whether a local owner fences the stream. Callers must hold
// g.mu.
func (g *retirementGate) isFenced(stream string) bool {
	_, ok := g.retiring[stream]
	return ok
}
