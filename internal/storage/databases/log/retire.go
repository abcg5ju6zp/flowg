package log

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	storage "link-society.com/flowg/internal/storage/interfaces"

	"link-society.com/flowg/internal/storage/databases/log/transactions"
)

// defaultDrainTimeout bounds how long a retirement waits for local writes that
// were already in flight when the fence landed. The durable in-transaction
// fence plus the keyspace-fact sweep stay correct past the deadline; the wait
// only lets received batches finish cleanly instead of being rejected.
const defaultDrainTimeout = 5 * time.Second

// deletionStages is the order in which data prefixes converge to zero keys.
var deletionStages = []transactions.RetirementStageName{
	transactions.StageEntries,
	transactions.StageIndexes,
	transactions.StageFields,
}

// streamWriteGate is the per-stream in-memory freeze gate. One exists for any
// stream that currently has active writers or an in-progress retirement.
type streamWriteGate struct {
	inflight int
	retiring bool
	// drained is closed the first time inflight reaches zero after the gate
	// became retiring.
	drained chan struct{}
}

// retirementGates holds the freeze gates for every stream.
type retirementGates struct {
	mu      sync.Mutex
	streams map[string]*streamWriteGate
}

func newRetirementGates() *retirementGates {
	return &retirementGates{streams: map[string]*streamWriteGate{}}
}

// beginWrite registers a local write (ingest or configuration change) against
// the stream. A retiring stream is rejected outright before any backend work;
// otherwise the returned release function must be called when the write's
// transaction has finished.
func (g *retirementGates) beginWrite(stream string) (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	f := g.streams[stream]
	if f == nil {
		f = &streamWriteGate{}
		g.streams[stream] = f
	}

	if f.retiring {
		return nil, fmt.Errorf(
			"stream '%s' is being purged: %w",
			stream, storage.ErrStreamRetiring,
		)
	}

	f.inflight++

	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()

		f.inflight--
		if f.retiring && f.inflight == 0 && f.drained != nil {
			select {
			case <-f.drained:
				// already closed
			default:
				close(f.drained)
			}
		}
	}, nil
}

// rejectIfRetiring is the cheaper gate for operations that do not need
// in-flight accounting (reads with lazy creation, configuration writes): it
// fails fast while the durable transaction guard remains the authority.
func (g *retirementGates) rejectIfRetiring(stream string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	f := g.streams[stream]
	if f != nil && f.retiring {
		return fmt.Errorf(
			"stream '%s' is being purged: %w",
			stream, storage.ErrStreamRetiring,
		)
	}

	return nil
}

// fence marks a stream as retiring. Writes in flight at that instant keep the
// gate's drained channel open until they complete; every later write is
// rejected. Fencing twice is idempotent.
func (g *retirementGates) fence(stream string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	f := g.streams[stream]
	if f == nil {
		f = &streamWriteGate{}
		g.streams[stream] = f
	}

	if !f.retiring {
		f.retiring = true
		f.drained = make(chan struct{})
		if f.inflight == 0 {
			close(f.drained)
		}
	}
}

// unfence removes the freeze after a retirement finalized successfully.
func (g *retirementGates) unfence(stream string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.streams, stream)
}

// awaitDrain blocks until the stream's in-flight writers finish, the bounded
// drain window elapses, or ctx is canceled. It reports true when every local
// writer drained, and false when the wait ended on the timeout or context.
func (g *retirementGates) awaitDrain(ctx context.Context, stream string, timeout time.Duration) bool {
	g.mu.Lock()
	f := g.streams[stream]
	if f == nil {
		g.mu.Unlock()
		return true
	}
	drained := f.drained
	g.mu.Unlock()

	if drained == nil {
		return true
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-drained:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// FenceStream implements [storage.LogStorage].
//
// It serializes the Fence→Finish window per stream (so duplicate purges cannot
// open two generations), persists the durable retirement fence and activates
// the local freeze gate. Purging a name with no generation, config or data is
// a side-effect-free success and returns existed=false without holding the
// serialization lock.
func (s *Storage[QTx, MTx]) FenceStream(
	ctx context.Context,
	stream string,
) (uint64, bool, error) {
	serial := s.retirementLock(stream)
	serial.Lock()

	var (
		retirement transactions.Retirement
		existed    bool
	)

	err := s.adapter.Update(ctx, func(txn MTx) error {
		var err error
		retirement, existed, err = transactions.BeginRetirement(txn, stream, time.Now())
		return err
	})
	if err != nil {
		serial.Unlock()
		return 0, false, fmt.Errorf("could not fence stream '%s': %w", stream, err)
	}

	if !existed {
		serial.Unlock()
		return 0, false, nil
	}

	// The durable record is the cross-process authority; the local gate freezes
	// writers of this process immediately and provides the drain accounting.
	s.gates.fence(stream)

	s.logger.InfoContext(
		ctx,
		"stream generation fenced for retirement",
		slog.String("stream", stream),
		slog.Uint64("generation", retirement.Generation),
	)

	return retirement.Generation, true, nil
}

// FinishStreamRetirement implements [storage.LogStorage]. It drains local
// in-flight writes, sweeps every data stage to zero keys, finalizes the
// configuration and fence removal, then releases the freeze gate. On error
// the gate stays closed while the serialization lock is released, so a
// duplicate purge or a restart recovery can resume the persisted state machine.
//
// Precondition: it must follow a FenceStream call by the same goroutine that
// returned existed=true; FenceStream holds the stream's serialization lock and
// Finish releases it. It must not be called when FenceStream reported the
// stream as absent.
func (s *Storage[QTx, MTx]) FinishStreamRetirement(ctx context.Context, stream string) error {
	defer s.retirementLock(stream).Unlock()

	err := s.runRetirement(ctx, stream)
	if err != nil {
		// Keep the freeze gate: the durable retirement is still in progress
		// and will be resumed by a duplicate purge or the startup recovery.
		return err
	}

	s.gates.unfence(stream)
	return nil
}

// StreamState implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) StreamState(
	ctx context.Context,
	stream string,
) (storage.StreamState, error) {
	var state storage.StreamState

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		state, err = transactions.FetchStreamState(txn, stream)
		return err
	})
	if err != nil {
		return storage.StreamState{}, err
	}

	return state, nil
}

// DeleteStream implements [storage.LogStorage] as the synchronous
// FenceStream → FinishStreamRetirement composition. Notifier orchestration
// stays the API layer's responsibility, so storage never depends on engines.
func (s *Storage[QTx, MTx]) DeleteStream(ctx context.Context, stream string) error {
	_, existed, err := s.FenceStream(ctx, stream)
	if err != nil {
		return err
	}

	if !existed {
		return nil
	}

	return s.FinishStreamRetirement(ctx, stream)
}

// RecoverRetirements resumes every durable retirement left unfinished by a
// crash or an uncertain response. It runs at startup before any service
// accepts traffic, so the local drain is immediate; the persisted stages and
// the keyspace-fact sweeps do all the work. A retirement record whose
// generation no longer matches the counter is obsolete (a newer lineage
// exists) and is discarded without touching data.
func (s *Storage[QTx, MTx]) RecoverRetirements(ctx context.Context) error {
	var retirements []transactions.Retirement

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		retirements, err = transactions.ListRetirements(txn)
		return err
	})
	if err != nil {
		return fmt.Errorf("could not list unfinished stream retirements: %w", err)
	}

	for _, retirement := range retirements {
		stream := retirement.Stream

		var currentGeneration uint64
		err := s.adapter.View(ctx, func(txn QTx) error {
			var err error
			currentGeneration, err = transactions.ReadGeneration(txn, stream)
			return err
		})
		if err != nil {
			return fmt.Errorf(
				"could not verify generation of unfinished retirement for '%s': %w",
				stream, err,
			)
		}

		if currentGeneration != retirement.Generation {
			s.logger.WarnContext(
				ctx,
				"discarding stale retirement record for rebuilt stream",
				slog.String("stream", stream),
				slog.Uint64("retired_generation", retirement.Generation),
				slog.Uint64("current_generation", currentGeneration),
			)

			err := s.adapter.Update(ctx, func(txn MTx) error {
				return transactions.DiscardRetirement(txn, stream)
			})
			if err != nil {
				return fmt.Errorf(
					"could not discard stale retirement for stream '%s': %w",
					stream, err,
				)
			}

			continue
		}

		s.logger.InfoContext(
			ctx,
			"resuming unfinished stream retirement",
			slog.String("stream", stream),
			slog.Uint64("generation", retirement.Generation),
			slog.String("phase", string(retirement.Phase)),
		)

		s.gates.fence(stream)

		if err := s.runRetirement(ctx, stream); err != nil {
			return fmt.Errorf(
				"could not resume retirement for stream '%s': %w",
				stream, err,
			)
		}

		s.gates.unfence(stream)
	}

	return nil
}

// runRetirement drives one retirement from whatever phase its durable record
// is in to finalization. It never increments the generation and never opens a
// retirement (FenceStream owns that); it only converges an existing record.
//
// Convergence relies exclusively on keyspace facts: each batch rescans its
// prefix root, so an uncertain commit or a restart simply heals on the next
// observation. Finalization's in-transaction zero-key verification is the
// single boundary at which the fence and the configuration are released.
func (s *Storage[QTx, MTx]) runRetirement(ctx context.Context, stream string) error {
	retirement, err := s.loadRetirement(ctx, stream)
	if err != nil {
		return err
	}
	if retirement == nil {
		// Already finalized (duplicate purge losing the race).
		return nil
	}

	generation := retirement.Generation

	// Draining phase: record it, then give local in-flight writes their
	// bounded window to finish cleanly.
	if err := s.persistPhase(ctx, stream, generation, transactions.PhaseDraining); err != nil {
		return err
	}

	s.logger.InfoContext(
		ctx,
		"draining in-flight writers before stream retirement",
		slog.String("stream", stream),
		slog.Uint64("generation", generation),
		slog.Duration("timeout", s.drainTimeout),
	)

	if drained := s.gates.awaitDrain(ctx, stream, s.drainTimeout); !drained {
		s.logger.WarnContext(
			ctx,
			"stream retirement drain window elapsed with writers in flight; proceeding on durable fence",
			slog.String("stream", stream),
			slog.Uint64("generation", generation),
		)
	}

	// Deleting phase: converge every data prefix to zero keys. The loop around
	// finalization re-enters the sweeps if new facts appear (a committed
	// straggler), so the phases never trust a counter over the keyspace.
	for {
		if err := s.persistPhase(ctx, stream, generation, transactions.PhaseDeleting); err != nil {
			return err
		}

		for _, stage := range deletionStages {
			if err := s.sweepStage(ctx, stream, generation, stage); err != nil {
				return err
			}
		}

		if err := s.persistPhase(ctx, stream, generation, transactions.PhaseFinalizing); err != nil {
			return err
		}

		finalized, err := s.finalize(ctx, stream, generation)
		if err != nil {
			return err
		}
		if finalized {
			s.logger.InfoContext(
				ctx,
				"stream generation retired",
				slog.String("stream", stream),
				slog.Uint64("generation", generation),
			)
			return nil
		}

		s.logger.WarnContext(
			ctx,
			"retirement finalization observed surviving keys, resweeping",
			slog.String("stream", stream),
			slog.Uint64("generation", generation),
		)
	}
}

// loadRetirement reads the durable record. A missing record signals an already
// finalized retirement.
func (s *Storage[QTx, MTx]) loadRetirement(
	ctx context.Context,
	stream string,
) (*transactions.Retirement, error) {
	var retirement *transactions.Retirement

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		retirement, err = transactions.LoadRetirement(txn, stream)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf(
			"could not load retirement for stream '%s': %w",
			stream, err,
		)
	}

	return retirement, nil
}

// persistPhase stamps a phase transition. A record that vanished (already
// finalized) makes this a no-op.
func (s *Storage[QTx, MTx]) persistPhase(
	ctx context.Context,
	stream string,
	generation uint64,
	phase transactions.RetirementPhase,
) error {
	return s.adapter.Update(ctx, func(txn MTx) error {
		retirement, err := transactions.LoadRetirement(txn, stream)
		if err != nil {
			return err
		}
		if retirement == nil {
			return nil
		}
		if retirement.Generation != generation {
			return fmt.Errorf(
				"stream '%s': record generation %d, expected %d: %w",
				stream, retirement.Generation, generation, transactions.ErrStaleRetirement,
			)
		}

		if retirement.Phase == phase {
			return nil
		}

		return transactions.SetRetirementPhase(txn, retirement, phase, time.Now())
	})
}

// sweepStage runs root-rescan batches for one data prefix until the observed
// key count for a batch is zero. The cumulative per-stage result is committed
// together with each batch's deletions, making progress queryable and
// crash-resumable.
func (s *Storage[QTx, MTx]) sweepStage(
	ctx context.Context,
	stream string,
	generation uint64,
	stage transactions.RetirementStageName,
) error {
	for {
		var (
			deleted   int
			stageDone bool
		)

		err := s.adapter.Update(ctx, func(txn MTx) error {
			retirement, err := transactions.LoadRetirement(txn, stream)
			if err != nil {
				return err
			}
			if retirement == nil {
				return nil
			}
			if retirement.Generation != generation {
				return fmt.Errorf(
					"stream '%s': record generation %d, expected %d during %s sweep: %w",
					stream, retirement.Generation, generation, stage, transactions.ErrStaleRetirement,
				)
			}

			// Rescan the prefix root unconditionally, even when the stage was
			// already observed empty: a writer whose transaction committed
			// late may have produced new keys, and only keyspace facts decide.
			deleted, err = transactions.DeleteRetirementStageBatch(
				txn, stream, stage, s.batchSize, retirement,
			)
			if err != nil {
				return err
			}

			stageDone = retirement.Stages[string(stage)].Done
			return nil
		})
		if err != nil {
			return fmt.Errorf(
				"could not sweep %s stage of retiring stream '%s': %w",
				stage, stream, err,
			)
		}

		if stageDone || deleted == 0 {
			return nil
		}
	}
}

// finalize performs the boundary transaction. It returns finalized=false when
// the zero-key verification failed so the caller resweeps instead of releasing
// the fence.
func (s *Storage[QTx, MTx]) finalize(
	ctx context.Context,
	stream string,
	generation uint64,
) (bool, error) {
	err := s.adapter.Update(ctx, func(txn MTx) error {
		retirement, err := transactions.LoadRetirement(txn, stream)
		if err != nil {
			return err
		}
		if retirement == nil {
			return nil
		}
		if retirement.Generation != generation {
			return fmt.Errorf(
				"stream '%s': record generation %d, expected %d at finalization: %w",
				stream, retirement.Generation, generation, transactions.ErrStaleRetirement,
			)
		}

		return transactions.FinalizeRetirement(txn, stream, generation, time.Now())
	})
	if err == nil {
		return true, nil
	}

	if errors.Is(err, transactions.ErrRetirementNotDrained) {
		return false, nil
	}

	return false, fmt.Errorf(
		"could not finalize retirement for stream '%s': %w",
		stream, err,
	)
}

// retirementLock returns the mutex serializing the Fence→Finish window for a
// stream. It is intentionally separate from the freeze gates so lock bookkeeping
// survives a gate removal.
func (s *Storage[QTx, MTx]) retirementLock(stream string) *sync.Mutex {
	s.retireLocksMu.Lock()
	defer s.retireLocksMu.Unlock()

	mu := s.retireLocks[stream]
	if mu == nil {
		mu = &sync.Mutex{}
		s.retireLocks[stream] = mu
	}

	return mu
}
