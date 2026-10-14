package log

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"link-society.com/flowg/internal/storage/databases/log/transactions"
)

// RetirementReasonPurged is the definite termination reason delivered to SSE
// watchers when their stream is purged.
const RetirementReasonPurged = "stream purged"

// retirementMaxConsecutiveBatchErrors bounds how many failing or inconclusive
// batches a purge tolerates within one DeleteStream call before leaving the
// tombstone in place and returning the error. The recovery worker and any
// duplicate purge then drive the remaining stages from the keyspace facts.
const retirementMaxConsecutiveBatchErrors = 5

// retirementBackoffs separate retries after an uncertain delete response or
// an inconclusive scan, so a failing backend is not hammered.
var retirementBackoffs = []time.Duration{
	100 * time.Millisecond,
	200 * time.Millisecond,
	400 * time.Millisecond,
	800 * time.Millisecond,
	1600 * time.Millisecond,
}

// StreamTerminator is notified when a purge freezes a stream. Implementations
// tear down the live SSE watchers with a definite reason and reject new
// subscriptions while the generation is retiring. It is satisfied structurally
// by the log notifier without this package importing the engines layer.
type StreamTerminator interface {
	CloseStream(ctx context.Context, stream, reason string) error
}

// WithTerminator wires the stream lifecycle sink (the live log notifier)
// notified when a purge fences a stream. It may stay nil in standalone tests;
// the persisted fence keeps guaranteeing correctness without it.
func (s *Storage[QTx, MTx]) WithTerminator(terminator StreamTerminator) *Storage[QTx, MTx] {
	s.terminator = terminator
	return s
}

// DeleteStream purges a stream under a freshly minted "retirement generation".
//
// The protocol is:
//  1. Probe the keyspace. A name that holds nothing (no tombstone, config,
//     generation token or data) is an idempotent no-op, preserving the
//     historical behavior for nonexistent streams.
//  2. Mint (or reuse) the generation tombstone first; it fences every write,
//     configuration change, subscription and GC run.
//  3. Freeze the stream: reject new local writes, wait for batches admitted
//     before the fence to finish, terminate live watchers with a definite
//     reason.
//  4. Cross the deletion boundaries in order — log shards, indexes, field
//     metadata, then retention configuration and the generation token —
//     persisting each stage's queryable result. Stage completion is decided
//     by scanning the keyspace, not by trusting a delete's response.
//  5. Remove the tombstone last, only while this generation still owns the
//     name, so a stale cleanup can never delete a recreated same-named
//     stream's data.
//
// A duplicate call joins the in-flight purge. An interrupted purge (crash,
// error) leaves its tombstone behind and is resumed by RecoverRetirements.
func (s *Storage[QTx, MTx]) DeleteStream(ctx context.Context, stream string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var (
			rec    *transactions.RetirementRecord
			exists bool
		)

		err := s.adapter.View(ctx, func(txn QTx) error {
			var err error

			rec, err = transactions.FetchRetirement(txn, stream)
			if err != nil {
				return err
			}

			if rec == nil {
				exists, err = transactions.StreamExists(txn, stream)
				if err != nil {
					return err
				}
			}

			return nil
		})
		if err != nil {
			return err
		}

		// Compatibility: purging a stream that never existed succeeds without
		// minting a generation, so it stays an idempotent no-op.
		if rec == nil && !exists {
			return nil
		}

		if !s.gate.tryBecomeOwner(stream) {
			// A local purge owns the stream: join it and observe its outcome.
			if err := s.gate.waitIdle(ctx, stream); err != nil {
				return err
			}

			continue // re-probe: either the tombstone is gone (done) or an
			// interrupted marker is now ownerless and we take it over.
		}

		runErr := s.purgeAsOwner(ctx, stream, rec)
		s.gate.release(stream)

		if errors.Is(runErr, transactions.ErrGenerationMoved) {
			s.logger.WarnContext(
				ctx,
				"Stale stream retirement abandoned: generation no longer owns the name",
				slog.String("stream", stream),
			)
			return nil
		}

		return runErr
	}
}

// purgeAsOwner runs the fenced phases of a purge. existing is nil when this
// process owns a brand-new generation; otherwise it is the tombstone resumed
// after a crash or a failed attempt.
func (s *Storage[QTx, MTx]) purgeAsOwner(
	ctx context.Context,
	stream string,
	existing *transactions.RetirementRecord,
) error {
	rec := existing

	if rec == nil {
		err := s.adapter.Update(ctx, func(txn MTx) error {
			var alreadyExisted bool
			var err error

			rec, alreadyExisted, err = transactions.BeginRetirement(txn, stream)
			if err != nil {
				return err
			}
			if alreadyExisted {
				// Lost the create race: concrete keyspace fact wins, proceed
				// with the generation that actually owns the name.
				s.logger.DebugContext(
					ctx,
					"Reusing existing retirement generation",
					slog.String("stream", stream),
					slog.String("generation", rec.Generation),
				)
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("could not begin retirement of stream '%s': %w", stream, err)
		}
	}

	s.logger.InfoContext(
		ctx,
		"Stream retirement started",
		slog.String("stream", stream),
		slog.String("generation", rec.Generation),
	)

	// Batches admitted before the fence either finish here or are rejected by
	// the in-transaction fence; no key is deleted before they are accounted
	// for.
	if err := s.gate.drainWriters(ctx, stream); err != nil {
		return err
	}

	// Freeze live watchers: they receive a definite termination reason, and
	// new subscriptions are rejected. Best effort — even if the notifier is
	// stopped, the tombstone-driven subscribe rejection and the SSE disconnect
	// semantics still apply, and recovery re-issues the termination.
	if s.terminator != nil {
		if err := s.terminator.CloseStream(ctx, stream, RetirementReasonPurged); err != nil {
			if ctx.Err() == nil {
				s.logger.WarnContext(
					ctx,
					"Failed to terminate stream watchers during purge",
					slog.String("stream", stream),
					slog.String("error", err.Error()),
				)
			}
		}
	}

	for _, stage := range transactions.OrderedRetirementStages {
		if err := s.completeRetirementStage(ctx, rec, stage); err != nil {
			return err
		}
	}

	if err := s.adapter.Update(ctx, func(txn MTx) error {
		return transactions.FinishRetirement(txn, stream, rec.Generation)
	}); err != nil {
		if errors.Is(err, transactions.ErrGenerationMoved) {
			return err
		}

		return fmt.Errorf(
			"could not finalize retirement generation '%s' of stream '%s': %w",
			rec.Generation, stream, err,
		)
	}

	s.logger.InfoContext(
		ctx,
		"Stream retirement completed",
		slog.String("stream", stream),
		slog.String("generation", rec.Generation),
	)

	return nil
}

// completeRetirementStage empties one deletion boundary and persists the
// stage's queryable result. Completion is established by a verification scan
// of the keyspace: a delete whose response was uncertain simply leaves the
// keys observable and they get deleted on the next scan-driven batch.
func (s *Storage[QTx, MTx]) completeRetirementStage(
	ctx context.Context,
	rec *transactions.RetirementRecord,
	stage transactions.RetirementStage,
) error {
	state := rec.Stage(stage)

	persist := func(status string) error {
		state.Status = status
		state.UpdatedAt = time.Now().UTC()

		return s.adapter.Update(ctx, func(txn MTx) error {
			return transactions.SaveRetirement(txn, rec)
		})
	}

	remaining, err := s.countStage(ctx, rec.Stream, stage)
	if err != nil {
		return err
	}

	switch {
	case state.Status == transactions.StageStatusDone && remaining == 0:
		return nil // resumed purge: keyspace confirms the stage is genuinely done.

	case state.Status == transactions.StageStatusDone && remaining > 0:
		// The stage was marked done but keys are back (orphaned indexes left
		// by an interleaved GC, or a crash between the verify and the marker
		// update). Reopen it against the facts.
		s.logger.WarnContext(
			ctx,
			"Retirement stage has residual keys, reopening it",
			slog.String("stream", rec.Stream),
			slog.String("stage", string(stage)),
			slog.Int("remaining", remaining),
		)

		if err := persist(transactions.StageStatusRunning); err != nil {
			return err
		}

	case state.Status != transactions.StageStatusRunning:
		if err := persist(transactions.StageStatusRunning); err != nil {
			return err
		}
	}

	consecutive := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var deleted int
		err := s.adapter.Update(ctx, func(txn MTx) error {
			var err error
			deleted, err = transactions.DeleteRetirementStageBatch(
				txn, rec.Stream, rec.Generation, stage, s.batchSize,
			)
			return err
		})

		if errors.Is(err, transactions.ErrGenerationMoved) {
			return err
		}

		if err != nil {
			if fail := s.onStageBatchError(ctx, rec, stage, err, &consecutive); fail != nil {
				return fail
			}
			continue
		}

		consecutive = 0
		state.Deleted += deleted

		remaining, err := s.countStage(ctx, rec.Stream, stage)
		if err != nil {
			return err
		}

		if remaining == 0 {
			// Keyspace fact: nothing this stage owns remains. Persist the
			// result before moving to the next boundary.
			if err := persist(transactions.StageStatusDone); err != nil {
				return err
			}

			s.logger.DebugContext(
				ctx,
				"Retirement stage completed",
				slog.String("stream", rec.Stream),
				slog.String("stage", string(stage)),
				slog.Int("deleted", state.Deleted),
			)

			return nil
		}

		if deleted == 0 {
			// Keys are observed but nothing could be cleared (an iterator
			// swallowed a backend error, or the outcome was uncertain). Back
			// off and re-derive truth from the next scan instead of hot
			// looping.
			inconclusive := errors.New("retirement batch deleted nothing while keys remain")
			if fail := s.onStageBatchError(ctx, rec, stage, inconclusive, &consecutive); fail != nil {
				return fail
			}
		}
	}
}

// onStageBatchError logs and backs off after a failed or inconclusive stage
// batch, returning the error to abort the purge once the retry budget is
// exhausted (the tombstone stays for recovery).
func (s *Storage[QTx, MTx]) onStageBatchError(
	ctx context.Context,
	rec *transactions.RetirementRecord,
	stage transactions.RetirementStage,
	batchErr error,
	consecutive *int,
) error {
	*consecutive++

	s.logger.ErrorContext(
		ctx,
		"Retirement stage batch failed, retrying from keyspace facts",
		slog.String("stream", rec.Stream),
		slog.String("generation", rec.Generation),
		slog.String("stage", string(stage)),
		slog.Int("attempt", *consecutive),
		slog.String("error", batchErr.Error()),
	)

	if *consecutive > retirementMaxConsecutiveBatchErrors {
		return fmt.Errorf(
			"retirement stage '%s' of stream '%s' stalled after %d attempts: %w",
			stage, rec.Stream, *consecutive-1, batchErr,
		)
	}

	backoff := retirementBackoffs[0]
	if *consecutive-1 < len(retirementBackoffs) {
		backoff = retirementBackoffs[*consecutive-1]
	}

	return sleepContext(ctx, backoff)
}

// countStage returns how many keys a stage still owns, read in a dedicated
// transaction.
func (s *Storage[QTx, MTx]) countStage(
	ctx context.Context,
	stream string,
	stage transactions.RetirementStage,
) (int, error) {
	var remaining int

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		remaining, err = transactions.CountRetirementStageKeys(txn, stream, stage)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf(
			"could not verify keyspace for stage '%s' of stream '%s': %w",
			stage, stream, err,
		)
	}

	return remaining, nil
}

// RecoverRetirements resumes every generation whose tombstone is present at
// startup or left behind by an aborted purge. It is safe to run repeatedly:
// streams purged by a live owner are skipped, finished purges have no
// tombstone, and each stage is idempotent and verified against the keyspace.
func (s *Storage[QTx, MTx]) RecoverRetirements(ctx context.Context) error {
	var records map[string]*transactions.RetirementRecord

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		records, err = transactions.ListRetirements(txn)
		return err
	})
	if err != nil {
		return fmt.Errorf("could not list interrupted stream retirements: %w", err)
	}

	streams := make([]string, 0, len(records))
	for stream := range records {
		streams = append(streams, stream)
	}
	sort.Strings(streams)

	for _, stream := range streams {
		if err := ctx.Err(); err != nil {
			return err
		}

		rec := records[stream]

		if !s.gate.tryBecomeOwner(stream) {
			s.logger.InfoContext(
				ctx,
				"Skipping retirement recovery: purge already in progress",
				slog.String("stream", stream),
				slog.String("generation", rec.Generation),
			)
			continue
		}

		s.logger.InfoContext(
			ctx,
			"Resuming interrupted stream retirement",
			slog.String("stream", stream),
			slog.String("generation", rec.Generation),
		)

		err := s.purgeAsOwner(ctx, stream, rec)
		s.gate.release(stream)

		if err != nil {
			if errors.Is(err, transactions.ErrGenerationMoved) {
				continue
			}

			return fmt.Errorf("could not recover retirement of stream '%s': %w", stream, err)
		}
	}

	return nil
}

// sleepContext waits for d or until ctx is canceled.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
