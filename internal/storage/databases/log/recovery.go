package log

import (
	"log/slog"
	"time"

	"github.com/vladopajic/go-actor/actor"

	"link-society.com/flowg/internal/storage/generic/kv"
)

// retirementRecoveryWorker is the actor that finishes purge generations that
// did not complete before a restart or a failed attempt. Its first pass runs
// immediately at startup (so watchers reconnecting after a crash are quickly
// terminated with a definite reason) and again at every interval, picking up
// tombstones left behind by aborted purges. Errors are logged and do not stop
// the worker: the persisted, per-stage record makes every pass resumable.
type retirementRecoveryWorker[QTx kv.QueryTx, MTx kv.MutationTx] struct {
	storage  *Storage[QTx, MTx]
	interval time.Duration
	ranOnce  bool
}

var _ actor.Worker = (*retirementRecoveryWorker[kv.QueryTx, kv.MutationTx])(nil)

// NewRetirementRecoveryWorker returns the recovery actor for unfinished
// stream retirement generations.
func NewRetirementRecoveryWorker[QTx kv.QueryTx, MTx kv.MutationTx](
	storage *Storage[QTx, MTx],
	interval time.Duration,
) actor.Worker {
	return &retirementRecoveryWorker[QTx, MTx]{
		storage:  storage,
		interval: interval,
	}
}

func (w *retirementRecoveryWorker[QTx, MTx]) DoWork(ctx actor.Context) actor.WorkerStatus {
	wait := w.interval
	if !w.ranOnce {
		// Recover immediately at startup; interval only paces later passes.
		wait = 0
		w.ranOnce = true
	}

	select {
	case <-ctx.Done():
		return actor.WorkerEnd

	case <-time.After(wait):
		if err := w.storage.RecoverRetirements(ctx); err != nil && ctx.Err() == nil {
			w.storage.logger.ErrorContext(
				ctx,
				"failed to recover stream retirements",
				slog.String("error", err.Error()),
			)
		}

		return actor.WorkerContinue
	}
}
