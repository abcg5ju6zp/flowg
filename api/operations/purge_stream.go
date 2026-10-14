package operations

import (
	"context"
	"log/slog"

	"net/http"

	"go.uber.org/fx"

	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"

	"link-society.com/flowg/api/auth"
	"link-society.com/flowg/api/routing"
	"link-society.com/flowg/api/schemas"

	"link-society.com/flowg/internal/engines/lognotify"
	"link-society.com/flowg/internal/models"
	storage "link-society.com/flowg/internal/storage/interfaces"
)

// PurgeStreamDeps lists the dependencies of [NewPurgeStreamUsecase].
type PurgeStreamDeps struct {
	fx.In

	Logger *slog.Logger

	AuthStorage storage.AuthStorage
	LogStorage  storage.LogStorage
	LogNotifier lognotify.LogNotifier
}

// NewPurgeStreamUsecase retires a stream generation and removes the generation
// along with all of its logs, indexes, metadata and retention configuration.
//
// Callers must have the write-streams permission. This is a destructive
// operation: the stored logs cannot be recovered afterwards.
//
// The retirement is a generation-fenced state machine: the stream is fenced
// first (new writes and subscriptions are rejected), its live watchers receive
// a definitive termination notice, in-flight writes are drained, and only then
// are the entries, indexes, field metadata and configuration deleted to a
// zero-key boundary. Purging a name that does not exist stays a successful
// no-op.
func NewPurgeStreamUsecase(deps PurgeStreamDeps) usecase.Interactor {
	u := usecase.NewInteractor(
		auth.RequireScopeApiDecorator(
			deps.Logger,
			deps.AuthStorage,
			models.SCOPE_WRITE_STREAMS,
			func(
				ctx context.Context,
				req schemas.PurgeStreamRequest,
				resp *schemas.PurgeStreamResponse,
			) error {
				generation, existed, err := deps.LogStorage.FenceStream(ctx, req.Stream)
				if err != nil {
					deps.Logger.ErrorContext(
						ctx,
						"Failed to fence stream for purge",
						slog.String("stream", req.Stream),
						slog.String("error", err.Error()),
					)

					resp.Success = false
					return status.Wrap(err, status.Internal)
				}

				if !existed {
					// Unknown name: no fence was created, so generation-0
					// watchers waiting for a future stream must not be closed.
					resp.Success = true
					return nil
				}

				// Close watchers at the fence boundary, before draining and
				// deleting, so they cannot observe late records. Storage is the
				// source of truth: a notifier failure is logged but must not
				// leave the stream half-purged.
				if err := deps.LogNotifier.CloseStream(
					ctx, req.Stream, generation, lognotify.TerminationPurged,
				); err != nil {
					deps.Logger.ErrorContext(
						ctx,
						"Failed to terminate stream watchers",
						slog.String("stream", req.Stream),
						slog.Uint64("generation", generation),
						slog.String("error", err.Error()),
					)
				}

				if err := deps.LogStorage.FinishStreamRetirement(ctx, req.Stream); err != nil {
					deps.Logger.ErrorContext(
						ctx,
						"Failed to purge stream",
						slog.String("stream", req.Stream),
						slog.Uint64("generation", generation),
						slog.String("error", err.Error()),
					)

					resp.Success = false
					return status.Wrap(err, status.Internal)
				}

				deps.Logger.InfoContext(
					ctx,
					"Log stream purged",
					slog.String("stream", req.Stream),
					slog.Uint64("generation", generation),
				)
				resp.Success = true

				return nil
			},
		),
	)

	u.SetName("purge_stream")
	u.SetTitle("Purge Stream")
	u.SetDescription("Remove all logs (and indexes) related to a stream")
	u.SetTags("streams")

	u.SetExpectedErrors(status.PermissionDenied, status.Internal)

	return u
}

func init() {
	routing.RegisterOperation(
		NewPurgeStreamUsecase,
		http.MethodDelete,
		"/api/v1/streams/{stream}",
	)
}
