package operations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"go.uber.org/fx"

	"github.com/swaggest/openapi-go"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"

	"link-society.com/flowg/api/auth"
	"link-society.com/flowg/api/routing"
	"link-society.com/flowg/api/schemas"

	"link-society.com/flowg/internal/engines/lognotify"
	"link-society.com/flowg/internal/models"
	storage "link-society.com/flowg/internal/storage/interfaces"
	"link-society.com/flowg/internal/utils/langs/filtering"
)

// WatchLogsDeps lists the dependencies of [NewWatchLogsUsecase].
type WatchLogsDeps struct {
	fx.In

	Logger *slog.Logger

	AuthStorage storage.AuthStorage
	LogStorage  storage.LogStorage
	LogNotifier lognotify.LogNotifier
}

// NewWatchLogsUsecase streams a stream's logs to the client in real time as
// Server-Sent Events.
//
// It subscribes to new records and forwards those matching the optional filter
// until the client disconnects or the watched generation is purged: an active
// purge closes the stream with a definitive "stream-retired" event carrying the
// reason and the retired generation. Callers must have the read-streams
// permission. A filter that fails to compile is reported as an
// invalid-argument error. Subscribing while the stream is mid-purge, or after
// it was purged but before it is recreated, fails with FailedPrecondition; a
// stream name that never existed can still be watched for future logs.
func NewWatchLogsUsecase(deps WatchLogsDeps) usecase.Interactor {
	u := usecase.NewInteractor(
		auth.RequireScopeApiDecorator(
			deps.Logger,
			deps.AuthStorage,
			models.SCOPE_READ_STREAMS,
			func(
				ctx context.Context,
				req schemas.WatchLogsRequest,
				resp *schemas.WatchLogsResponse,
			) error {
				var filter filtering.Filter

				if req.Filter != nil && *req.Filter != "" {
					var err error

					filter, err = filtering.Compile(*req.Filter)
					if err != nil {
						deps.Logger.ErrorContext(
							ctx,
							"Failed to compile filter",
							slog.String("error", err.Error()),
							slog.String("stream", req.Stream),
							slog.String("filter", *req.Filter),
						)

						return status.Wrap(err, status.InvalidArgument)
					}
				}

				// Generation-aware admission: a retiring or already-retired
				// generation is rejected definitively, while a name never seen
				// (generation 0) keeps the historical "wait for future logs"
				// behavior.
				streamState, err := deps.LogStorage.StreamState(ctx, req.Stream)
				if err != nil {
					deps.Logger.ErrorContext(
						ctx,
						"Failed to read stream state",
						slog.String("stream", req.Stream),
						slog.String("error", err.Error()),
					)

					return status.Wrap(err, status.Internal)
				}

				if streamState.Retiring {
					return status.Wrap(storage.ErrStreamRetiring, status.FailedPrecondition)
				}

				if !streamState.Exists && streamState.Generation > 0 {
					return status.Wrap(storage.ErrStreamRetired, status.FailedPrecondition)
				}

				logM, err := deps.LogNotifier.Subscribe(ctx, req.Stream, streamState.Generation)
				if err != nil {
					if errors.Is(err, lognotify.ErrStreamRetired) {
						return status.Wrap(err, status.FailedPrecondition)
					}

					deps.Logger.ErrorContext(
						ctx,
						"Failed to subscribe to logs",
						slog.String("error", err.Error()),
						slog.String("stream", req.Stream),
					)

					return status.Wrap(err, status.Internal)
				}

				resp.Writer.(http.ResponseWriter).Header().Set("Access-Control-Allow-Origin", "*")
				resp.Writer.(http.ResponseWriter).Header().Set("Access-Control-Expose-Headers", "Content-Type")

				resp.Writer.(http.ResponseWriter).Header().Set("Content-Type", "text/event-stream")
				resp.Writer.(http.ResponseWriter).Header().Set("Cache-Control", "no-cache")
				resp.Writer.(http.ResponseWriter).Header().Set("Connection", "keep-alive")

				deps.Logger.InfoContext(
					ctx,
					"watch logs",
					slog.String("stream", req.Stream),
					slog.Uint64("generation", streamState.Generation),
				)
				defer deps.Logger.InfoContext(
					ctx,
					"done watching logs",
					slog.String("stream", req.Stream),
				)

				for log := range logM.ReceiveC() {
					if log.Terminated != nil {
						payload, err := json.Marshal(struct {
							Generation uint64                     `json:"generation"`
							Reason     lognotify.TerminationReason `json:"reason"`
						}{
							Generation: log.Terminated.Generation,
							Reason:     log.Terminated.Reason,
						})
						if err != nil {
							fmt.Fprintf(resp.Writer, "event: error\n")
							fmt.Fprintf(resp.Writer, "data: %s\n\n", err.Error())
							resp.Writer.(http.Flusher).Flush()
							return nil
						}

						fmt.Fprintf(resp.Writer, "event: stream-retired\n")
						fmt.Fprintf(resp.Writer, "data: %s\n\n", payload)
						resp.Writer.(http.Flusher).Flush()
						return nil
					}

					matches := false

					if filter == nil {
						matches = true
					} else {
						var err error
						matches, err = filter.Evaluate(&log.LogRecord)
						if err != nil {
							fmt.Fprintf(resp.Writer, "event: error\n")
							fmt.Fprintf(
								resp.Writer,
								"data: failed to evaluate filter for log entry '%s': %s\n\n",
								log.LogKey,
								err.Error(),
							)
							resp.Writer.(http.Flusher).Flush()

							return nil
						}
					}

					if matches {
						payload, err := json.Marshal(log.LogRecord)
						if err != nil {
							fmt.Fprintf(resp.Writer, "event: error\n")
							fmt.Fprintf(resp.Writer, "data: %s\n\n", err.Error())
							resp.Writer.(http.Flusher).Flush()

							return nil
						}

						fmt.Fprintf(resp.Writer, "id: %s\n", log.LogKey)
						fmt.Fprintf(resp.Writer, "event: log\n")
						fmt.Fprintf(resp.Writer, "data: %s\n\n", payload)
						resp.Writer.(http.Flusher).Flush()
					}
				}

				return nil
			},
		),
	)

	u.SetName("watch_logs")
	u.SetTitle("Watch Logs")
	u.SetDescription("Server-Sent Events endpoint to watch logs in real time.")
	u.SetTags("streams")

	u.SetExpectedErrors(
		status.PermissionDenied,
		status.InvalidArgument,
		status.FailedPrecondition,
		status.Internal,
	)

	return u
}

// annotateWatchLogs documents the watch response as a Server-Sent Events stream.
func annotateWatchLogs(oc openapi.OperationContext) error {
	contentUnits := oc.Response()
	for i, cu := range contentUnits {
		if cu.HTTPStatus == 200 {
			cu.ContentType = "text/event-stream"
			cu.Description = "Stream of log entries, terminated by a stream-retired event when the watched generation is purged"
			cu.Format = "Server-Sent Events"
		}

		contentUnits[i] = cu
	}

	return nil
}

func init() {
	routing.RegisterOperation(
		NewWatchLogsUsecase,
		http.MethodGet,
		"/api/v1/streams/{stream}/logs/watch",
		routing.Annotated(annotateWatchLogs),
	)
}
