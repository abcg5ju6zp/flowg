package pipelines

import (
	"context"
	"errors"

	"fmt"
	"log/slog"

	"io"
	"sync"

	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/fx"

	"link-society.com/flowg/internal/models"

	storage "link-society.com/flowg/internal/storage/interfaces"

	"link-society.com/flowg/internal/engines/confignotify"
	"link-society.com/flowg/internal/engines/lognotify"
)

// bootstrapBuilds eagerly compiles every persisted pipeline concurrently,
// tagging each result with the pipeline's per-item configuration version.
// Build errors are returned per pipeline, never failing the whole startup.
func bootstrapBuilds(
	ctx context.Context,
	configStorage storage.ConfigStorage,
	pipelines []string,
	versions map[string]uint64,
) []bootBuildResult {
	results := make([]bootBuildResult, len(pipelines))

	var wg sync.WaitGroup
	for i, pipelineName := range pipelines {
		wg.Add(1)

		go func(i int, name string) {
			defer wg.Done()

			pipeline, err := buildCandidate(ctx, configStorage, name)

			results[i] = bootBuildResult{
				name:     name,
				version:  versions[name],
				pipeline: pipeline,
				err:      err,
			}
		}(i, pipelineName)
	}
	wg.Wait()

	return results
}

// Runner executes pipelines against incoming log records. It is the public
// entry point of the engine: callers submit a record to a named pipeline and
// the runner drives it through the pipeline's node graph.
//
// The pipeline lifecycle itself is not part of the interface: the stored
// configuration's current version is reconciled eagerly at boot and follows
// every versioned change broadcast on the confignotify bus.
type Runner interface {
	// Process pushes a record through pipelineName, starting at entrypoint (e.g.
	// "direct" or "syslog"), and blocks until processing completes.
	Process(ctx context.Context, pipelineName string, entrypoint string, record *models.LogRecord) error
	// ScrapMetrics forwards a request to scrap metrics from a pipeline.
	ScrapMetrics(ctx context.Context, pipelineName string, w io.Writer) error
	// Status returns the convergence state of every pipeline: the version
	// currently serving, the desired version and the running/pending/error
	// phase, including build failures kept alive by an older build.
	Status(ctx context.Context) ([]PipelineStatus, error)
}

type runnerImpl struct {
	mbox actor.MailboxSender[message]
}

type deps struct {
	fx.In

	Logger         *slog.Logger
	ConfigStorage  storage.ConfigStorage
	LogStorage     storage.LogStorage
	LogNotifier    lognotify.LogNotifier
	ConfigNotifier confignotify.Notifier
}

var _ Runner = (*runnerImpl)(nil)

// NewRunner returns an fx module providing a Runner backed by a single actor.
// The actor owns the pipeline convergence state and build cache; on shutdown
// every active build is invalidated (closing forwarders and transformers)
// before the actor stops.
func NewRunner() fx.Option {
	return fx.Module(
		"pipelineRunner",
		fx.Provide(func(lc fx.Lifecycle) actor.Mailbox[message] {
			mbox := actor.NewMailbox[message]()

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					mbox.Start()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					mbox.Stop()
					return nil
				},
			})

			return mbox
		}),
		fx.Provide(func(lc fx.Lifecycle, d deps, mbox actor.Mailbox[message]) Runner {
			buildCtx, stopBuilds := context.WithCancel(context.Background())

			w := &worker{
				logger:        d.Logger.With(slog.String("channel", "pipelines")),
				mbox:          mbox,
				configStorage: d.ConfigStorage,
				logStorage:    d.LogStorage,
				logNotifier:   d.LogNotifier,
				buildCtx:      buildCtx,

				cache:  make(map[string]*Pipeline),
				states: make(map[string]*pipelineState),
			}
			a := actor.New(w)
			runner := &runnerImpl{mbox: mbox}

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					// the subscription must outlive the start phase; it is torn
					// down with the notifier's own lifecycle. It is armed before
					// the actor starts, so no change event can be missed: events
					// emitted during the boot builds queue in the subscription
					// mailbox and are processed after the bootstrap seed.
					events, err := d.ConfigNotifier.Subscribe(context.Background())
					if err != nil {
						return fmt.Errorf("failed to subscribe to config changes: %w", err)
					}
					w.eventsC = events.ReceiveC()

					// boot reconciliation: snapshot the configuration version
					// and every persisted pipeline synchronously (a read
					// failure keeps failing startup, as before), then eagerly
					// build every pipeline concurrently so the service starts
					// ready to serve. A broken pipeline is reported as error
					// without preventing the others or the server from
					// starting. The actor owns the final state transition.
					globalVersion, err := d.ConfigStorage.ReadConfigVersion(ctx)
					if err != nil {
						return fmt.Errorf("failed to read config version at startup: %w", err)
					}

					pipelines, err := d.ConfigStorage.ListPipelines(ctx)
					if err != nil {
						return fmt.Errorf("failed to start pipelines: %w", err)
					}

					versions, err := d.ConfigStorage.ListItemVersions(ctx, "pipeline")
					if err != nil {
						return fmt.Errorf("failed to read pipeline versions at startup: %w", err)
					}

					results := bootstrapBuilds(ctx, d.ConfigStorage, pipelines, versions)

					// enqueue the bootstrap seed before starting the actor so it
					// is the first message it processes; its boot gate keeps
					// buffered config events behind it
					if err := mbox.Send(ctx, bootstrapMessage{
						version: globalVersion,
						results: results,
					}); err != nil {
						for _, result := range results {
							if result.pipeline != nil {
								_ = result.pipeline.Close(context.Background())
							}
						}
						return fmt.Errorf("failed to seed boot builds: %w", err)
					}

					a.Start()

					return nil
				},
				OnStop: func(ctx context.Context) error {
					stopBuilds()

					w.cacheMu.Lock()
					names := make([]string, 0, len(w.cache))
					for pipelineName := range w.cache {
						names = append(names, pipelineName)
					}
					w.cacheMu.Unlock()

					var errs []error
					for _, pipelineName := range names {
						if err := runner.terminate(ctx, pipelineName); err != nil {
							errs = append(errs, err)
						}
					}
					if len(errs) > 0 {
						return fmt.Errorf("failed to close pipelines: %w", errors.Join(errs...))
					}

					a.Stop()

					return nil
				},
			})

			return runner
		}),
	)
}

// terminate sends a request to the actor to terminate a pipeline. It waits for
// in-flight records to drain, bounded by ctx; the teardown itself always
// completes in the background.
func (r *runnerImpl) terminate(ctx context.Context, pipelineName string) error {
	replyTo := make(chan error, 1)

	err := r.mbox.Send(ctx, terminateMessage{
		replyTo:      replyTo,
		pipelineName: pipelineName,
	})
	if err != nil {
		return err
	}

	select {
	case err := <-replyTo:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Process sends a processing request to the actor and waits for the result. The
// active tracer (if any, set via WithTracer) is forwarded so dry runs can record
// per-node traces.
func (r *runnerImpl) Process(
	ctx context.Context,
	pipelineName string,
	entrypoint string,
	record *models.LogRecord,
) error {
	replyTo := make(chan error, 1)

	err := r.mbox.Send(ctx, logMessage{
		replyTo: replyTo,

		pipelineName: pipelineName,
		entrypoint:   entrypoint,
		record:       record,
		tracer:       GetTracer(ctx),
	})
	if err != nil {
		return err
	}

	select {
	case err := <-replyTo:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ScrapMetrics sends a request to the actor to scrap metrics from a pipeline.
func (r *runnerImpl) ScrapMetrics(
	ctx context.Context,
	pipelineName string,
	w io.Writer,
) error {
	replyTo := make(chan error, 1)

	err := r.mbox.Send(ctx, scrapMetricsMessage{
		replyTo:      replyTo,
		pipelineName: pipelineName,
		w:            w,
	})
	if err != nil {
		return err
	}

	select {
	case err := <-replyTo:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Status sends a request to the actor for the current convergence state of
// every pipeline.
func (r *runnerImpl) Status(ctx context.Context) ([]PipelineStatus, error) {
	replyTo := make(chan []PipelineStatus, 1)

	err := r.mbox.Send(ctx, statusMessage{replyTo: replyTo})
	if err != nil {
		return nil, err
	}

	select {
	case statuses := <-replyTo:
		return statuses, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
