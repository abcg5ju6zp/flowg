package pipelines

import (
	"context"

	"io"

	"github.com/prometheus/common/expfmt"

	"link-society.com/flowg/internal/models"
)

const (
	// DIRECT_ENTRYPOINT is the default source type, used when a record is
	// submitted directly (e.g. via the ingestion API or a parent pipeline).
	DIRECT_ENTRYPOINT = "direct"
	// SYSLOG_ENTRYPOINT is the source type for records arriving from the syslog
	// service.
	SYSLOG_ENTRYPOINT = "syslog"
)

// message is a request handled by the runner actor; each variant knows how to
// service itself against the worker.
type message interface {
	handle(ctx context.Context, w *worker)
}

// logMessage requests that a record be processed by a pipeline. When tracer is
// set the run is a dry run that rebuilds the pipeline from the traced flow and
// records per-node traces.
type logMessage struct {
	replyTo chan<- error

	pipelineName string
	entrypoint   string
	record       *models.LogRecord
	tracer       *NodeTracer
}

// scrapMetricsMessage forwards a request to scrap metrics from a pipeline.
type scrapMetricsMessage struct {
	replyTo chan<- error

	pipelineName string
	w            io.Writer
}

// terminateMessage requests that a pipeline be terminated and removed.
type terminateMessage struct {
	replyTo chan<- error

	pipelineName string
}

// buildResultMessage reports the outcome of a detached candidate build tagged
// with the configuration version it was compiled for; applying it is the only
// path that mutates convergence state.
type buildResultMessage struct {
	name     string
	version  uint64
	pipeline *Pipeline
	err      error
}

// statusMessage requests a snapshot of every pipeline's convergence state.
type statusMessage struct {
	replyTo chan<- []PipelineStatus
}

// reconcileMessage carries a pipeline storage snapshot fetched off the actor
// goroutine in response to a DependenciesChanged event, or the snapshot error
// that should trigger a bounded retry.
type reconcileMessage struct {
	version uint64
	names   []string

	err error
}

// reconcileRetryMessage re-arms a failed reconcile snapshot fetch for the given
// version after the retry interval.
type reconcileRetryMessage struct {
	version uint64
}

// bootBuildResult is one eagerly-built pipeline at startup, tagged with its
// per-item configuration version.
type bootBuildResult struct {
	name     string
	version  uint64
	pipeline *Pipeline
	err      error
}

// bootstrapMessage seeds the actor with the builds performed synchronously
// during startup, so service begins only once every persisted pipeline has been
// compiled (or recorded as an error).
type bootstrapMessage struct {
	version uint64
	results []bootBuildResult
}

var (
	_ message = logMessage{}
	_ message = scrapMetricsMessage{}
	_ message = terminateMessage{}
	_ message = buildResultMessage{}
	_ message = statusMessage{}
	_ message = reconcileMessage{}
	_ message = reconcileRetryMessage{}
	_ message = bootstrapMessage{}
)

func (msg logMessage) handle(ctx context.Context, w *worker) {
	go func() {
		defer close(msg.replyTo)

		ctx := context.WithValue(ctx, workerKey, w)

		var pipeline *Pipeline
		if msg.tracer != nil {
			ctx = WithTracer(ctx, msg.tracer)

			var err error
			pipeline, err = BuildFlow(ctx, w.configStorage, msg.pipelineName, &msg.tracer.Flow)
			if err != nil {
				msg.replyTo <- err
				return
			}

			if err := pipeline.Init(ctx); err != nil {
				_ = pipeline.Close(ctx)
				msg.replyTo <- err
				return
			}
			defer func() { _ = pipeline.Close(ctx) }()
		} else {
			var release func()
			var err error
			pipeline, release, err = w.acquirePipeline(msg.pipelineName)
			if err != nil {
				msg.replyTo <- err
				return
			}
			defer release()
		}

		msg.replyTo <- pipeline.Process(ctx, msg.entrypoint, msg.record)
	}()
}

func (msg scrapMetricsMessage) handle(ctx context.Context, w *worker) {
	go func() {
		defer close(msg.replyTo)

		pipeline, release, err := w.acquirePipeline(msg.pipelineName)
		if err != nil {
			msg.replyTo <- err
			return
		}
		defer release()

		families, err := pipeline.Metrics.Gather()
		if err != nil {
			msg.replyTo <- err
			return
		}

		encoder := expfmt.NewEncoder(msg.w, expfmt.NewFormat(expfmt.TypeTextPlain))

		for _, family := range families {
			if err := encoder.Encode(family); err != nil {
				msg.replyTo <- err
				return
			}
		}

		msg.replyTo <- nil
	}()
}

func (msg terminateMessage) handle(ctx context.Context, w *worker) {
	done, err := w.closePipeline(msg.pipelineName)
	if err != nil {
		msg.replyTo <- err
		close(msg.replyTo)
		return
	}

	// reply once the drain completed, without blocking the actor loop
	go func() {
		msg.replyTo <- <-done
		close(msg.replyTo)
	}()
}

func (msg buildResultMessage) handle(ctx context.Context, w *worker) {
	w.applyBuildResult(ctx, msg)
}

func (msg statusMessage) handle(_ context.Context, w *worker) {
	msg.replyTo <- w.statuses()
	close(msg.replyTo)
}

func (msg reconcileMessage) handle(ctx context.Context, w *worker) {
	w.applyReconcile(ctx, msg)
}

func (msg bootstrapMessage) handle(ctx context.Context, w *worker) {
	w.applyBootstrap(ctx, msg)
}

func (msg reconcileRetryMessage) handle(_ context.Context, w *worker) {
	// a newer version may already have been applied; an in-flight newer
	// snapshot supersedes this retry when its result is applied
	if msg.version <= w.depsVersion {
		return
	}

	w.requestReconcile(nil, msg.version)
}
