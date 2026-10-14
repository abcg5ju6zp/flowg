package pipelines

import (
	"context"
	"log/slog"

	"sort"
	"sync"
	"time"

	"github.com/vladopajic/go-actor/actor"

	storage "link-society.com/flowg/internal/storage/interfaces"

	"link-society.com/flowg/internal/engines/confignotify"
	"link-society.com/flowg/internal/engines/lognotify"
)

// reconcileRetryInterval bounds how long a dependency reconcile waits before
// retrying a failed storage snapshot.
const reconcileRetryInterval = 2 * time.Second

// worker is the runner's actor body. It owns the convergence state of every
// pipeline and the cache of active (serving) builds. Candidate builds run on
// detached goroutines and report back through the mailbox, so storage I/O,
// script compilation and forwarder connects never block the actor loop.
type worker struct {
	logger *slog.Logger

	mbox actor.Mailbox[message]

	// eventsC delivers config change notifications; pipeline lifecycle follows
	// storage mutations, wherever they were initiated.
	eventsC <-chan confignotify.Event

	configStorage storage.ConfigStorage
	logStorage    storage.LogStorage
	logNotifier   lognotify.LogNotifier

	// buildCtx is cancelled on shutdown; every detached candidate build and
	// reconcile snapshot derives from it.
	buildCtx context.Context

	// cache holds the active builds keyed by pipeline name; it is also read by
	// processing goroutines, hence its own lock.
	cache   map[string]*Pipeline
	cacheMu sync.Mutex

	// states is the per-pipeline convergence state, touched only on the actor
	// goroutine (including buildResult/reconcile messages).
	states map[string]*pipelineState

	// bootstrapped gates event processing until the boot snapshot is applied:
	// the bootstrap message is enqueued before the actor starts, so config
	// events emitted during the boot builds (e.g. a migration write-back) stay
	// buffered in the subscription mailbox and are processed afterwards, in
	// version order.
	bootstrapped bool

	// depsVersion is the global version of the last applied dependency
	// reconcile, initialized at boot from the storage's current version.
	depsVersion uint64
	// reconcilingVersion is the highest version whose dependency snapshot is
	// currently being fetched; it lets duplicate notifications be skipped while
	// a reconcile is already in flight.
	reconcilingVersion uint64
}

var _ actor.Worker = (*worker)(nil)

func (w *worker) DoWork(ctx actor.Context) actor.WorkerStatus {
	if !w.bootstrapped {
		// the bootstrap message is the first mailbox message; config events
		// wait in their own subscription mailbox until it is applied
		select {
		case <-ctx.Done():
			return actor.WorkerEnd

		case msg, ok := <-w.mbox.ReceiveC():
			if !ok {
				return actor.WorkerEnd
			}

			msg.handle(ctx, w)

			return actor.WorkerContinue
		}
	}

	select {
	case <-ctx.Done():
		return actor.WorkerEnd

	case msg, ok := <-w.mbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}

		msg.handle(ctx, w)

		return actor.WorkerContinue

	case event, ok := <-w.eventsC:
		if !ok {
			// subscription torn down at shutdown; a nil channel blocks forever
			w.eventsC = nil
			return actor.WorkerContinue
		}

		w.handleEvent(ctx, event)

		return actor.WorkerContinue
	}
}

// handleEvent maps a versioned config change to the matching lifecycle action.
// All bookkeeping happens on the actor goroutine; the heavy work is detached.
func (w *worker) handleEvent(ctx actor.Context, event confignotify.Event) {
	switch event.Kind {
	case confignotify.PipelineChanged:
		w.onPipelineChanged(ctx, event)

	case confignotify.PipelineDeleted:
		w.onPipelineDeleted(ctx, event)

	case confignotify.DependenciesChanged:
		w.onDependenciesChanged(ctx, event)
	}
}

// onPipelineChanged starts a candidate build tagged with the event version; an
// event for a version already observed (duplicate notification, watch
// reconnection) is skipped.
func (w *worker) onPipelineChanged(_ actor.Context, event confignotify.Event) {
	st := w.states[event.Pipeline]
	if st == nil {
		st = &pipelineState{name: event.Pipeline}
		w.states[event.Pipeline] = st
	} else if event.Version != 0 && event.Version <= st.desiredVersion {
		w.logger.DebugContext(
			w.buildCtx,
			"ignoring duplicate pipeline change",
			"pipeline", event.Pipeline,
			"version", event.Version,
		)
		return
	}

	w.startBuild(st, event.Version)
}

// onPipelineDeleted retires the instance and drops its state; an event older
// than the latest known version is a stale, out-of-order notification.
func (w *worker) onPipelineDeleted(ctx actor.Context, event confignotify.Event) {
	st := w.states[event.Pipeline]
	if st != nil && event.Version != 0 && event.Version < st.desiredVersion {
		w.logger.DebugContext(
			ctx,
			"ignoring stale pipeline deletion",
			"pipeline", event.Pipeline,
			"version", event.Version,
		)
		return
	}

	delete(w.states, event.Pipeline)

	done, err := w.closePipeline(event.Pipeline)
	if err != nil {
		w.logger.DebugContext(
			ctx,
			"no running build for deleted pipeline",
			"pipeline", event.Pipeline,
			"version", event.Version,
			"error", err.Error(),
		)
		return
	}

	w.logger.InfoContext(
		ctx,
		"terminated pipeline on deletion",
		"pipeline", event.Pipeline,
		"version", event.Version,
	)

	go func(version uint64) {
		if err := <-done; err != nil {
			w.logger.ErrorContext(
				w.buildCtx,
				"failed to close deleted pipeline build",
				"pipeline", event.Pipeline,
				"version", version,
				"error", err.Error(),
			)
		}
	}(event.Version)
}

// onDependenciesChanged accepts a dependency reconcile only when its version is
// newer than the applied and in-flight ones; the storage snapshot itself is
// fetched off the actor goroutine.
func (w *worker) onDependenciesChanged(ctx actor.Context, event confignotify.Event) {
	if event.Version != 0 {
		switch {
		case event.Version <= w.depsVersion:
			w.logger.DebugContext(
				ctx,
				"ignoring duplicate dependency change",
				"version", event.Version,
			)
			return

		case event.Version <= w.reconcilingVersion:
			w.logger.DebugContext(
				ctx,
				"ignoring dependency change while reconcile is in flight",
				"version", event.Version,
			)
			return
		}
	}

	w.requestReconcile(ctx, event.Version)
}

// requestReconcile fetches the current pipeline set and its per-item versions
// off the actor goroutine and posts the snapshot back as a reconcileMessage.
func (w *worker) requestReconcile(_ actor.Context, version uint64) {
	if version > w.reconcilingVersion {
		w.reconcilingVersion = version
	}

	w.logger.InfoContext(
		w.buildCtx,
		"resolving pipeline dependencies",
		"version", version,
	)

	go func() {
		names, err := w.configStorage.ListPipelines(w.buildCtx)
		w.postReconcile(reconcileMessage{
			version: version,
			names:   names,
			err:     err,
		})
	}()
}

// postReconcile delivers a fetched snapshot to the actor; when the mailbox is
// stopped (shutdown) the message is simply dropped.
func (w *worker) postReconcile(msg reconcileMessage) {
	if err := w.mbox.Send(w.buildCtx, msg); err != nil {
		w.logger.DebugContext(
			w.buildCtx,
			"dropped reconcile result while stopping",
			"version", msg.version,
		)
	}
}

// applyReconcile converges the running instances with a storage snapshot:
// every listed pipeline gets a fresh candidate build (transformer/forwarder
// dependencies may have changed) and builds missing from storage are retired.
// Each build is independent, so a single failure cannot block the others.
func (w *worker) applyReconcile(ctx actor.Context, msg reconcileMessage) {
	if msg.err != nil {
		w.logger.ErrorContext(
			ctx,
			"failed to snapshot pipelines during reconcile",
			"version", msg.version,
			"error", msg.err.Error(),
		)

		// retry the latest pending version once; superseding versions replace it
		if msg.version == w.reconcilingVersion && msg.version > w.depsVersion {
			scheduleReconcileRetry(w, msg.version)
		}
		return
	}

	if msg.version != 0 &&
		(msg.version < w.depsVersion || msg.version < w.reconcilingVersion) {
		w.logger.DebugContext(
			ctx,
			"discarding stale reconcile snapshot",
			"version", msg.version,
			"applied_version", w.depsVersion,
			"inflight_version", w.reconcilingVersion,
		)
	} else {
		listed := make(map[string]struct{}, len(msg.names))
		for _, name := range msg.names {
			listed[name] = struct{}{}

			st := w.states[name]
			if st == nil {
				st = &pipelineState{name: name}
				w.states[name] = st
			}

			// a dependency change may affect every compiled build, so each
			// listed pipeline is rebuilt tagged with the global event version;
			// an unknown (0) version rebuilds against the version already
			// tracked, so its result is never considered stale
			target := msg.version
			if target == 0 {
				target = st.desiredVersion
			}
			w.startBuild(st, target)
		}

		for name := range w.states {
			if _, exists := listed[name]; exists {
				continue
			}

			delete(w.states, name)

			done, err := w.closePipeline(name)
			if err != nil {
				w.logger.DebugContext(
					ctx,
					"no running build for pipeline missing from storage",
					"pipeline", name,
					"error", err.Error(),
				)
				continue
			}

			w.logger.InfoContext(
				ctx,
				"retired pipeline missing from storage",
				"pipeline", name,
				"version", msg.version,
			)

			go func(name string, done <-chan error) {
				if err := <-done; err != nil {
					w.logger.ErrorContext(
						w.buildCtx,
						"failed to close retired pipeline build",
						"pipeline", name,
						"version",  msg.version,
						"error",    err.Error(),
					)
				}
			}(name, done)
		}

		// only an applied snapshot advances the cursors; a discarded stale
		// snapshot leaves the newer in-flight or applied version authoritative
		if msg.version >= w.depsVersion {
			w.depsVersion = msg.version
		}
		if w.reconcilingVersion == msg.version {
			w.reconcilingVersion = 0
		}
	}
}

// applyBootstrap seeds convergence state from the builds performed
// synchronously during startup. Pipelines that an early config event already
// started tracking are left to that event-driven path; the boot candidate is
// discarded when stale.
func (w *worker) applyBootstrap(ctx actor.Context, msg bootstrapMessage) {
	for _, result := range msg.results {
		if st := w.states[result.name]; st != nil {
			w.logger.DebugContext(
				ctx,
				"discarding boot build superseded by a config event",
				"pipeline", result.name,
				"version", result.version,
			)
			if result.pipeline != nil {
				if err := result.pipeline.Close(ctx); err != nil {
					w.logger.ErrorContext(
						ctx,
						"failed to close superseded boot build",
						"pipeline", result.name,
						"version", result.version,
						"error", err.Error(),
					)
				}
			}
			continue
		}

		st := &pipelineState{name: result.name, desiredVersion: result.version}

		if result.err != nil {
			st.failedVersion = result.version
			st.lastError = result.err.Error()

			w.logger.ErrorContext(
				ctx,
				"failed to start pipeline at boot",
				"pipeline", result.name,
				"version", result.version,
				"error", result.err.Error(),
			)
		} else {
			st.active = result.pipeline
			st.activeVersion = result.version

			w.cacheMu.Lock()
			w.cache[result.name] = result.pipeline
			w.cacheMu.Unlock()

			w.logger.InfoContext(
				ctx,
				"started pipeline at boot",
				"pipeline", result.name,
				"version", result.version,
			)
		}

		w.states[result.name] = st
	}

	if msg.version > w.depsVersion {
		w.depsVersion = msg.version
	}

	// unblock event processing: the subscription mailbox already holds any
	// event emitted during the boot builds, and it is drained after this point
	w.bootstrapped = true
}

// scheduleReconcileRetry re-arms the snapshot fetch after the retry interval,
// unless the worker is stopping in the meantime.
func scheduleReconcileRetry(w *worker, version uint64) {
	go func() {
		timer := time.NewTimer(reconcileRetryInterval)
		defer timer.Stop()

		select {
		case <-w.buildCtx.Done():
			return
		case <-timer.C:
			if err := w.mbox.Send(w.buildCtx, reconcileRetryMessage{version: version}); err != nil {
				w.logger.DebugContext(
					w.buildCtx,
					"dropped reconcile retry while stopping",
					"version", version,
				)
			}
		}
	}()
}

// startBuild compiles a candidate for st tagged with version on a detached
// goroutine; the result is applied on the actor through buildResultMessage.
func (w *worker) startBuild(st *pipelineState, version uint64) {
	st.building++

	if version > st.desiredVersion {
		st.desiredVersion = version
	}

	w.logger.InfoContext(
		w.buildCtx,
		"building pipeline candidate",
		"pipeline", st.name,
		"version", version,
	)

	go func() {
		pipeline, err := buildCandidate(w.buildCtx, w.configStorage, st.name)

		result := buildResultMessage{
			name:     st.name,
			version:  version,
			pipeline: pipeline,
			err:      err,
		}

		if err := w.mbox.Send(w.buildCtx, result); err != nil {
			// the actor mailbox is stopped (shutdown): nobody will apply or
			// close this candidate, so release its resources here
			if pipeline != nil {
				if closeErr := pipeline.Close(context.Background()); closeErr != nil {
					w.logger.ErrorContext(
						context.Background(),
						"failed to close orphan candidate build",
						"pipeline", st.name,
						"version", version,
						"error", closeErr.Error(),
					)
				}
			}
		}
	}()
}

// applyBuildResult is the single place where a candidate outcome mutates the
// convergence state. Stale results (older than the desired version) and
// results for pipelines deleted while building are discarded, and their
// candidate closed; failures keep the previous active build serving.
func (w *worker) applyBuildResult(ctx actor.Context, msg buildResultMessage) {
	st := w.states[msg.name]
	if st == nil {
		w.logger.DebugContext(
			ctx,
			"discarding build result for untracked pipeline",
			"pipeline", msg.name,
			"version", msg.version,
		)
		w.discardCandidate(ctx, msg)
		return
	}

	st.building--

	if msg.err != nil {
		// an unknown (0) result only speaks for a state that never tracked a
		// version; against a versioned desired state it is stale
		if msg.version < st.desiredVersion {
			w.logger.DebugContext(
				ctx,
				"ignoring failure of stale candidate build",
				"pipeline", msg.name,
				"version", msg.version,
				"desired_version", st.desiredVersion,
			)
			return
		}

		st.failedVersion = msg.version
		st.lastError = msg.err.Error()

		w.logger.ErrorContext(
			ctx,
			"pipeline candidate build failed; keeping last running build",
			"pipeline", msg.name,
			"version", msg.version,
			"serving", st.active != nil,
			"error", msg.err.Error(),
		)
		return
	}

	// an unknown (0) result may only adopt an unversioned desired state
	if msg.version < st.desiredVersion {
		w.logger.DebugContext(
			ctx,
			"discarding successful but stale candidate build",
			"pipeline", msg.name,
			"version", msg.version,
			"desired_version", st.desiredVersion,
		)
		if err := msg.pipeline.Close(ctx); err != nil {
			w.logger.ErrorContext(
				ctx,
				"failed to close stale candidate build",
				"pipeline", msg.name,
				"version", msg.version,
				"error", err.Error(),
			)
		}
		return
	}

	previous := st.active

	w.cacheMu.Lock()
	w.cache[msg.name] = msg.pipeline
	w.cacheMu.Unlock()

	st.active = msg.pipeline
	st.activeVersion = msg.version
	st.failedVersion = 0
	st.lastError = ""
	if st.desiredVersion < msg.version {
		st.desiredVersion = msg.version
	}

	w.logger.InfoContext(
		ctx,
		"switched pipeline to new build",
		"pipeline", msg.name,
		"version", msg.version,
	)

	if previous != nil {
		done := w.retirePipeline(previous)
		go func() {
			if err := <-done; err != nil {
				w.logger.ErrorContext(
					context.Background(),
					"failed to close previous pipeline build",
					"pipeline", msg.name,
					"version", msg.version,
					"error", err.Error(),
				)
			}
		}()
	}
}

// discardCandidate releases a candidate that must not enter the cache because
// its pipeline is no longer tracked.
func (w *worker) discardCandidate(ctx actor.Context, msg buildResultMessage) {
	if msg.pipeline == nil {
		return
	}

	if err := msg.pipeline.Close(ctx); err != nil {
		w.logger.ErrorContext(
			ctx,
			"failed to close discarded candidate build",
			"pipeline", msg.name,
			"version", msg.version,
			"error", err.Error(),
		)
	}
}

// statuses returns the convergence status of every tracked pipeline, sorted by
// name; it executes on the actor goroutine and needs no locking.
func (w *worker) statuses() []PipelineStatus {
	statuses := make([]PipelineStatus, 0, len(w.states))
	for _, st := range w.states {
		statuses = append(statuses, st.snapshot())
	}

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Name < statuses[j].Name
	})

	return statuses
}

// acquirePipeline returns the cached build with an in-flight reservation so a
// concurrent retire cannot close it mid-use; callers must call release.
func (w *worker) acquirePipeline(pipelineName string) (*Pipeline, func(), error) {
	w.cacheMu.Lock()
	pipeline, exists := w.cache[pipelineName]
	w.cacheMu.Unlock()

	if !exists || !pipeline.acquire() {
		return nil, nil, &PipelineNotFoundError{Pipeline: pipelineName}
	}

	return pipeline, pipeline.release, nil
}

// retirePipeline marks a build closed and closes it in the background once its
// in-flight uses drained; the returned channel reports the close error.
func (w *worker) retirePipeline(pipeline *Pipeline) <-chan error {
	pipeline.stateMu.Lock()
	pipeline.closed = true
	pipeline.stateMu.Unlock()

	done := make(chan error, 1)
	go func() {
		pipeline.inflight.Wait()
		done <- pipeline.Close(context.Background())
		close(done)
	}()

	return done
}

// closePipeline removes a build from the worker's cache and retires it; the
// returned channel reports the close error once in-flight work drained.
func (w *worker) closePipeline(pipelineName string) (<-chan error, error) {
	w.cacheMu.Lock()
	pipeline, exists := w.cache[pipelineName]
	delete(w.cache, pipelineName)
	w.cacheMu.Unlock()

	if !exists {
		return nil, &PipelineNotFoundError{Pipeline: pipelineName}
	}

	return w.retirePipeline(pipeline), nil
}

// buildCandidate loads the persisted flow graph, compiles it and initializes
// the node graph. It performs no worker state mutation, so it is safe to run
// off the actor goroutine. A missing pipeline yields a typed not-found error;
// a failed initialization closes the partially built candidate.
func buildCandidate(ctx context.Context, configStorage storage.ConfigStorage, pipelineName string) (*Pipeline, error) {
	pipeline, err := BuildFromStorage(ctx, configStorage, pipelineName)
	if err != nil {
		return nil, err
	}
	if pipeline == nil {
		return nil, &PipelineNotFoundError{Pipeline: pipelineName}
	}

	if err := pipeline.Init(ctx); err != nil {
		_ = pipeline.Close(ctx)
		return nil, err
	}

	return pipeline, nil
}
