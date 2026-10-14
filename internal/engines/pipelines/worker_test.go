package pipelines

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/vladopajic/go-actor/actor"

	"link-society.com/flowg/internal/models"

	"link-society.com/flowg/internal/engines/confignotify"
	lognotifyMocks "link-society.com/flowg/internal/engines/lognotify/mocks"
	"link-society.com/flowg/internal/storage/generic/kv"
	"link-society.com/flowg/internal/storage/mocks"

	appMetrics "link-society.com/flowg/internal/app/metrics"
)

func TestMain(m *testing.M) {
	appMetrics.Setup()
	m.Run()
}

// minimalFlowGraph returns a flow graph with a single direct source node,
// enough to compile without touching transformers or forwarders.
func minimalFlowGraph() *models.FlowGraphV2 {
	return &models.FlowGraphV2{
		MajorVersion: 2,
		MinorVersion: 1,
		Nodes: []*models.FlowNodeV2{
			{
				ID:   "source",
				Type: "source",
				Data: map[string]string{"type": "direct"},
			},
		},
		Edges: []*models.FlowEdgeV2{},
	}
}

// metricFlowGraph returns a flow graph with a direct source feeding a metric
// node tagged with nodeID, used to tell two candidate builds apart.
func metricFlowGraph(nodeID string) *models.FlowGraphV2 {
	return &models.FlowGraphV2{
		MajorVersion: 2,
		MinorVersion: 1,
		Nodes: []*models.FlowNodeV2{
			{
				ID:   "source",
				Type: "source",
				Data: map[string]string{"type": "direct"},
			},
			{
				ID:   nodeID,
				Type: "metric",
				Data: map[string]string{"name": nodeID},
			},
		},
		Edges: []*models.FlowEdgeV2{
			{ID: "e-" + nodeID, Source: "source", Target: nodeID},
		},
	}
}

// routedFlowGraph returns a flow graph whose direct source feeds a router node,
// so that processing exercises the log storage.
func routedFlowGraph() *models.FlowGraphV2 {
	return &models.FlowGraphV2{
		MajorVersion: 2,
		MinorVersion: 1,
		Nodes: []*models.FlowNodeV2{
			{
				ID:   "source",
				Type: "source",
				Data: map[string]string{"type": "direct"},
			},
			{
				ID:   "router",
				Type: "router",
				Data: map[string]string{"stream": "default"},
			},
		},
		Edges: []*models.FlowEdgeV2{
			{ID: "e1", Source: "source", Target: "router"},
		},
	}
}

func newTestWorker(configStorage *mocks.MockConfigStorage) *worker {
	return &worker{
		logger:        slog.New(slog.DiscardHandler),
		configStorage: configStorage,
		buildCtx:      context.Background(),
		cache:         make(map[string]*Pipeline),
		states:        make(map[string]*pipelineState),
	}
}

// newActorTestWorker starts the worker as a real actor with an event channel
// tests can push confignotify events into.
func newActorTestWorker(t *testing.T, configStorage *mocks.MockConfigStorage) (*worker, actor.Mailbox[message], chan confignotify.Event) {
	t.Helper()

	mbox := actor.NewMailbox[message]()
	mbox.Start()

	eventsC := make(chan confignotify.Event, 4)

	w := &worker{
		logger:        slog.New(slog.DiscardHandler),
		mbox:          mbox,
		eventsC:       eventsC,
		configStorage: configStorage,
		buildCtx:      context.Background(),
		cache:         make(map[string]*Pipeline),
		states:        make(map[string]*pipelineState),
		// unit tests drive events directly; only the fx boot path gates them
		bootstrapped: true,
	}

	a := actor.New(w)
	a.Start()

	t.Cleanup(func() {
		a.Stop()
		mbox.Stop()
	})

	return w, mbox, eventsC
}

// newPausedActorTestWorker builds the worker and mailbox without starting the
// actor, returning a start function, so tests can seed state and dispatch
// builds without racing the actor goroutine.
func newPausedActorTestWorker(t *testing.T, configStorage *mocks.MockConfigStorage) (
	*worker,
	actor.Mailbox[message],
	chan confignotify.Event,
	func(),
) {
	t.Helper()

	mbox := actor.NewMailbox[message]()
	mbox.Start()

	eventsC := make(chan confignotify.Event, 4)

	w := &worker{
		logger:        slog.New(slog.DiscardHandler),
		mbox:          mbox,
		eventsC:       eventsC,
		configStorage: configStorage,
		buildCtx:      context.Background(),
		cache:         make(map[string]*Pipeline),
		states:        make(map[string]*pipelineState),
		bootstrapped:  true,
	}

	a := actor.New(w)

	t.Cleanup(func() {
		a.Stop()
		mbox.Stop()
	})

	return w, mbox, eventsC, a.Start
}

func mustBuildCandidate(t *testing.T, configStorage *mocks.MockConfigStorage, name string) *Pipeline {
	t.Helper()

	pipeline, err := buildCandidate(actor.ContextStarted(), configStorage, name)
	if err != nil {
		t.Fatalf("failed to build candidate %q: %v", name, err)
	}
	return pipeline
}

func seedActive(w *worker, name string, version uint64, pipeline *Pipeline) *pipelineState {
	st := &pipelineState{
		name:           name,
		active:         pipeline,
		activeVersion:  version,
		desiredVersion: version,
	}
	w.states[name] = st
	w.cache[name] = pipeline
	return st
}

func requestStatuses(t *testing.T, mbox actor.MailboxSender[message]) []PipelineStatus {
	t.Helper()

	replyTo := make(chan []PipelineStatus, 1)
	if err := mbox.Send(context.Background(), statusMessage{replyTo: replyTo}); err != nil {
		t.Fatalf("failed to request statuses: %v", err)
	}

	select {
	case statuses := <-replyTo:
		return statuses
	case <-time.After(2 * time.Second):
		t.Fatal("status request timed out")
		return nil
	}
}

func findStatus(statuses []PipelineStatus, name string) (PipelineStatus, bool) {
	for _, status := range statuses {
		if status.Name == name {
			return status, true
		}
	}
	return PipelineStatus{}, false
}

func waitForMboxStatus(
	t *testing.T,
	mbox actor.MailboxSender[message],
	name string,
	match func(PipelineStatus) bool,
) PipelineStatus {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, ok := findStatus(requestStatuses(t, mbox), name)
		if ok && match(status) {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("pipeline %q did not reach expected status before timeout", name)
	return PipelineStatus{}
}

func TestBuildCandidate_UnknownPipeline(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "missing").
		Return((*models.FlowGraphV2)(nil), nil)

	if _, err := buildCandidate(actor.ContextStarted(), configStorage, "missing"); err == nil {
		t.Fatal("expected error for missing pipeline")
	} else {
		var notFound *PipelineNotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("expected PipelineNotFoundError, got: %v", err)
		}
	}
}

func TestBuildCandidate_Success(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Return(minimalFlowGraph(), nil)

	pipeline := mustBuildCandidate(t, configStorage, "test")

	if pipeline == nil {
		t.Fatal("expected compiled pipeline")
	}
	if err := pipeline.Close(actor.ContextStarted()); err != nil {
		t.Fatalf("failed to close candidate: %v", err)
	}
}

func TestApplyBuildResult_SuccessSwapsAndRetires(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Return(minimalFlowGraph(), nil)

	w := newTestWorker(configStorage)

	first := mustBuildCandidate(t, configStorage, "test")
	st := seedActive(w, "test", 1, first)

	second := mustBuildCandidate(t, configStorage, "test")
	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:     "test",
		version:  2,
		pipeline: second,
	})

	if st.active != second || st.activeVersion != 2 || st.desiredVersion != 2 {
		t.Fatalf("unexpected state after swap: %+v", st)
	}
	if w.cache["test"] != second {
		t.Fatal("expected new build in cache")
	}
	if st.phase() != PhaseRunning {
		t.Fatalf("expected phase running, got %s", st.phase())
	}
	if first.acquire() {
		t.Fatal("expected retired build to refuse new uses")
	}
}

func TestApplyBuildResult_FailureKeepsActiveBuild(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "p1").
		Return(minimalFlowGraph(), nil)

	w := newTestWorker(configStorage)

	active := mustBuildCandidate(t, configStorage, "p1")
	st := seedActive(w, "p1", 1, active)
	st.desiredVersion = 2

	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:    "p1",
		version: 2,
		err:     errors.New("compile failed"),
	})

	status := st.snapshot()
	if status.Phase != PhaseError || !status.Serving {
		t.Fatalf("expected error phase with serving fallback, got %+v", status)
	}
	if status.ActiveVersion != 1 || status.DesiredVersion != 2 || status.Error == "" {
		t.Fatalf("unexpected status: %+v", status)
	}
	if w.cache["p1"] != active {
		t.Fatal("expected previous build to keep serving")
	}
	if _, release, err := w.acquirePipeline("p1"); err != nil {
		t.Fatalf("expected previous build to still serve records: %v", err)
	} else {
		release()
	}

	// a later successful build converges back to running
	next := mustBuildCandidate(t, configStorage, "p1")
	st.desiredVersion = 3
	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:     "p1",
		version:  3,
		pipeline: next,
	})

	if st.phase() != PhaseRunning || st.activeVersion != 3 || st.lastError != "" {
		t.Fatalf("expected running v3 after recovery, got %+v", st.snapshot())
	}
}

func TestApplyBuildResult_FailureWithoutActiveBuild(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	w := newTestWorker(configStorage)

	w.states["broken"] = &pipelineState{name: "broken", desiredVersion: 1}

	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:    "broken",
		version: 1,
		err:     errors.New("compile failed"),
	})

	status := w.states["broken"].snapshot()
	if status.Phase != PhaseError || status.Serving {
		t.Fatalf("expected error without serving fallback, got %+v", status)
	}
	if _, _, err := w.acquirePipeline("broken"); err == nil {
		t.Fatal("expected no build to be servable")
	}
}

func TestApplyBuildResult_StaleSuccessIsClosed(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Return(minimalFlowGraph(), nil)

	w := newTestWorker(configStorage)
	st := &pipelineState{name: "test", desiredVersion: 2}
	w.states["test"] = st

	stale := mustBuildCandidate(t, configStorage, "test")
	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:     "test",
		version:  1,
		pipeline: stale,
	})

	if st.active != nil {
		t.Fatal("stale candidate must not enter the cache")
	}
	if _, exists := w.cache["test"]; exists {
		t.Fatal("stale candidate must not be cached")
	}
	if stale.Metrics != nil {
		t.Fatal("stale candidate must be closed")
	}
}

func TestApplyBuildResult_UntrackedCandidateIsClosed(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "ghost").
		Return(minimalFlowGraph(), nil)

	w := newTestWorker(configStorage)

	candidate := mustBuildCandidate(t, configStorage, "ghost")
	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:     "ghost",
		version:  1,
		pipeline: candidate,
	})

	if _, exists := w.cache["ghost"]; exists {
		t.Fatal("untracked candidate must not be cached")
	}
	if candidate.Metrics != nil {
		t.Fatal("untracked candidate must be closed")
	}
}

func TestPipelineChanged_DuplicateVersionBuildsOnce(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Return(minimalFlowGraph(), nil).Once()

	_, mbox, eventsC := newActorTestWorker(t, configStorage)

	eventsC <- confignotify.Event{
		Kind: confignotify.PipelineChanged, Pipeline: "test", Version: 2,
	}
	eventsC <- confignotify.Event{
		Kind: confignotify.PipelineChanged, Pipeline: "test", Version: 2,
	}

	status := waitForMboxStatus(t, mbox, "test", func(s PipelineStatus) bool {
		return s.Phase == PhaseRunning && s.ActiveVersion == 2 && s.DesiredVersion == 2
	})
	if !status.Serving {
		t.Fatal("expected converged pipeline to serve")
	}

	// let a potentially wrongly-queued second build settle, then assert the
	// storage read happened exactly once
	time.Sleep(50 * time.Millisecond)
	configStorage.AssertNumberOfCalls(t, "ReadPipeline", 1)
}

func TestPipelineChanged_OutOfOrderCandidates(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)

	firstEntered := make(chan struct{})
	unblockFirst := make(chan struct{})

	configStorage.On("ReadPipeline", mock.Anything, "test").
		Run(func(args mock.Arguments) {
			close(firstEntered)
			<-unblockFirst
		}).
		Return(metricFlowGraph("metric-old"), nil).Once()
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Return(metricFlowGraph("metric-new"), nil).Once()

	w, mbox, eventsC := newActorTestWorker(t, configStorage)

	eventsC <- confignotify.Event{
		Kind: confignotify.PipelineChanged, Pipeline: "test", Version: 1,
	}
	<-firstEntered

	eventsC <- confignotify.Event{
		Kind: confignotify.PipelineChanged, Pipeline: "test", Version: 2,
	}

	// the v2 build converges before v1 unblocks
	waitForMboxStatus(t, mbox, "test", func(s PipelineStatus) bool {
		return s.ActiveVersion == 2
	})

	close(unblockFirst)

	// after the stale v1 result lands, the v2 build is still the active one
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		w.cacheMu.Lock()
		active := w.cache["test"]
		w.cacheMu.Unlock()
		if active != nil && active.nodes["metric-new"] != nil && active.nodes["metric-old"] == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("newer build was overwritten by the stale v1 candidate")
}

func TestDependenciesChanged_FailureIsolationAndFallback(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ListPipelines", mock.Anything).
		Return([]string{"p1", "p2"}, nil)
	configStorage.On("ReadPipeline", mock.Anything, "p1").
		Return((*models.FlowGraphV2)(nil), errors.New("broken graph"))
	configStorage.On("ReadPipeline", mock.Anything, "p2").
		Return(minimalFlowGraph(), nil)

	// p1 already serves a healthy v1 build before the dependency change
	activeStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	activeStorage.On("ReadPipeline", mock.Anything, "p1").
		Return(minimalFlowGraph(), nil)

	w, mbox, eventsC := newActorTestWorker(t, configStorage)
	seedActive(w, "p1", 1, mustBuildCandidate(t, activeStorage, "p1"))

	eventsC <- confignotify.Event{Kind: confignotify.DependenciesChanged, Version: 5}

	p1 := waitForMboxStatus(t, mbox, "p1", func(s PipelineStatus) bool {
		return s.Phase == PhaseError && s.Serving && s.DesiredVersion == 5 && s.ActiveVersion == 1
	})
	if p1.Error == "" {
		t.Fatal("expected failure reason to be exposed")
	}

	p2 := waitForMboxStatus(t, mbox, "p2", func(s PipelineStatus) bool {
		return s.Phase == PhaseRunning && s.ActiveVersion == 5
	})
	if !p2.Serving {
		t.Fatal("expected p2 to converge independently")
	}

	// records keep flowing through p1's last healthy build
	pipeline, release, err := w.acquirePipeline("p1")
	if err != nil {
		t.Fatalf("expected fallback build to be acquirable: %v", err)
	}
	record := models.NewLogRecord(map[string]string{"message": "test"})
	if err := pipeline.Process(actor.ContextStarted(), DIRECT_ENTRYPOINT, record); err != nil {
		release()
		t.Fatalf("expected fallback build to keep serving: %v", err)
	}
	release()
}

func TestDependenciesChanged_RetiresExtrasAndDedupes(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ListPipelines", mock.Anything).
		Return([]string{"keeper"}, nil).Once()
	configStorage.On("ReadPipeline", mock.Anything, "keeper").
		Return(minimalFlowGraph(), nil)

	ghostStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	ghostStorage.On("ReadPipeline", mock.Anything, "ghost").
		Return(minimalFlowGraph(), nil)

	w, mbox, eventsC := newActorTestWorker(t, configStorage)
	seedActive(w, "ghost", 4, mustBuildCandidate(t, ghostStorage, "ghost"))

	eventsC <- confignotify.Event{Kind: confignotify.DependenciesChanged, Version: 3}

	waitForMboxStatus(t, mbox, "keeper", func(s PipelineStatus) bool {
		return s.Phase == PhaseRunning
	})

	if _, exists := w.states["ghost"]; exists {
		t.Fatal("expected ghost state retired")
	}
	if _, _, err := w.acquirePipeline("ghost"); err == nil {
		t.Fatal("expected ghost build removed from cache")
	}

	// the duplicate notification must not trigger another snapshot/rebuild
	eventsC <- confignotify.Event{Kind: confignotify.DependenciesChanged, Version: 3}
	time.Sleep(100 * time.Millisecond)
	configStorage.AssertNumberOfCalls(t, "ListPipelines", 1)
}

func TestReconcileSnapshotFailureRetries(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ListPipelines", mock.Anything).
		Return(([]string)(nil), errors.New("storage unavailable")).Once()
	configStorage.On("ListPipelines", mock.Anything).
		Return([]string{"keeper"}, nil).Once()
	configStorage.On("ReadPipeline", mock.Anything, "keeper").
		Return(minimalFlowGraph(), nil)

	_, mbox, eventsC := newActorTestWorker(t, configStorage)
	eventsC <- confignotify.Event{Kind: confignotify.DependenciesChanged, Version: 9}

	// the 2s retry must eventually converge
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		status, ok := findStatus(requestStatuses(t, mbox), "keeper")
		if ok && status.Phase == PhaseRunning {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("reconcile did not recover after snapshot failure")
}

func TestPipelineDeleted_TerminatesAndDiscardsLateCandidate(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Return(minimalFlowGraph(), nil)

	w, mbox, eventsC := newActorTestWorker(t, configStorage)
	seedActive(w, "test", 1, mustBuildCandidate(t, configStorage, "test"))

	eventsC <- confignotify.Event{
		Kind: confignotify.PipelineDeleted, Pipeline: "test", Version: 1,
	}

	// observe deletion through mailbox roundtrips: the reply channel gives a
	// happens-after edge with the actor's state writes
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, exists := findStatus(requestStatuses(t, mbox), "test"); !exists {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status, exists := findStatus(requestStatuses(t, mbox), "test"); exists {
		t.Fatalf("deleted pipeline state was not removed: %+v", status)
	}
	if _, _, err := w.acquirePipeline("test"); err == nil {
		t.Fatal("expected deleted build to be gone")
	}

	// a candidate that finished building just after deletion is discarded
	late := mustBuildCandidate(t, configStorage, "test")
	if err := mbox.Send(context.Background(), buildResultMessage{
		name:     "test",
		version:  1,
		pipeline: late,
	}); err != nil {
		t.Fatalf("failed to send late build result: %v", err)
	}

	// a status roundtrip guarantees the late result has been processed before
	// the candidate is inspected from the test goroutine
	if _, exists := findStatus(requestStatuses(t, mbox), "test"); exists {
		t.Fatal("late candidate must not resurrect the deleted pipeline")
	}
	if late.Metrics != nil {
		t.Fatal("late candidate must be closed after deletion")
	}
}

// TestApplyBuildResult_VersionZeroGuards pins the unknown-version semantics:
// a 0 result may adopt an unversioned state but must never overwrite or fail a
// state that already tracks a positive version.
func TestApplyBuildResult_VersionZeroGuards(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, mock.Anything).
		Return(minimalFlowGraph(), nil)

	w := newTestWorker(configStorage)
	active := mustBuildCandidate(t, configStorage, "test")
	st := seedActive(w, "test", 2, active)
	st.desiredVersion = 2

	stale := mustBuildCandidate(t, configStorage, "test")
	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:     "test",
		version:  0,
		pipeline: stale,
	})
	if st.active != active || st.activeVersion != 2 {
		t.Fatal("unknown-version build must not overwrite a versioned active build")
	}
	if stale.Metrics != nil {
		t.Fatal("discarded unknown-version candidate must be closed")
	}

	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:    "test",
		version: 0,
		err:     errors.New("legacy failure"),
	})
	if st.lastError != "" || st.phase() != PhaseRunning {
		t.Fatalf("unknown-version failure must not mark a versioned state: %+v", st.snapshot())
	}

	// an unversioned state adopts unversioned results
	fresh := &pipelineState{name: "unversioned"}
	w.states["unversioned"] = fresh
	first := mustBuildCandidate(t, configStorage, "unversioned")
	w.applyBuildResult(actor.ContextStarted(), buildResultMessage{
		name:     "unversioned",
		version:  0,
		pipeline: first,
	})
	if fresh.active != first || fresh.phase() != PhaseRunning {
		t.Fatalf("expected unversioned state to adopt the v0 build: %+v", fresh.snapshot())
	}
}

func TestApplyBootstrap_SeedsRunningAndError(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "good").
		Return(minimalFlowGraph(), nil)
	configStorage.On("ReadPipeline", mock.Anything, "bad").
		Return((*models.FlowGraphV2)(nil), nil)

	results := bootstrapBuilds(
		actor.ContextStarted(),
		configStorage,
		[]string{"good", "bad"},
		map[string]uint64{"good": 1, "bad": 2},
	)
	if len(results) != 2 {
		t.Fatalf("expected 2 boot results, got %d", len(results))
	}

	w := newTestWorker(configStorage)
	w.applyBootstrap(actor.ContextStarted(), bootstrapMessage{
		version: 2,
		results: results,
	})

	good := w.states["good"].snapshot()
	if good.Phase != PhaseRunning || !good.Serving || good.ActiveVersion != 1 {
		t.Fatalf("expected good pipeline running at v1, got %+v", good)
	}

	bad := w.states["bad"].snapshot()
	if bad.Phase != PhaseError || bad.Serving || bad.DesiredVersion != 2 || bad.Error == "" {
		t.Fatalf("expected broken pipeline error without serving, got %+v", bad)
	}

	if _, _, err := w.acquirePipeline("good"); err != nil {
		t.Fatalf("expected good build to be in cache: %v", err)
	}
	if _, _, err := w.acquirePipeline("bad"); err == nil {
		t.Fatal("expected broken build to be absent from cache")
	}
	if w.depsVersion != 2 {
		t.Fatalf("expected dependency cursor 2 after boot, got %d", w.depsVersion)
	}
}

func TestApplyBootstrap_DoesNotOverrideNewerEventState(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Return(minimalFlowGraph(), nil)

	w := newTestWorker(configStorage)

	// an event-driven build path already tracks a newer version
	w.states["test"] = &pipelineState{name: "test", desiredVersion: 9, building: 1}

	results := bootstrapBuilds(
		actor.ContextStarted(),
		configStorage,
		[]string{"test"},
		map[string]uint64{"test": 1},
	)
	w.applyBootstrap(actor.ContextStarted(), bootstrapMessage{
		version: 1,
		results: results,
	})

	st := w.states["test"]
	if st.active != nil || st.activeVersion != 0 {
		t.Fatal("boot build must not override newer event-driven state")
	}
	if _, exists := w.cache["test"]; exists {
		t.Fatal("discarded boot candidate must not be cached")
	}
	if results[0].pipeline.Metrics != nil {
		t.Fatal("superseded boot candidate must be closed")
	}
}

// TestBootstrap_GatesConfigEventsUntilApplied pins the startup ordering: an
// event already sitting in the subscription mailbox when the actor starts must
// not be processed before the bootstrap seed, otherwise boot builds get
// discarded as "superseded" and the instance is briefly unservable.
func TestBootstrap_GatesConfigEventsUntilApplied(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)

	var calls atomic.Int32
	unblockEventBuild := make(chan struct{})

	configStorage.On("ReadPipeline", mock.Anything, "x").
		Run(func(args mock.Arguments) {
			if calls.Add(1) == 2 {
				<-unblockEventBuild
			}
		}).
		Return(minimalFlowGraph(), nil)

	mbox := actor.NewMailbox[message]()
	mbox.Start()

	eventsC := make(chan confignotify.Event, 1)
	eventsC <- confignotify.Event{
		Kind: confignotify.PipelineChanged, Pipeline: "x", Version: 2,
	}

	w := &worker{
		logger:        slog.New(slog.DiscardHandler),
		mbox:          mbox,
		eventsC:       eventsC,
		configStorage: configStorage,
		buildCtx:      context.Background(),
		cache:         make(map[string]*Pipeline),
		states:        make(map[string]*pipelineState),
		// bootstrapped stays false: the bootstrap message opens the gate
	}

	bootPipeline := mustBuildCandidate(t, configStorage, "x")
	// the boot build was the first storage read; reset the gate accounting so
	// the actor-driven event build is the one that blocks
	calls.Store(0)

	if err := mbox.Send(context.Background(), bootstrapMessage{
		version: 1,
		results: []bootBuildResult{{
			name:     "x",
			version:  1,
			pipeline: bootPipeline,
		}},
	}); err != nil {
		t.Fatalf("failed to seed bootstrap: %v", err)
	}

	a := actor.New(w)
	a.Start()
	t.Cleanup(func() {
		close(unblockEventBuild)
		a.Stop()
		mbox.Stop()
	})

	// while the v2 event build is blocked, the boot build must already serve v1;
	// go through the mailbox so the read races the actor
	replyTo := make(chan []PipelineStatus, 1)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := mbox.Send(context.Background(), statusMessage{replyTo: replyTo}); err != nil {
			t.Fatalf("status send failed: %v", err)
		}
		statuses := <-replyTo
		for _, status := range statuses {
			if status.Name == "x" && status.Serving && status.ActiveVersion == 1 {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("bootstrap seed did not take precedence over the queued config event")
}

func TestStatuses_SortedByName(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	w := newTestWorker(configStorage)

	w.states["b"] = &pipelineState{name: "b", desiredVersion: 1}
	w.states["a"] = &pipelineState{name: "a", desiredVersion: 1}

	statuses := w.statuses()
	if len(statuses) != 2 || statuses[0].Name != "a" || statuses[1].Name != "b" {
		t.Fatalf("expected sorted statuses, got %v", statuses)
	}
}

func TestActorLoop_NotBlockedByInFlightBuild(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)

	entered := make(chan struct{})
	unblock := make(chan struct{})
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Run(func(args mock.Arguments) {
			close(entered)
			<-unblock
		}).
		Return(metricFlowGraph("metric-blocked"), nil)

	t.Cleanup(func() { close(unblock) })

	activeStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	activeStorage.On("ReadPipeline", mock.Anything, "test").
		Return(minimalFlowGraph(), nil)

	w, mbox, _, start := newPausedActorTestWorker(t, configStorage)
	seedActive(w, "test", 1, mustBuildCandidate(t, activeStorage, "test"))

	// seed the pending state before the actor runs, so its state is not
	// mutated from the test goroutine
	st := w.states["test"]
	st.desiredVersion = 2
	st.failedVersion = 1
	st.lastError = "previous failure"
	w.startBuild(st, 2)
	<-entered

	start()

	// the in-flight candidate is visible as pending, and the previous failure
	// stops being the current verdict while it builds
	pending := waitForMboxStatus(t, mbox, "test", func(s PipelineStatus) bool {
		return s.Phase == PhasePending && s.Error == "" &&
			s.DesiredVersion == 2 && s.ActiveVersion == 1
	})
	if !pending.Serving {
		t.Fatal("pending pipeline should keep serving its last build")
	}

	// status must answer while the candidate is blocked
	statusDone := make(chan []PipelineStatus, 1)
	go func() {
		statusDone <- requestStatuses(t, mbox)
	}()
	select {
	case <-statusDone:
	case <-time.After(1 * time.Second):
		t.Fatal("status query blocked by in-flight candidate build")
	}

	// record processing on the active build must answer too
	processDone := make(chan error, 1)
	go func() {
		replyTo := make(chan error, 1)
		_ = mbox.Send(context.Background(), logMessage{
			replyTo:      replyTo,
			pipelineName: "test",
			entrypoint:   DIRECT_ENTRYPOINT,
			record:       models.NewLogRecord(map[string]string{"message": "test"}),
		})
		processDone <- <-replyTo
	}()
	select {
	case err := <-processDone:
		if err != nil {
			t.Fatalf("processing failed while build was in flight: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("record processing blocked by in-flight candidate build")
	}
}

func TestClosePipeline_DrainsBeforeClose(t *testing.T) {
	configStorage := mocks.NewMockConfigStorage().(*mocks.MockConfigStorage)
	configStorage.On("ReadPipeline", mock.Anything, "test").
		Return(routedFlowGraph(), nil)

	entered := make(chan struct{})
	unblock := make(chan struct{})

	logStorage := mocks.NewMockLogStorage().(*mocks.MockLogStorage)
	logStorage.On("Ingest", mock.Anything, "default", mock.Anything).
		Run(func(args mock.Arguments) {
			close(entered)
			<-unblock
		}).
		Return(kv.Key{"k"}, nil)

	logNotifier := lognotifyMocks.NewMockNotifier().(*lognotifyMocks.MockNotifier)
	logNotifier.On("Notify", mock.Anything, "default", mock.Anything, mock.Anything).
		Return(nil)

	w := newTestWorker(configStorage)
	w.logStorage = logStorage
	w.logNotifier = logNotifier

	w.cache["test"] = mustBuildCandidate(t, configStorage, "test")

	// in-flight record, blocked inside the router node
	processDone := make(chan error, 1)
	pipeline, release, err := w.acquirePipeline("test")
	if err != nil {
		t.Fatalf("failed to acquire pipeline: %v", err)
	}
	go func() {
		defer release()
		ctx := context.WithValue(context.Background(), workerKey, w)
		processDone <- pipeline.Process(ctx, DIRECT_ENTRYPOINT, &models.LogRecord{
			Timestamp: time.Now(),
			Fields:    map[string]string{"message": "test"},
		})
	}()

	<-entered

	closeDone, err := w.closePipeline("test")
	if err != nil {
		t.Fatalf("failed to close pipeline: %v", err)
	}

	// new uses are rejected while the record is still in flight
	if _, _, err := w.acquirePipeline("test"); err == nil {
		t.Fatal("expected acquire to fail after close")
	}

	// the close must not complete while the record is in flight
	select {
	case <-closeDone:
		t.Fatal("pipeline closed while a record was in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(unblock)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("failed to close pipeline: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not complete after drain")
	}

	if err := <-processDone; err != nil {
		t.Fatalf("in-flight record failed: %v", err)
	}
}
