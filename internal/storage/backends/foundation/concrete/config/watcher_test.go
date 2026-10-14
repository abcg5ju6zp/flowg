package config

import (
	"crypto/sha256"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/mock"

	"link-society.com/flowg/internal/engines/confignotify"
	confignotifyMocks "link-society.com/flowg/internal/engines/confignotify/mocks"
)

type fakeSystemCache struct {
	invalidated int
}

func (f *fakeSystemCache) InvalidateSystemConfigCache() {
	f.invalidated++
}

// makeSnapshot builds a versioned snapshot; system carries literal values
// hashed into the snapshot shape, item versions are given directly.
func makeSnapshot(
	globalVersion uint64,
	versions map[string]map[string]uint64,
	system map[string]string,
) *snapshot {
	systemHashes := make(map[string][sha256.Size]byte, len(system))
	for name, value := range system {
		systemHashes[name] = sha256.Sum256([]byte(value))
	}

	return &snapshot{
		globalVersion: globalVersion,
		versions:      versions,
		system:        systemHashes,
	}
}

func newTestWatcher() (*watchWorker, *confignotifyMocks.MockNotifier, *fakeSystemCache) {
	notifier := confignotifyMocks.NewMockNotifier().(*confignotifyMocks.MockNotifier)
	cache := &fakeSystemCache{}

	return &watchWorker{
		logger:      slog.New(slog.DiscardHandler),
		notifier:    notifier,
		systemCache: cache,
	}, notifier, cache
}

func TestEmitDiff_SystemChangeInvalidatesCache(t *testing.T) {
	w, notifier, cache := newTestWatcher()

	previous := makeSnapshot(1, nil, map[string]string{"config": "v1"})
	next := makeSnapshot(2, nil, map[string]string{"config": "v2"})

	// no expectation set: any emitted event would fail the test
	w.emitDiff(t.Context(), previous, next)

	if cache.invalidated != 1 {
		t.Fatalf("expected 1 cache invalidation, got %d", cache.invalidated)
	}

	notifier.AssertExpectations(t)
}

func TestEmitDiff_PipelineChanges(t *testing.T) {
	w, notifier, cache := newTestWatcher()

	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:     confignotify.PipelineChanged,
		Pipeline: "updated",
		Version:  2,
	}).Return(nil).Once()
	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:     confignotify.PipelineChanged,
		Pipeline: "added",
		Version:  3,
	}).Return(nil).Once()
	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:     confignotify.PipelineDeleted,
		Pipeline: "removed",
		Version:  7,
	}).Return(nil).Once()

	previous := makeSnapshot(6, map[string]map[string]uint64{
		"pipeline": {"updated": 1, "removed": 4, "untouched": 5},
	}, nil)
	next := makeSnapshot(7, map[string]map[string]uint64{
		"pipeline": {"updated": 2, "added": 3, "untouched": 5},
	}, nil)

	w.emitDiff(t.Context(), previous, next)

	if cache.invalidated != 0 {
		t.Fatalf("expected no cache invalidation, got %d", cache.invalidated)
	}

	notifier.AssertExpectations(t)
}

func TestEmitDiff_DependencyChangeCollapses(t *testing.T) {
	w, notifier, _ := newTestWatcher()

	// a single DependenciesChanged stamped with the global version, even
	// though a pipeline changed too
	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: 7,
	}).Return(nil).Once()

	previous := makeSnapshot(6, map[string]map[string]uint64{
		"transformer": {"parse": 1},
		"pipeline":    {"default": 2},
	}, nil)
	next := makeSnapshot(7, map[string]map[string]uint64{
		"transformer": {"parse": 6},
		"pipeline":    {"default": 7},
	}, nil)

	w.emitDiff(t.Context(), previous, next)

	notifier.AssertExpectations(t)
}

// TestEmitDiff_ReconnectOnlyReportsUnackedVersions covers the watch
// reconnection contract: after re-arming, only items whose version moved past
// the previous snapshot produce events; unchanged items stay silent.
func TestEmitDiff_ReconnectOnlyReportsUnackedVersions(t *testing.T) {
	w, notifier, _ := newTestWatcher()

	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:     confignotify.PipelineChanged,
		Pipeline: "b",
		Version:  4,
	}).Return(nil).Once()

	previous := makeSnapshot(3, map[string]map[string]uint64{
		"pipeline": {"a": 1, "b": 2, "c": 3},
	}, nil)
	next := makeSnapshot(4, map[string]map[string]uint64{
		"pipeline": {"a": 1, "b": 4, "c": 3},
	}, nil)

	w.emitDiff(t.Context(), previous, next)

	notifier.AssertExpectations(t)
}

// TestEmitDiff_LegacyUnstampedItemsStaySilent covers stores that never went
// through a versioned write: absent version maps diff against each other
// without producing an event storm (the first scan primes silently anyway).
func TestEmitDiff_LegacyUnstampedItemsStaySilent(t *testing.T) {
	w, notifier, _ := newTestWatcher()

	previous := makeSnapshot(0, map[string]map[string]uint64{
		"pipeline": {},
	}, nil)
	next := makeSnapshot(0, map[string]map[string]uint64{
		"pipeline": {},
	}, nil)

	w.emitDiff(t.Context(), previous, next)

	notifier.AssertExpectations(t)
}
