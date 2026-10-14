package config_test

import (
	"bytes"
	"testing"

	"log/slog"

	"github.com/stretchr/testify/mock"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"link-society.com/flowg/cmd/flowg-server/logging"
	"link-society.com/flowg/internal/models"

	"link-society.com/flowg/internal/engines/confignotify"
	confignotifyMocks "link-society.com/flowg/internal/engines/confignotify/mocks"

	badgerconfig "link-society.com/flowg/internal/storage/backends/badger/concrete/config"
	storage "link-society.com/flowg/internal/storage/interfaces"
)

// startStorage builds an in-memory badger-backed config storage with the given
// notifier mock wired in.
func startStorage(t *testing.T, notifier confignotify.Notifier) (storage.ConfigStorage, *fxtest.App) {
	t.Helper()

	opts := badgerconfig.DefaultOptions()
	opts.InMemory = true

	var configStorage storage.ConfigStorage

	app := fxtest.New(
		t,
		fx.Supply(slog.New(slog.DiscardHandler)),
		fx.Supply(opts),
		badgerconfig.Module(),
		fx.Provide(func() confignotify.Notifier { return notifier }),
		fx.Populate(&configStorage),
		fx.NopLogger,
	)
	app.RequireStart()

	return configStorage, app
}

// TestNotifyOnMutations verifies that every config mutation broadcasts the
// matching confignotify event, carrying the version committed in the same
// transaction.
func TestNotifyOnMutations(t *testing.T) {
	logging.Discard()

	ctx := t.Context()

	notifier := confignotifyMocks.NewMockNotifier().(*confignotifyMocks.MockNotifier)
	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:     confignotify.PipelineChanged,
		Pipeline: "test",
		Version:  1,
	}).Return(nil).Once()
	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:     confignotify.PipelineDeleted,
		Pipeline: "test",
		Version:  2,
	}).Return(nil).Once()
	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: 3,
	}).Return(nil).Once()
	notifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: 4,
	}).Return(nil).Once()

	configStorage, app := startStorage(t, notifier)
	defer app.RequireStop()

	if err := configStorage.WritePipeline(ctx, "test", &models.FlowGraphV2{
		MajorVersion: 2,
		MinorVersion: 1,
		Nodes:        []*models.FlowNodeV2{},
		Edges:        []*models.FlowEdgeV2{},
	}); err != nil {
		t.Fatalf("failed to write pipeline: %v", err)
	}

	if err := configStorage.DeletePipeline(ctx, "test"); err != nil {
		t.Fatalf("failed to delete pipeline: %v", err)
	}

	if err := configStorage.WriteTransformer(ctx, "test", "."); err != nil {
		t.Fatalf("failed to write transformer: %v", err)
	}

	if err := configStorage.DeleteTransformer(ctx, "test"); err != nil {
		t.Fatalf("failed to delete transformer: %v", err)
	}

	notifier.AssertExpectations(t)
}

// TestConfigVersions verifies the global and per-item version semantics:
// strictly increasing global version on every mutation, per-item stamps
// following their item, removal of the stamp with the item, and zero values on
// a legacy/empty store.
func TestConfigVersions(t *testing.T) {
	logging.Discard()

	ctx := t.Context()

	notifier := confignotifyMocks.NewMockNotifier().(*confignotifyMocks.MockNotifier)
	notifier.On("Notify", mock.Anything, mock.AnythingOfType("confignotify.Event")).Return(nil)

	configStorage, app := startStorage(t, notifier)
	defer app.RequireStop()

	version, err := configStorage.ReadConfigVersion(ctx)
	if err != nil {
		t.Fatalf("failed to read config version: %v", err)
	}
	if version != 0 {
		t.Fatalf("expected legacy version 0, got %d", version)
	}

	versions, err := configStorage.ListItemVersions(ctx, "pipeline")
	if err != nil {
		t.Fatalf("failed to list item versions: %v", err)
	}
	if len(versions) != 0 {
		t.Fatalf("expected no stamped items on empty store, got %v", versions)
	}

	flow := &models.FlowGraphV2{
		MajorVersion: 2,
		MinorVersion: 1,
		Nodes:        []*models.FlowNodeV2{},
		Edges:        []*models.FlowEdgeV2{},
	}

	if err := configStorage.WritePipeline(ctx, "first", flow); err != nil {
		t.Fatalf("failed to write first pipeline: %v", err)
	}
	if err := configStorage.WritePipeline(ctx, "second", flow); err != nil {
		t.Fatalf("failed to write second pipeline: %v", err)
	}
	if err := configStorage.WriteForwarder(ctx, "fwd", &models.ForwarderV2{
		Version: 2,
		Config: models.ForwarderConfigV2{
			Http: &models.ForwarderHttpV2{Type: "http", Url: "http://example.invalid"},
		},
	}); err != nil {
		t.Fatalf("failed to write forwarder: %v", err)
	}
	if err := configStorage.DeletePipeline(ctx, "first"); err != nil {
		t.Fatalf("failed to delete first pipeline: %v", err)
	}

	version, err = configStorage.ReadConfigVersion(ctx)
	if err != nil {
		t.Fatalf("failed to read config version: %v", err)
	}
	if version != 4 {
		t.Fatalf("expected global version 4 after 4 mutations, got %d", version)
	}

	versions, err = configStorage.ListItemVersions(ctx, "pipeline")
	if err != nil {
		t.Fatalf("failed to list pipeline versions: %v", err)
	}
	if len(versions) != 1 || versions["second"] != 2 {
		t.Fatalf("expected only second@2 after delete, got %v", versions)
	}

	forwarderVersions, err := configStorage.ListItemVersions(ctx, "forwarder")
	if err != nil {
		t.Fatalf("failed to list forwarder versions: %v", err)
	}
	if len(forwarderVersions) != 1 || forwarderVersions["fwd"] != 3 {
		t.Fatalf("expected fwd@3, got %v", forwarderVersions)
	}
}

// TestRestoreBumpsAndStampsVersions verifies that loading a snapshot into a
// fresh store advances the global version past the snapshot's one, stamps
// every surviving item with the new version (even though the snapshot carried
// smaller per-item versions) and notifies with the new version, so restored
// content is always reconciled.
func TestRestoreBumpsAndStampsVersions(t *testing.T) {
	logging.Discard()

	ctx := t.Context()

	// seed a backup containing a pipeline (v1) and a transformer (v2)
	sourceNotifier := confignotifyMocks.NewMockNotifier().(*confignotifyMocks.MockNotifier)
	sourceNotifier.On("Notify", mock.Anything, mock.Anything).Return(nil)

	source, sourceApp := startStorage(t, sourceNotifier)
	if err := source.WriteRawPipeline(ctx, "kept", `{"version":2,"nodes":[],"edges":[]}`); err != nil {
		t.Fatalf("failed to write pipeline: %v", err)
	}
	if err := source.WriteTransformer(ctx, "kept", "."); err != nil {
		t.Fatalf("failed to write transformer: %v", err)
	}

	var backup bytes.Buffer
	if _, err := source.Dump(ctx, &backup, 0); err != nil {
		t.Fatalf("failed to dump config: %v", err)
	}
	sourceApp.RequireStop()

	// restore into a fresh store: the backup carries global version 2, the
	// restore transaction must advance it to 3 and stamp both items at 3
	restoreNotifier := confignotifyMocks.NewMockNotifier().(*confignotifyMocks.MockNotifier)
	restoreNotifier.On("Notify", mock.Anything, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: 3,
	}).Return(nil).Once()

	target, targetApp := startStorage(t, restoreNotifier)
	defer targetApp.RequireStop()

	if err := target.Load(ctx, bytes.NewReader(backup.Bytes())); err != nil {
		t.Fatalf("failed to restore config: %v", err)
	}

	version, err := target.ReadConfigVersion(ctx)
	if err != nil {
		t.Fatalf("failed to read config version: %v", err)
	}
	if version != 3 {
		t.Fatalf("expected global version 3 after restore, got %d", version)
	}

	expected := map[string]map[string]uint64{
		"pipeline":    {"kept": 3},
		"transformer": {"kept": 3},
	}
	for itemType, expectedVersions := range expected {
		got, err := target.ListItemVersions(ctx, itemType)
		if err != nil {
			t.Fatalf("failed to list %s versions: %v", itemType, err)
		}
		if len(got) != len(expectedVersions) {
			t.Fatalf("expected %d stamped %s, got %v", len(expectedVersions), itemType, got)
		}
		for name, v := range expectedVersions {
			if got[name] != v {
				t.Fatalf("expected %s %q stamped at %d, got %v", itemType, name, v, got)
			}
		}
	}

	names, err := target.ListPipelines(ctx)
	if err != nil {
		t.Fatalf("failed to list pipelines: %v", err)
	}
	if len(names) != 1 || names[0] != "kept" {
		t.Fatalf("expected only restored pipeline 'kept', got %v", names)
	}

	restoreNotifier.AssertExpectations(t)
}
