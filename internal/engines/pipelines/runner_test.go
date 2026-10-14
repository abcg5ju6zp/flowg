package pipelines

import (
	"testing"

	"log/slog"

	"errors"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"link-society.com/flowg/internal/models"
	storage "link-society.com/flowg/internal/storage/interfaces"

	"link-society.com/flowg/internal/engines/confignotify"
	"link-society.com/flowg/internal/engines/lognotify"

	badgerconfig "link-society.com/flowg/internal/storage/backends/badger/concrete/config"
	badgerlog "link-society.com/flowg/internal/storage/backends/badger/concrete/log"
)

const rawTestPipeline = `{
	"version": 2,
	"nodes": [
		{
			"id": "source",
			"type": "source",
			"data": {
				"type": "direct"
			}
		}
	],
	"edges": []
}`

func testRunnerModules(configOpts badgerconfig.Options, logOpts badgerlog.Options) []fx.Option {
	return []fx.Option{
		confignotify.NewNotifier(),
		fx.Supply(configOpts),
		badgerconfig.Module(),
		fx.Supply(logOpts),
		badgerlog.Module(),
		lognotify.NewLogNotifier(),
		NewRunner(),
		fx.NopLogger,
	}
}

const brokenRawTestPipeline = `{"version":2,"nodes":[`

func waitForStatus(
	t *testing.T,
	runner Runner,
	pipelineName string,
	match func(PipelineStatus) bool,
) PipelineStatus {
	t.Helper()

	var last PipelineStatus

	waitFor(t, 5*time.Second, func() bool {
		statuses, err := runner.Status(t.Context())
		if err != nil {
			return false
		}
		for _, status := range statuses {
			if status.Name == pipelineName {
				last = status
				return match(status)
			}
		}
		return false
	})

	return last
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition not met before timeout")
}

func TestRunner_StorageDrivenLifecycle(t *testing.T) {
	configOpts := badgerconfig.DefaultOptions()
	configOpts.InMemory = true
	logOpts := badgerlog.DefaultOptions()
	logOpts.InMemory = true

	var (
		runner        Runner
		configStorage storage.ConfigStorage
	)

	app := fxtest.New(
		t,
		fx.Supply(slog.New(slog.DiscardHandler)),
		fx.Options(testRunnerModules(configOpts, logOpts)...),
		fx.Populate(&runner, &configStorage),
	)
	app.RequireStart()
	defer app.RequireStop()

	ctx := t.Context()
	record := models.NewLogRecord(map[string]string{"message": "test"})

	process := func() error {
		return runner.Process(ctx, "test", DIRECT_ENTRYPOINT, record)
	}

	var notFound *PipelineNotFoundError
	if err := process(); !errors.As(err, &notFound) {
		t.Fatalf("expected PipelineNotFoundError before creation, got: %v", err)
	}

	// creating the pipeline in storage starts it
	if err := configStorage.WriteRawPipeline(ctx, "test", rawTestPipeline); err != nil {
		t.Fatalf("failed to write pipeline: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return process() == nil })

	// deleting it from storage terminates it
	if err := configStorage.DeletePipeline(ctx, "test"); err != nil {
		t.Fatalf("failed to delete pipeline: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return errors.As(process(), &notFound)
	})
}

func TestRunner_StartsPersistedPipelinesOnBoot(t *testing.T) {
	dir := t.TempDir()

	configOpts := badgerconfig.DefaultOptions()
	configOpts.Directory = dir
	logOpts := badgerlog.DefaultOptions()
	logOpts.InMemory = true

	// seed the pipeline with a first app instance
	{
		var configStorage storage.ConfigStorage

		app := fxtest.New(
			t,
			fx.Supply(slog.New(slog.DiscardHandler)),
			fx.Options(testRunnerModules(configOpts, logOpts)...),
			fx.Populate(&configStorage),
		)
		app.RequireStart()

		if err := configStorage.WriteRawPipeline(t.Context(), "test", rawTestPipeline); err != nil {
			t.Fatalf("failed to write pipeline: %v", err)
		}

		app.RequireStop()
	}

	// a fresh instance on the same storage starts it eagerly
	var runner Runner

	app := fxtest.New(
		t,
		fx.Supply(slog.New(slog.DiscardHandler)),
		fx.Options(testRunnerModules(configOpts, logOpts)...),
		fx.Populate(&runner),
	)
	app.RequireStart()
	defer app.RequireStop()

	record := models.NewLogRecord(map[string]string{"message": "test"})
	if err := runner.Process(t.Context(), "test", DIRECT_ENTRYPOINT, record); err != nil {
		t.Fatalf("expected pipeline to be running after boot, got: %v", err)
	}
}

func TestRunner_BootWithBrokenPipelineReportsError(t *testing.T) {
	dir := t.TempDir()

	configOpts := badgerconfig.DefaultOptions()
	configOpts.Directory = dir
	logOpts := badgerlog.DefaultOptions()
	logOpts.InMemory = true

	// seed a good and a broken pipeline with a first app instance
	{
		var configStorage storage.ConfigStorage

		app := fxtest.New(
			t,
			fx.Supply(slog.New(slog.DiscardHandler)),
			fx.Options(testRunnerModules(configOpts, logOpts)...),
			fx.Populate(&configStorage),
		)
		app.RequireStart()

		if err := configStorage.WriteRawPipeline(t.Context(), "good", rawTestPipeline); err != nil {
			t.Fatalf("failed to write good pipeline: %v", err)
		}
		if err := configStorage.WriteRawPipeline(t.Context(), "broken", brokenRawTestPipeline); err != nil {
			t.Fatalf("failed to write broken pipeline: %v", err)
		}

		app.RequireStop()
	}

	// a fresh instance serves the good build immediately and exposes the
	// broken one as an error without blocking startup
	var runner Runner

	app := fxtest.New(
		t,
		fx.Supply(slog.New(slog.DiscardHandler)),
		fx.Options(testRunnerModules(configOpts, logOpts)...),
		fx.Populate(&runner),
	)
	app.RequireStart()
	defer app.RequireStop()

	record := models.NewLogRecord(map[string]string{"message": "test"})
	if err := runner.Process(t.Context(), "good", DIRECT_ENTRYPOINT, record); err != nil {
		t.Fatalf("expected good pipeline to serve after boot, got: %v", err)
	}

	broken := waitForStatus(t, runner, "broken", func(status PipelineStatus) bool {
		return status.Phase == PhaseError
	})
	if broken.Serving {
		t.Fatalf("expected broken pipeline to have no serving build, got: %+v", broken)
	}
	if broken.Error == "" {
		t.Fatal("expected the build failure reason to be exposed")
	}

	good := waitForStatus(t, runner, "good", func(status PipelineStatus) bool {
		return status.Phase == PhaseRunning && status.Serving
	})
	if good.ActiveVersion == 0 {
		t.Fatalf("expected good pipeline to be stamped with its config version, got: %+v", good)
	}
}

func TestRunner_FailedSaveKeepsServingThenRecovers(t *testing.T) {
	configOpts := badgerconfig.DefaultOptions()
	configOpts.InMemory = true
	logOpts := badgerlog.DefaultOptions()
	logOpts.InMemory = true

	var (
		runner        Runner
		configStorage storage.ConfigStorage
	)

	app := fxtest.New(
		t,
		fx.Supply(slog.New(slog.DiscardHandler)),
		fx.Options(testRunnerModules(configOpts, logOpts)...),
		fx.Populate(&runner, &configStorage),
	)
	app.RequireStart()
	defer app.RequireStop()

	ctx := t.Context()
	record := models.NewLogRecord(map[string]string{"message": "test"})

	if err := configStorage.WriteRawPipeline(ctx, "flaky", rawTestPipeline); err != nil {
		t.Fatalf("failed to write pipeline: %v", err)
	}
	healthy := waitForStatus(t, runner, "flaky", func(status PipelineStatus) bool {
		return status.Phase == PhaseRunning
	})

	if err := runner.Process(ctx, "flaky", DIRECT_ENTRYPOINT, record); err != nil {
		t.Fatalf("expected healthy pipeline to serve: %v", err)
	}

	// a broken save fails to build: the old build must keep serving, exposed
	// as an error against the newer desired version
	if err := configStorage.WriteRawPipeline(ctx, "flaky", brokenRawTestPipeline); err != nil {
		t.Fatalf("failed to write broken pipeline: %v", err)
	}
	failed := waitForStatus(t, runner, "flaky", func(status PipelineStatus) bool {
		return status.Phase == PhaseError &&
			status.Serving &&
			status.DesiredVersion > status.ActiveVersion
	})
	if failed.ActiveVersion != healthy.ActiveVersion {
		t.Fatalf("expected fallback to stay on version %d, got %d",
			healthy.ActiveVersion, failed.ActiveVersion)
	}
	if err := runner.Process(ctx, "flaky", DIRECT_ENTRYPOINT, record); err != nil {
		t.Fatalf("expected last runnable build to keep serving after failure: %v", err)
	}

	// a fixed save converges the instance back to running
	if err := configStorage.WriteRawPipeline(ctx, "flaky", rawTestPipeline); err != nil {
		t.Fatalf("failed to rewrite healthy pipeline: %v", err)
	}
	recovered := waitForStatus(t, runner, "flaky", func(status PipelineStatus) bool {
		return status.Phase == PhaseRunning &&
			status.ActiveVersion == status.DesiredVersion &&
			status.ActiveVersion > failed.DesiredVersion
	})
	if !recovered.Serving || recovered.Error != "" {
		t.Fatalf("expected recovered status, got: %+v", recovered)
	}
	if err := runner.Process(ctx, "flaky", DIRECT_ENTRYPOINT, record); err != nil {
		t.Fatalf("expected recovered pipeline to serve: %v", err)
	}
}
