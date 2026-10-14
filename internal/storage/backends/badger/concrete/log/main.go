package log

import (
	"context"
	"log/slog"

	"time"

	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/fx"

	"link-society.com/flowg/internal/engines/lognotify"
	"link-society.com/flowg/internal/models"

	"link-society.com/flowg/internal/storage/backends/badger"

	"link-society.com/flowg/internal/storage/databases/log"
	storage "link-society.com/flowg/internal/storage/interfaces"
)

// Options configures the badger-backed log storage.
type Options struct {
	Directory  string
	InMemory   bool
	ReadOnly   bool
	GCInterval time.Duration
	BatchSize  int
	// StreamConfigOverride, when not nil, is used as the configuration of every
	// stream instead of the stored one.
	StreamConfigOverride *models.StreamConfig
}

type deps struct {
	fx.In

	Logger      *slog.Logger
	Options     Options
	Adapter     *badger.BadgerAdapter `name:"storage.log"`
	LogNotifier lognotify.LogNotifier
}

// DefaultOptions returns the default [Options] for the log storage.
func DefaultOptions() Options {
	return Options{
		Directory:  "",
		InMemory:   false,
		ReadOnly:   false,
		GCInterval: 5 * time.Minute,
	}
}

// adapterOptions returns the options of the underlying badger adapter.
func (o Options) adapterOptions() badger.AdapterOptions {
	return badger.AdapterOptions{
		Directory: o.Directory,
		InMemory:  o.InMemory,
		ReadOnly:  o.ReadOnly,
	}
}

// Module returns an fx module providing a badger-backed [storage.LogStorage]
// configured by the [Options] provided to the graph. It starts a background
// worker that periodically runs the retention garbage collector and another
// that resumes interrupted stream retirement generations, immediately at
// startup and then on every GC interval.
func Module() fx.Option {
	return fx.Module(
		"storage.log",
		badger.NewAdapter("storage.log", Options.adapterOptions),
		fx.Provide(func(lc fx.Lifecycle, d deps) storage.LogStorage {
			store := log.
				NewStorage(d.Logger, d.Adapter, d.Options.BatchSize, d.Options.StreamConfigOverride).
				WithTerminator(d.LogNotifier)

			gc := actor.New(log.NewGarbageCollector(store, d.Options.GCInterval))
			recovery := actor.New(log.NewRetirementRecoveryWorker(store, d.Options.GCInterval))

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					gc.Start()
					recovery.Start()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					gc.Stop()
					recovery.Stop()
					return nil
				},
			})

			return store
		}),
	)
}
