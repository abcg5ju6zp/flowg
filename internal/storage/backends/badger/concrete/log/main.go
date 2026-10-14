package log

import (
	"context"
	"log/slog"

	"time"

	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/fx"

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
	// DrainTimeout bounds how long a purge waits for in-flight writes to
	// finish; zero uses the storage default.
	DrainTimeout time.Duration
	// StreamConfigOverride, when not nil, is used as the configuration of every
	// stream instead of the stored one.
	StreamConfigOverride *models.StreamConfig
}

type deps struct {
	fx.In

	Logger  *slog.Logger
	Options Options
	Adapter *badger.BadgerAdapter `name:"storage.log"`
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
// configured by the [Options] provided to the graph. It also starts a
// background worker that periodically runs the value-log garbage collector.
func Module() fx.Option {
	return fx.Module(
		"storage.log",
		badger.NewAdapter("storage.log", Options.adapterOptions),
		fx.Provide(func(lc fx.Lifecycle, d deps) storage.LogStorage {
			storage := log.NewStorage(
				d.Logger,
				d.Adapter,
				d.Options.BatchSize,
				d.Options.DrainTimeout,
				d.Options.StreamConfigOverride,
			)

			gc := actor.New(log.NewGarbageCollector(storage, d.Options.GCInterval))

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					// Resume any purge a previous process did not finish before
					// any service accepts traffic or the GC starts sweeping.
					if err := storage.RecoverRetirements(ctx); err != nil {
						return err
					}

					gc.Start()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					gc.Stop()
					return nil
				},
			})

			return storage
		}),
	)
}
