package config

import (
	"context"
	"log/slog"

	"maps"

	"crypto/sha256"
	"time"

	"github.com/vladopajic/go-actor/actor"

	"link-society.com/flowg/internal/engines/confignotify"

	"link-society.com/flowg/internal/storage/backends/foundation"
	"link-society.com/flowg/internal/storage/databases/config/transactions"
	"link-society.com/flowg/internal/storage/generic/kv"
)

// watchedItemTypes are the config item prefixes carrying per-item versions the
// watcher diffs to emit lifecycle events; the system item is observed
// separately because it only drives the local system-config cache.
var watchedItemTypes = []string{"pipeline", "transformer", "forwarder"}

// watchBackoff is how long the watcher waits before re-arming after an error.
const watchBackoff = 5 * time.Second

// snapshot is the versioned configuration view at one point in time: the
// global configuration version, per-item version maps and a hash map of the
// system configuration. Items without a version stamp (legacy data) are
// absent from the maps and read as version 0.
type snapshot struct {
	globalVersion uint64
	versions      map[string]map[string]uint64
	system        map[string][sha256.Size]byte
}

// systemConfigCache is the subset of the config storage the watcher needs to
// drop the cached system configuration on remote changes.
type systemConfigCache interface {
	InvalidateSystemConfigCache()
}

// watchWorker observes the config changelog key and translates mutations into
// versioned confignotify events, so every node of the cluster (including the
// one that made the change) reacts to configuration changes through the same
// path.
type watchWorker struct {
	logger      *slog.Logger
	adapter     *foundation.FoundationAdapter
	notifier    confignotify.Notifier
	systemCache systemConfigCache

	current *snapshot
}

var _ actor.Worker = (*watchWorker)(nil)

// DoWork arms the changelog watch, scans the versioned configuration view and
// waits for the watch to fire. Arming the watch before scanning guarantees no
// change is missed in between, and watch coalescing is harmless because the
// diff always compares against stored versions. The first scan primes the
// snapshot silently: the pipeline runner builds everything at boot on its own.
func (w *watchWorker) DoWork(ctx actor.Context) actor.WorkerStatus {
	if ctx.Err() != nil {
		return actor.WorkerEnd
	}

	firedC, err := w.adapter.Watch(ctx, foundation.ChangeLogKey)
	if err != nil {
		w.logError(ctx, "failed to arm config changelog watch", err)
		return w.backoff(ctx)
	}

	next, err := w.scan(ctx)
	if err != nil {
		w.logError(ctx, "failed to scan config items", err)
		return w.backoff(ctx)
	}

	if w.current != nil {
		w.emitDiff(ctx, w.current, next)
	}
	w.current = next

	select {
	case <-ctx.Done():
		return actor.WorkerEnd

	case err := <-firedC:
		if err != nil {
			if ctx.Err() != nil {
				return actor.WorkerEnd
			}

			w.logError(ctx, "config changelog watch failed", err)
			return w.backoff(ctx)
		}

		return actor.WorkerContinue
	}
}

// scan reads the global configuration version, the per-item version maps and
// the hashed system configuration in a single read transaction.
func (w *watchWorker) scan(ctx context.Context) (*snapshot, error) {
	next := &snapshot{
		versions: make(map[string]map[string]uint64, len(watchedItemTypes)),
		system:   make(map[string][sha256.Size]byte),
	}

	err := w.adapter.View(ctx, func(txn *foundation.FoundationQueryTx) error {
		var err error
		next.globalVersion, err = transactions.ReadVersion(txn)
		if err != nil {
			return err
		}

		for _, itemType := range watchedItemTypes {
			versions, err := transactions.ListItemVersions(txn, itemType)
			if err != nil {
				return err
			}
			next.versions[itemType] = versions
		}

		for pair := range txn.IterPairs(kv.Key{"system"}, kv.KeyRange{}) {
			key := pair.Key()
			next.system[key[len(key)-1]] = sha256.Sum256(pair.Value())
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return next, nil
}

// emitDiff broadcasts the versioned difference between two snapshots. A system
// config change drops the local cache; a transformer or forwarder version
// change collapses into a single DependenciesChanged stamped with the global
// version, since the resulting reconcile rebuilds every pipeline anyway;
// pipeline versions changed since the previous snapshot produce per-pipeline
// create/update or delete events. Items whose version did not change emit
// nothing, so a reconnected watch only reports versions the runner has not yet
// acknowledged.
func (w *watchWorker) emitDiff(ctx context.Context, previous *snapshot, next *snapshot) {
	if !maps.Equal(previous.system, next.system) {
		w.systemCache.InvalidateSystemConfigCache()
	}

	for _, itemType := range watchedItemTypes {
		if itemType == "pipeline" {
			continue
		}

		if !maps.Equal(previous.versions[itemType], next.versions[itemType]) {
			w.notify(ctx, confignotify.Event{
				Kind:    confignotify.DependenciesChanged,
				Version: next.globalVersion,
			})
			return
		}
	}

	for name, version := range next.versions["pipeline"] {
		previousVersion, exists := previous.versions["pipeline"][name]
		if !exists || previousVersion != version {
			w.notify(ctx, confignotify.Event{
				Kind:     confignotify.PipelineChanged,
				Pipeline: name,
				Version:  version,
			})
		}
	}

	for name := range previous.versions["pipeline"] {
		if _, exists := next.versions["pipeline"][name]; !exists {
			w.notify(ctx, confignotify.Event{
				Kind:     confignotify.PipelineDeleted,
				Pipeline: name,
				Version:  next.globalVersion,
			})
		}
	}
}

func (w *watchWorker) notify(ctx context.Context, event confignotify.Event) {
	if err := w.notifier.Notify(ctx, event); err != nil {
		w.logError(ctx, "failed to notify config change", err)
	}
}

// backoff waits before retrying after an error, aborting early on shutdown.
func (w *watchWorker) backoff(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case <-time.After(watchBackoff):
		return actor.WorkerContinue
	}
}

func (w *watchWorker) logError(ctx context.Context, message string, err error) {
	w.logger.ErrorContext(
		ctx,
		message,
		"error", err.Error(),
	)
}
