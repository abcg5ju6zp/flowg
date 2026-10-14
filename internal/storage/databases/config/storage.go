package config

import (
	"context"
	"fmt"
	"log/slog"

	"sync"
	"sync/atomic"

	"encoding/json"
	"strings"

	"io"
	"net"

	"link-society.com/flowg/internal/models"

	"link-society.com/flowg/internal/engines/confignotify"

	"link-society.com/flowg/internal/storage/generic/kv"
	storage "link-society.com/flowg/internal/storage/interfaces"

	"link-society.com/flowg/internal/storage/databases/config/transactions"
)

const (
	transformerItemType = "transformer"
	pipelineItemType    = "pipeline"
	forwarderItemType   = "forwarder"
	systemItemType      = "system"
)

// Storage is a backend-agnostic implementation of [storage.ConfigStorage]. It
// runs the config transactions from the transactions subpackage on top of any
// [kv.Adapter] and caches the decoded system configuration in memory.
//
// When a [confignotify.Notifier] is wired, every successful mutation is
// broadcast on it so the pipeline runner can react to configuration changes.
type Storage[QTx kv.QueryTx, MTx kv.MutationTx] struct {
	logger   *slog.Logger
	adapter  kv.Adapter[QTx, MTx]
	notifier confignotify.Notifier

	lock                  *sync.Mutex
	configurationInstance atomic.Pointer[models.SystemConfiguration]
}

var _ storage.ConfigStorage = (*Storage[kv.QueryTx, kv.MutationTx])(nil)

// NewStorage returns a [Storage] that persists configuration data through the
// given key-value adapter. notifier may be nil, in which case no change
// notification is emitted (e.g. on backends with their own change watcher). It
// logs through a logger derived from rootLogger.
func NewStorage[QTx kv.QueryTx, MTx kv.MutationTx](
	rootLogger *slog.Logger,
	adapter kv.Adapter[QTx, MTx],
	notifier confignotify.Notifier,
) *Storage[QTx, MTx] {
	return &Storage[QTx, MTx]{
		logger:   rootLogger.With(slog.String("channel", "storage.config")),
		adapter:  adapter,
		notifier: notifier,
		lock:     &sync.Mutex{},
	}
}

// notify broadcasts a config change when a notifier is wired; the mutation
// already succeeded, so delivery errors are logged instead of returned.
func (s *Storage[QTx, MTx]) notify(ctx context.Context, event confignotify.Event) {
	if s.notifier == nil {
		return
	}

	if err := s.notifier.Notify(ctx, event); err != nil {
		s.logger.ErrorContext(
			ctx,
			"failed to notify config change",
			"kind", event.Kind,
			"pipeline", event.Pipeline,
			"version", event.Version,
			"error", err.Error(),
		)
	}
}

// ReadConfigVersion implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ReadConfigVersion(ctx context.Context) (uint64, error) {
	var version uint64

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		version, err = transactions.ReadVersion(txn)
		return err
	})

	return version, err
}

// ListItemVersions implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ListItemVersions(ctx context.Context, itemType string) (map[string]uint64, error) {
	var versions map[string]uint64

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		versions, err = transactions.ListItemVersions(txn, itemType)
		return err
	})

	return versions, err
}

// Dump implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) Dump(ctx context.Context, w io.Writer, version uint64) (uint64, error) {
	return s.adapter.Backup(ctx, w, version)
}

// Load implements [storage.ConfigStorage].
//
// After the snapshot is loaded, a single version transaction advances the
// global version and stamps every surviving pipeline, transformer and
// forwarder with it, so the restored content is always reconciled even when
// the snapshot carried smaller per-item versions than the running builds.
func (s *Storage[QTx, MTx]) Load(ctx context.Context, r io.Reader) error {
	if err := s.adapter.Restore(ctx, r); err != nil {
		return err
	}

	version, err := s.stampRestoredVersions(ctx)
	if err != nil {
		return err
	}

	s.InvalidateSystemConfigCache()
	s.notify(ctx, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: version,
	})
	return nil
}

// stampRestoredVersions runs the post-restore version transaction: bump the
// global version once, stamp every surviving item with it and drop stamps
// orphaned by the restore, returning the new global version.
func (s *Storage[QTx, MTx]) stampRestoredVersions(ctx context.Context) (uint64, error) {
	var version uint64

	err := s.adapter.Update(ctx, func(txn MTx) error {
		var err error
		version, err = transactions.BumpGlobalVersion(txn)
		if err != nil {
			return err
		}

		for _, itemType := range []string{transformerItemType, pipelineItemType, forwarderItemType} {
			names, err := transactions.ListItems(txn, itemType)
			if err != nil {
				return err
			}

			versions, err := transactions.ListItemVersions(txn, itemType)
			if err != nil {
				return err
			}

			listed := make(map[string]struct{}, len(names))
			for _, name := range names {
				listed[name] = struct{}{}

				if err := transactions.StampItemVersion(txn, itemType, name, version); err != nil {
					return err
				}
			}

			for name := range versions {
				if _, exists := listed[name]; exists {
					continue
				}
				if err := transactions.ClearItemVersion(txn, itemType, name); err != nil {
					return err
				}
			}
		}

		return nil
	})

	return version, err
}

// InvalidateSystemConfigCache drops the in-memory system configuration so the
// next read fetches it from storage again. It is called when the stored
// configuration changed behind the cache: on restore, or when another node of
// a cluster wrote it.
func (s *Storage[QTx, MTx]) InvalidateSystemConfigCache() {
	s.configurationInstance.Store(nil)
}

// ListTransformers implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ListTransformers(ctx context.Context) ([]string, error) {
	return s.listItems(ctx, transformerItemType)
}

// ReadTransformer implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ReadTransformer(ctx context.Context, name string) (*string, error) {
	content, err := s.readItem(ctx, transformerItemType, name)
	if err != nil {
		return nil, err
	}

	if content == nil {
		return nil, nil
	}

	script := string(content)
	return &script, nil
}

// WriteTransformer implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) WriteTransformer(ctx context.Context, name string, content string) error {
	version, err := s.writeItem(ctx, transformerItemType, name, []byte(content))
	if err != nil {
		return err
	}

	s.notify(ctx, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: version,
	})
	return nil
}

// DeleteTransformer implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) DeleteTransformer(ctx context.Context, name string) error {
	version, err := s.deleteItem(ctx, transformerItemType, name)
	if err != nil {
		return err
	}

	s.notify(ctx, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: version,
	})
	return nil
}

// ListPipelines implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ListPipelines(ctx context.Context) ([]string, error) {
	return s.listItems(ctx, pipelineItemType)
}

// ReadPipeline implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ReadPipeline(ctx context.Context, name string) (*models.FlowGraphV2, error) {
	content, err := s.readItem(ctx, pipelineItemType, name)
	if err != nil {
		return nil, err
	}
	if content == nil {
		return nil, nil
	}

	flowGraph, changed, err := models.ConvertFlowGraph(content)
	if err != nil {
		return nil, err
	}

	if changed {
		// If the flow graph was converted to a new version, we should save it back to the storage.
		if err := s.WritePipeline(ctx, name, flowGraph); err != nil {
			return nil, fmt.Errorf("failed to write updated flow graph: %w", err)
		}
	}

	return flowGraph, nil
}

// WritePipeline implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) WritePipeline(ctx context.Context, name string, flow *models.FlowGraphV2) error {
	content, err := json.Marshal(flow)
	if err != nil {
		return fmt.Errorf("failed to marshal flow graph: %w", err)
	}

	version, err := s.writeItem(ctx, pipelineItemType, name, content)
	if err != nil {
		return err
	}

	s.notify(ctx, confignotify.Event{
		Kind:     confignotify.PipelineChanged,
		Pipeline: name,
		Version:  version,
	})
	return nil
}

// WriteRawPipeline implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) WriteRawPipeline(ctx context.Context, name string, content string) error {
	version, err := s.writeItem(ctx, pipelineItemType, name, []byte(content))
	if err != nil {
		return err
	}

	s.notify(ctx, confignotify.Event{
		Kind:     confignotify.PipelineChanged,
		Pipeline: name,
		Version:  version,
	})
	return nil
}

// DeletePipeline implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) DeletePipeline(ctx context.Context, name string) error {
	version, err := s.deleteItem(ctx, pipelineItemType, name)
	if err != nil {
		return err
	}

	s.notify(ctx, confignotify.Event{
		Kind:     confignotify.PipelineDeleted,
		Pipeline: name,
		Version:  version,
	})
	return nil
}

// ListForwarders implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ListForwarders(ctx context.Context) ([]string, error) {
	return s.listItems(ctx, forwarderItemType)
}

// ReadForwarder implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ReadForwarder(ctx context.Context, name string) (*models.ForwarderV2, error) {
	content, err := s.readItem(ctx, forwarderItemType, name)
	if err != nil {
		return nil, err
	}
	if content == nil {
		return nil, nil
	}

	webhook, changed, err := models.ConvertForwarder(content)
	if err != nil {
		return nil, err
	}

	if changed {
		// If the forwarder was converted to a new version, we should save it back
		// to the storage.
		if err := s.WriteForwarder(ctx, name, webhook); err != nil {
			return nil, fmt.Errorf("failed to write updated forwarder: %w", err)
		}
	}

	return webhook, nil
}

// WriteForwarder implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) WriteForwarder(ctx context.Context, name string, forwarder *models.ForwarderV2) error {
	content, err := json.Marshal(forwarder)
	if err != nil {
		return fmt.Errorf("failed to marshal forwarder: %w", err)
	}

	version, err := s.writeItem(ctx, forwarderItemType, name, content)
	if err != nil {
		return err
	}

	s.notify(ctx, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: version,
	})
	return nil
}

// DeleteForwarder implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) DeleteForwarder(ctx context.Context, name string) error {
	version, err := s.deleteItem(ctx, forwarderItemType, name)
	if err != nil {
		return err
	}

	s.notify(ctx, confignotify.Event{
		Kind:    confignotify.DependenciesChanged,
		Version: version,
	})
	return nil
}

// HasSystemConfig implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) HasSystemConfig(ctx context.Context) (bool, error) {
	val, err := s.readItem(ctx, systemItemType, "config")
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

// ReadSystemConfig implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) ReadSystemConfig(ctx context.Context) (*models.SystemConfiguration, error) {
	if config := s.configurationInstance.Load(); config != nil {
		return config, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if config := s.configurationInstance.Load(); config != nil {
		return config, nil
	}

	content, err := s.readItem(ctx, systemItemType, "config")
	if err != nil {
		return nil, err
	}

	config := &models.SystemConfiguration{}
	if content != nil {
		if err := json.Unmarshal(content, config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal system configuration: %w", err)
		}
	}

	s.configurationInstance.Store(config)
	return config, nil
}

// WriteSystemConfig implements [storage.ConfigStorage].
func (s *Storage[QTx, MTx]) WriteSystemConfig(ctx context.Context, config *models.SystemConfiguration) error {
	if config.SyslogAllowedOrigins != nil {
		for _, origin := range config.SyslogAllowedOrigins {
			if strings.Contains(origin, "/") {
				_, _, err := net.ParseCIDR(origin)
				if err != nil {
					return fmt.Errorf("invalid syslog allow origin: %s", origin)
				}
			} else {
				if net.ParseIP(origin) == nil {
					return fmt.Errorf("invalid syslog allow origin: %s", origin)
				}
			}
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.configurationInstance.Store(nil)

	content, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// the system configuration is not part of the pipeline configuration
	// version stream (it does not affect compiled graphs), so it is stored
	// without bumping the global config version
	return s.adapter.Update(ctx, func(txn MTx) error {
		return transactions.WriteItem(txn, systemItemType, "config", content)
	})
}

func (s *Storage[QTx, MTx]) listItems(ctx context.Context, itemType string) ([]string, error) {
	var items []string

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		items, err = transactions.ListItems(txn, itemType)
		return err
	})

	return items, err
}

func (s *Storage[QTx, MTx]) readItem(ctx context.Context, itemType string, name string) ([]byte, error) {
	var content []byte

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		content, err = transactions.ReadItem(txn, itemType, name)
		return err
	})

	return content, err
}

// writeItem stores the item and advances the configuration version in the same
// transaction, returning the new global version.
func (s *Storage[QTx, MTx]) writeItem(ctx context.Context, itemType string, name string, content []byte) (uint64, error) {
	var version uint64

	err := s.adapter.Update(ctx, func(txn MTx) error {
		if err := transactions.WriteItem(txn, itemType, name, content); err != nil {
			return err
		}

		var err error
		version, err = transactions.BumpVersion(txn, itemType, name)
		return err
	})

	return version, err
}

// deleteItem removes the item and its version stamp while advancing the global
// version in the same transaction, returning the new global version.
func (s *Storage[QTx, MTx]) deleteItem(ctx context.Context, itemType string, name string) (uint64, error) {
	var version uint64

	err := s.adapter.Update(ctx, func(txn MTx) error {
		if err := transactions.DeleteItem(txn, itemType, name); err != nil {
			return err
		}

		var err error
		version, err = transactions.BumpGlobalVersion(txn)
		if err != nil {
			return err
		}

		return transactions.ClearItemVersion(txn, itemType, name)
	})

	return version, err
}
