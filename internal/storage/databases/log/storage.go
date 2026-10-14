package log

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"sync"
	"time"

	"link-society.com/flowg/internal/models"

	"link-society.com/flowg/internal/storage/generic/kv"
	storage "link-society.com/flowg/internal/storage/interfaces"

	"link-society.com/flowg/internal/storage/databases/log/transactions"

	"link-society.com/flowg/internal/utils/langs/filtering"
)

// defaultBatchSize bounds how many keys a whole-stream maintenance operation
// (delete, index back-fill, retention eviction) touches per transaction, so a
// single pass stays within a backend's transaction size and time limits.
const defaultBatchSize = 1000

// Storage is a backend-agnostic implementation of [storage.LogStorage]. It runs
// the log transactions from the transactions subpackage on top of any
// [kv.Adapter].
type Storage[QTx kv.QueryTx, MTx kv.MutationTx] struct {
	logger               *slog.Logger
	adapter              kv.Adapter[QTx, MTx]
	batchSize            int
	drainTimeout         time.Duration
	streamConfigOverride *models.StreamConfig

	// gates freezes local writers of retiring generations and drains the
	// writes that were already in flight when the fence landed.
	gates *retirementGates
	// retireLocks serializes the Fence→Finish window per stream so duplicate
	// purges cannot open two generations concurrently.
	retireLocksMu sync.Mutex
	retireLocks   map[string]*sync.Mutex
}

var _ storage.LogStorage = (*Storage[kv.QueryTx, kv.MutationTx])(nil)

// NewStorage returns a [Storage] that persists log data through the given
// key-value adapter. batchSize bounds how many keys whole-stream operations
// touch per transaction; a value of zero or less uses [defaultBatchSize].
// drainTimeout bounds how long a purge waits for in-flight writes to finish; a
// value of zero or less uses [defaultDrainTimeout]. When streamConfigOverride
// is not nil, it is used as the configuration of every stream instead of the
// stored one (e.g. to enforce a short retention in demo mode). It logs through
// a logger derived from rootLogger.
func NewStorage[QTx kv.QueryTx, MTx kv.MutationTx](
	rootLogger *slog.Logger,
	adapter kv.Adapter[QTx, MTx],
	batchSize int,
	drainTimeout time.Duration,
	streamConfigOverride *models.StreamConfig,
) *Storage[QTx, MTx] {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if drainTimeout <= 0 {
		drainTimeout = defaultDrainTimeout
	}

	return &Storage[QTx, MTx]{
		logger:               rootLogger.With(slog.String("channel", "logstorage")),
		adapter:              adapter,
		batchSize:            batchSize,
		drainTimeout:         drainTimeout,
		streamConfigOverride: streamConfigOverride,
		gates:                newRetirementGates(),
		retireLocks:          map[string]*sync.Mutex{},
	}
}

// Dump implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) Dump(ctx context.Context, w io.Writer, since uint64) (uint64, error) {
	return s.adapter.Backup(ctx, w, since)
}

// Load implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) Load(ctx context.Context, r io.Reader) error {
	return s.adapter.Restore(ctx, r)
}

// ListStreamConfigs implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) ListStreamConfigs(ctx context.Context) (map[string]models.StreamConfig, error) {
	var streams map[string]models.StreamConfig

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		streams, err = transactions.FetchStreamConfigs(txn, s.streamConfigOverride)
		return err
	})
	if err != nil {
		return nil, err
	}

	return streams, nil
}

// ListStreamFields implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) ListStreamFields(ctx context.Context, stream string) ([]string, error) {
	var fields []string

	err := s.adapter.View(ctx, func(txn QTx) error {
		fields = transactions.FetchStreamFields(txn, stream)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return fields, nil
}

// GetOrCreateStreamConfig implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) GetOrCreateStreamConfig(ctx context.Context, stream string) (models.StreamConfig, error) {
	// A retiring generation must not be lazily recreated by a GET. The
	// transaction performs the same check authoritatively.
	if err := s.gates.rejectIfRetiring(stream); err != nil {
		return models.StreamConfig{}, err
	}

	var streamConfig models.StreamConfig

	err := s.adapter.Update(ctx, func(txn MTx) error {
		var err error
		streamConfig, err = transactions.GetOrCreateStreamConfig(txn, stream, s.streamConfigOverride)
		return err
	})

	if err != nil {
		return models.StreamConfig{}, err
	}

	return streamConfig, nil
}

// ConfigureStream implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) ConfigureStream(ctx context.Context, stream string, config models.StreamConfig) error {
	// A retiring generation rejects configuration changes instead of being
	// resurrected; reconfiguring a purged name is allowed and creates the next
	// generation (no fence). The whole call is accounted as an in-flight writer
	// so a purge drains its follow-up index maintenance before deleting, and
	// every maintenance batch is additionally bound to this generation.
	release, err := s.gates.beginWrite(stream)
	if err != nil {
		return err
	}
	defer release()

	var (
		toIndex    []string
		toUnindex  []string
		generation uint64
	)

	err = s.adapter.Update(ctx, func(txn MTx) error {
		var err error
		toIndex, toUnindex, generation, err = transactions.SaveStreamConfig(txn, stream, config, s.streamConfigOverride)
		return err
	})
	if err != nil {
		return err
	}

	// The index maintenance runs in its own batched transactions, so on a large
	// stream it neither exceeds the backend's limits nor blocks the config
	// write. Every batch verifies the bound generation, so a purge landing
	// mid-maintenance stops it before the sweep and a same-name rebuild can
	// never be touched by the stale batches.
	for _, field := range toIndex {
		if err := s.indexField(ctx, stream, field, generation); err != nil {
			return err
		}
	}

	for _, field := range toUnindex {
		if err := s.unindexField(ctx, stream, field, generation); err != nil {
			return err
		}
	}

	return nil
}

// DeleteStream is implemented in retire.go as the generation-fenced
// FenceStream → FinishStreamRetirement composition.

// StreamUsage implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) StreamUsage(ctx context.Context, stream string) (int64, error) {
	var usage int64

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		usage, err = transactions.EstimateStorage(txn, stream)
		return err
	})

	return usage, err
}

// IndexField implements [storage.LogStorage]. It binds the back-fill to the
// stream's current generation and is rejected while a purge is in progress.
func (s *Storage[QTx, MTx]) IndexField(ctx context.Context, stream, field string) error {
	state, err := s.StreamState(ctx, stream)
	if err != nil {
		return err
	}
	if state.Retiring {
		return fmt.Errorf("stream '%s' is being purged: %w", stream, storage.ErrStreamRetiring)
	}

	release, err := s.gates.beginWrite(stream)
	if err != nil {
		return err
	}
	defer release()

	return s.indexField(ctx, stream, field, state.Generation)
}

// UnindexField implements [storage.LogStorage]. It binds the index removal to
// the stream's current generation and is rejected while a purge is in progress.
func (s *Storage[QTx, MTx]) UnindexField(ctx context.Context, stream, field string) error {
	state, err := s.StreamState(ctx, stream)
	if err != nil {
		return err
	}
	if state.Retiring {
		return fmt.Errorf("stream '%s' is being purged: %w", stream, storage.ErrStreamRetiring)
	}

	release, err := s.gates.beginWrite(stream)
	if err != nil {
		return err
	}
	defer release()

	return s.unindexField(ctx, stream, field, state.Generation)
}

// indexField back-fills a field's inverted index over an existing stream, one
// bounded batch of entries per transaction, resuming after the last entry each
// batch processed until the whole stream has been indexed. Every batch is bound
// to expectedGeneration: a purge or a same-name rebuild aborts the back-fill.
func (s *Storage[QTx, MTx]) indexField(
	ctx context.Context,
	stream, field string,
	expectedGeneration uint64,
) error {
	var cursor kv.Key

	for {
		var (
			next      kv.Key
			processed int
		)

		err := s.adapter.Update(ctx, func(txn MTx) error {
			var err error
			next, processed, err = transactions.IndexFieldBatch(
				s.logger, txn, stream, field, cursor, s.batchSize, expectedGeneration,
			)
			return err
		})
		if err != nil {
			return err
		}

		if processed == 0 {
			return nil
		}

		cursor = next
	}
}

// unindexField drops a field's inverted index, one bounded batch of keys per
// transaction, until none remain. Every batch is bound to expectedGeneration.
func (s *Storage[QTx, MTx]) unindexField(
	ctx context.Context,
	stream, field string,
	expectedGeneration uint64,
) error {
	for {
		var deleted int

		err := s.adapter.Update(ctx, func(txn MTx) error {
			var err error
			deleted, err = transactions.UnindexFieldBatch(
				txn, stream, field, s.batchSize, expectedGeneration,
			)
			return err
		})
		if err != nil {
			return err
		}

		if deleted == 0 {
			return nil
		}
	}
}

// CollectGarbage enforces every stream's retention-size budget, evicting the
// oldest records of any over-budget stream (and their index references) in
// bounded batches across successive transactions so retention enforcement never
// exceeds a backend's transaction limits.
//
// Streams with an in-progress retirement are skipped: the purge owns their
// keyspace convergence, and interleaving eviction with it could leave orphan
// index keys behind.
func (s *Storage[QTx, MTx]) CollectGarbage(ctx context.Context) error {
	var configs map[string]models.StreamConfig

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		configs, err = transactions.FetchStreamConfigs(txn, s.streamConfigOverride)
		return err
	})
	if err != nil {
		return err
	}

	var retiring map[string]uint64
	err = s.adapter.View(ctx, func(txn QTx) error {
		var err error
		retiring, err = transactions.FetchRetiringStreams(txn)
		return err
	})
	if err != nil {
		return err
	}

	for stream, config := range configs {
		if generation, skip := retiring[stream]; skip {
			s.logger.DebugContext(
				ctx,
				"skipping GC for retiring stream",
				slog.String("stream", stream),
				slog.Uint64("generation", generation),
			)
			continue
		}

		retentionSize := config.RetentionSize * 1024 * 1024 // MB to bytes
		if retentionSize <= 0 {
			continue
		}

		var streamSize int64
		err := s.adapter.View(ctx, func(txn QTx) error {
			var err error
			streamSize, err = transactions.EstimateStorage(txn, stream)
			return err
		})
		if err != nil {
			return err
		}

		for streamSize > retentionSize {
			var (
				freed   int64
				deleted int
			)

			err := s.adapter.Update(ctx, func(txn MTx) error {
				var err error
				freed, deleted, err = transactions.EvictOldestBatch(
					txn, stream, streamSize-retentionSize, s.batchSize,
				)
				return err
			})
			if err != nil {
				// A purge that fenced the stream mid-run owns the keyspace
				// now: stop evicting it and continue with the other streams.
				if errors.Is(err, storage.ErrStreamRetiring) {
					s.logger.DebugContext(
						ctx,
						"stopping GC for stream retired mid-run",
						slog.String("stream", stream),
					)
					break
				}

				return err
			}

			if deleted == 0 {
				break
			}

			streamSize -= freed
		}
	}

	return nil
}

// Distinct implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) Distinct(ctx context.Context, stream string) (map[string][]string, error) {
	var indices map[string][]string

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		indices, err = transactions.Distinct(txn, stream)
		return err
	})
	if err != nil {
		return nil, err
	}

	return indices, nil
}

// Ingest implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) Ingest(ctx context.Context, stream string, logRecord *models.LogRecord) (kv.Key, error) {
	// Freeze gate: anything arriving after the fence is rejected before it
	// touches the backend; writes admitted here are accounted for so the purge
	// drains them before deleting. The retirement record checked inside the
	// ingest transaction is the durable, cross-process authority.
	release, err := s.gates.beginWrite(stream)
	if err != nil {
		return nil, err
	}
	defer release()

	key := newDbKey(stream, logRecord)
	err = s.adapter.Update(ctx, func(txn MTx) error {
		return transactions.Ingest(s.logger, txn, stream, logRecord, key, s.streamConfigOverride)
	})
	if err != nil {
		return nil, err
	}

	return key, nil
}

// FetchLogs implements [storage.LogStorage].
func (s *Storage[QTx, MTx]) FetchLogs(
	ctx context.Context,
	stream string,
	from, to time.Time,
	filter filtering.Filter,
	indexing map[string][]string,
) ([]models.LogRecord, error) {
	var results []models.LogRecord

	err := s.adapter.View(ctx, func(txn QTx) error {
		var err error
		results, err = transactions.FetchLogs(txn, stream, from, to, filter, indexing)
		return err
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}
