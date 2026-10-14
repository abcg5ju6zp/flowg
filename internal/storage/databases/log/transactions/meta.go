package transactions

import (
	"fmt"

	"encoding/json"

	"link-society.com/flowg/internal/models"
	"link-society.com/flowg/internal/storage/generic/kv"
)

// FetchStreamConfigs returns the configuration of every stream, keyed by stream
// name. When override is not nil, it is substituted for all of them.
func FetchStreamConfigs(txn kv.QueryTx, override *models.StreamConfig) (map[string]models.StreamConfig, error) {
	streams := map[string]models.StreamConfig{}

	for pair := range txn.IterPairs(kv.Key{"stream", "config"}, kv.KeyRange{}) {
		key := pair.Key()

		stream := key[len(key)-1]

		var streamConfig models.StreamConfig
		if override != nil {
			streamConfig = *override
		} else {
			val := pair.Value()
			// If the value is empty, we treat it as a default config (the zero-value
			// of StreamConfig).
			if len(val) > 0 {
				if err := json.Unmarshal(val, &streamConfig); err != nil {
					return nil, fmt.Errorf("could not unmarshal stream config '%s': %w", stream, err)
				}
			}
		}

		if streamConfig.IndexedFields == nil {
			streamConfig.IndexedFields = []string{}
		}

		streams[stream] = streamConfig
	}

	return streams, nil
}

// FetchStreamFields lists the field names recorded for a stream from its
// "stream:field:<stream>:" existence markers.
func FetchStreamFields(txn kv.QueryTx, stream string) []string {
	fields := []string{}

	for key := range txn.IterKeys(kv.Key{"stream", "field", stream}, kv.KeyRange{}) {
		fieldName := key[len(key)-1]
		fields = append(fields, fieldName)
	}

	return fields
}

// GetOrCreateStreamConfig returns a stream's configuration, lazily creating an
// empty (default) config the first time a stream is referenced so later writes
// have something to update. A non-nil override short-circuits to that config.
//
// A stream fenced by an active retirement generation fails with
// storage.ErrStreamRetiring, so a purge cannot be followed by a lazily
// recreated config that would smuggle the old generation back. The first
// write of a (re)created stream mints its generation token in the same
// transaction.
func GetOrCreateStreamConfig(txn kv.MutationTx, stream string, override *models.StreamConfig) (models.StreamConfig, error) {
	if override != nil {
		return *override, nil
	}

	if err := EnsureWritable(txn, stream); err != nil {
		return models.StreamConfig{}, err
	}

	var streamConfig models.StreamConfig

	streamKey := kv.Key{"stream", "config", stream}
	val, err := txn.Get(streamKey)
	if err != nil {
		return models.StreamConfig{}, fmt.Errorf(
			"could not fetch stream config '%s': %w",
			stream, err,
		)
	}

	if val == nil {
		err := txn.Set(streamKey, []byte{})
		if err != nil {
			return models.StreamConfig{}, fmt.Errorf(
				"could not create default stream config '%s': %w",
				stream, err,
			)
		}
	}

	// Every live stream owns a generation token, minted at creation. Legacy
	// streams (created before generations existed) are upgraded on first
	// touch. The token is what stale cleanups compare themselves against.
	if _, err := MintStreamGeneration(txn, stream); err != nil {
		return models.StreamConfig{}, err
	}

	// If the value is empty, we treat it as a default config (the zero-value
	// of StreamConfig).
	if len(val) > 0 {
		if err := json.Unmarshal(val, &streamConfig); err != nil {
			return models.StreamConfig{}, fmt.Errorf(
				"could not unmarshal stream config '%s': %w",
				stream, err,
			)
		}
	}

	if streamConfig.IndexedFields == nil {
		streamConfig.IndexedFields = []string{}
	}

	return streamConfig, nil
}

// SaveStreamConfig stores a new stream configuration and computes how its
// indexed fields differ from the previous one. It returns the fields newly
// added to the index (whose existing records must be back-filled) and the
// fields removed from it (whose index must be dropped).
//
// The actual back-fill and index removal are performed separately, in their own
// batched transactions, because on a large stream they can exceed a single
// transaction's size and time limits.
//
// When override is not nil, the previous configuration is taken to be override
// (see [GetOrCreateStreamConfig]).
func SaveStreamConfig(
	txn kv.MutationTx,
	stream string,
	config models.StreamConfig,
	override *models.StreamConfig,
) (toIndex []string, toUnindex []string, err error) {
	if config.IndexedFields == nil {
		config.IndexedFields = []string{}
	}

	// Reconfiguring a stream being purged is rejected outright, even when an
	// override config short-circuits GetOrCreateStreamConfig.
	if err := EnsureWritable(txn, stream); err != nil {
		return nil, nil, err
	}

	oldConfig, err := GetOrCreateStreamConfig(txn, stream, override)
	if err != nil {
		return nil, nil, fmt.Errorf("could not fetch old stream config '%s': %w", stream, err)
	}

	// In override mode GetOrCreateStreamConfig does not persist anything, so
	// make sure an explicitly (re)configured stream still owns a generation
	// token distinct from any purged one.
	if override != nil {
		if _, err := MintStreamGeneration(txn, stream); err != nil {
			return nil, nil, err
		}
	}

	streamKey := kv.Key{"stream", "config", stream}
	configVal, err := json.Marshal(config)
	if err != nil {
		return nil, nil, fmt.Errorf("could not marshal stream config '%s': %w", stream, err)
	}

	if err := txn.Set(streamKey, configVal); err != nil {
		return nil, nil, fmt.Errorf("could not save stream config '%s': %w", stream, err)
	}

	for _, field := range config.IndexedFields {
		if !oldConfig.IsFieldIndexed(field) {
			toIndex = append(toIndex, field)
		}
	}

	for _, field := range oldConfig.IndexedFields {
		if !config.IsFieldIndexed(field) {
			toUnindex = append(toUnindex, field)
		}
	}

	return toIndex, toUnindex, nil
}


