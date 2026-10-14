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
// A stream whose generation is being retired is fenced: the retirement guard
// runs before the override short-circuit (so demo mode is fenced too) and
// before the lazy creation (so a late write cannot resurrect a purged stream).
// Creating a configuration assigns the next generation (1 for a new name,
// counter+1 when a purged name is recreated).
func GetOrCreateStreamConfig(txn kv.MutationTx, stream string, override *models.StreamConfig) (models.StreamConfig, error) {
	if err := guardRetiring(txn, stream); err != nil {
		return models.StreamConfig{}, err
	}

	if override != nil {
		return *override, nil
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
		// The name is (re)born: allocate its generation before creating the
		// configuration, in the same transaction as the creation.
		if err := createNextGeneration(txn, stream); err != nil {
			return models.StreamConfig{}, fmt.Errorf(
				"could not allocate generation for stream '%s': %w",
				stream, err,
			)
		}

		err := txn.Set(streamKey, []byte{})
		if err != nil {
			return models.StreamConfig{}, fmt.Errorf(
				"could not create default stream config '%s': %w",
				stream, err,
			)
		}
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
// added to the index (whose existing records must be back-filled), the fields
// removed from it (whose index must be dropped), and the generation under which
// the configuration was written — callers bind their follow-up maintenance
// batches to it so a concurrent purge cannot redirect them at another lineage.
//
// The actual back-fill and index removal are performed separately, in their own
// batched transactions, because on a large stream they can exceed a single
// transaction's size and time limits.
//
// When override is not nil, the previous configuration is taken to be override
// (see [GetOrCreateStreamConfig]); the returned generation is the current
// counter value (0 when the name has never been persisted, as happens in demo
// mode where no config is stored).
func SaveStreamConfig(
	txn kv.MutationTx,
	stream string,
	config models.StreamConfig,
	override *models.StreamConfig,
) (toIndex []string, toUnindex []string, generation uint64, err error) {
	if config.IndexedFields == nil {
		config.IndexedFields = []string{}
	}

	oldConfig, err := GetOrCreateStreamConfig(txn, stream, override)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("could not fetch old stream config '%s': %w", stream, err)
	}

	generation, err = readGeneration(txn, stream)
	if err != nil {
		return nil, nil, 0, err
	}

	streamKey := kv.Key{"stream", "config", stream}
	configVal, err := json.Marshal(config)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("could not marshal stream config '%s': %w", stream, err)
	}

	if err := txn.Set(streamKey, configVal); err != nil {
		return nil, nil, 0, fmt.Errorf("could not save stream config '%s': %w", stream, err)
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

	return toIndex, toUnindex, generation, nil
}
