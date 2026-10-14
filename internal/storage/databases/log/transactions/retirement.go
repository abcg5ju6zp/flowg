package transactions

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"link-society.com/flowg/internal/storage/generic/kv"
	storage "link-society.com/flowg/internal/storage/interfaces"
)

// RetirementStage identifies one deletion boundary of a stream purge. A
// retirement crosses the stages in order; each stage's result is persisted in
// the RetirementRecord so an interrupted purge can be resumed without guessing
// what is already gone.
type RetirementStage string

const (
	// StageEntries deletes the log shards ("entry:<stream>:*").
	StageEntries RetirementStage = "entries"
	// StageIndexes deletes the inverted indexes ("index:<stream>:*"),
	// including index references orphaned by an interleaved GC or crash.
	StageIndexes RetirementStage = "indexes"
	// StageFields deletes the stream's field metadata
	// ("stream:field:<stream>:*").
	StageFields RetirementStage = "fields"
	// StageConfig deletes the retention configuration and the generation
	// token ("stream:config:<stream>", "stream:gen:<stream>").
	StageConfig RetirementStage = "config"
)

// OrderedRetirementStages is the order in which a purge crosses the deletion
// boundaries: data first, metadata and retention configuration last, so an
// interruption never leaves a stream that looks configured but empty-handed.
var OrderedRetirementStages = []RetirementStage{
	StageEntries,
	StageIndexes,
	StageFields,
	StageConfig,
}

// Per-stage lifecycle states persisted in the RetirementRecord.
const (
	StageStatusPending = "pending"
	StageStatusRunning = "running"
	StageStatusDone    = "done"
)

// ErrGenerationMoved is returned when a mutation belongs to a retirement
// generation that no longer owns the stream name. The concrete marker in the
// keyspace always wins: either the retirement finished and a new, same-named
// stream was created (its data must survive), or a newer generation superseded
// this one. A stale cleanup must stop without deleting a single key.
var ErrGenerationMoved = errors.New("stream generation moved")

// RetirementStageState is the persisted, queryable result of one deletion
// stage: whether it has completed, how many keys it deleted in total, and when
// it last made progress.
type RetirementStageState struct {
	Status    string    `json:"status"`
	Deleted   int       `json:"deleted"`
	UpdatedAt time.Time `json:"updated_at"`
}

// RetirementRecord is the persisted tombstone of one purge generation. It is
// written first — fencing every new write, subscription and GC run — and
// removed last, in the same transactional boundary as the stream's retention
// configuration. While it exists the stream name belongs to this generation.
type RetirementRecord struct {
	Stream     string                                    `json:"stream"`
	Generation string                                    `json:"generation"`
	StartedAt  time.Time                                 `json:"started_at"`
	Stages     map[RetirementStage]*RetirementStageState `json:"stages"`
}

// NewRetirementRecord builds a fresh, all-pending record for a new generation.
func NewRetirementRecord(stream, generation string) *RetirementRecord {
	rec := &RetirementRecord{
		Stream:     stream,
		Generation: generation,
		StartedAt:  time.Now().UTC(),
		Stages:     map[RetirementStage]*RetirementStageState{},
	}

	for _, stage := range OrderedRetirementStages {
		rec.Stages[stage] = &RetirementStageState{
			Status:    StageStatusPending,
			UpdatedAt: rec.StartedAt,
		}
	}

	return rec
}

// Stage returns the persisted state of a stage, lazily creating the entry
// (defensive: records written by older builds may miss newly added stages).
func (r *RetirementRecord) Stage(stage RetirementStage) *RetirementStageState {
	state, exists := r.Stages[stage]
	if !exists {
		state = &RetirementStageState{
			Status:    StageStatusPending,
			UpdatedAt: r.StartedAt,
		}
		r.Stages[stage] = state
	}

	return state
}

// retirementMarkerKey is the tombstone key: "stream:retire:<stream>".
func retirementMarkerKey(stream string) kv.Key {
	return kv.Key{"stream", "retire", stream}
}

// streamGenerationKey holds the generation token currently owning the name:
// "stream:gen:<stream>". A missing key means a legacy (pre-generation) stream;
// the first write mints its token.
func streamGenerationKey(stream string) kv.Key {
	return kv.Key{"stream", "gen", stream}
}

// FetchRetirement loads the active retirement record of a stream. A missing
// marker (or no active retirement) yields a nil record and no error.
func FetchRetirement(txn kv.QueryTx, stream string) (*RetirementRecord, error) {
	val, err := txn.Get(retirementMarkerKey(stream))
	if err != nil {
		return nil, fmt.Errorf(
			"could not fetch retirement marker for stream '%s': %w",
			stream, err,
		)
	}
	if val == nil {
		return nil, nil
	}

	var rec RetirementRecord
	if err := json.Unmarshal(val, &rec); err != nil {
		return nil, fmt.Errorf(
			"could not unmarshal retirement marker for stream '%s': %w",
			stream, err,
		)
	}

	return &rec, nil
}

// ListRetirements returns every active retirement record, keyed by stream
// name. It is what process restart uses to discover purges interrupted by the
// crash and resume them.
func ListRetirements(txn kv.QueryTx) (map[string]*RetirementRecord, error) {
	records := map[string]*RetirementRecord{}

	for pair := range txn.IterPairs(kv.Key{"stream", "retire"}, kv.KeyRange{}) {
		key := pair.Key()
		stream := key[len(key)-1]

		var rec RetirementRecord
		if err := json.Unmarshal(pair.Value(), &rec); err != nil {
			return nil, fmt.Errorf(
				"could not unmarshal retirement marker for stream '%s': %w",
				stream, err,
			)
		}

		records[stream] = &rec
	}

	return records, nil
}

// BeginRetirement creates the generation tombstone of a stream unless one
// already exists. The retirement inherits the generation token currently
// owning the stream, so its fence checks compare like with like; a legacy
// stream that predates tokens (or an orphaned stream holding data only) gets a
// freshly minted retirement id instead. It returns the record owning the name
// and whether an existing marker was reused, so callers always proceed
// against the concrete keyspace fact rather than assuming they created the
// generation.
func BeginRetirement(txn kv.MutationTx, stream string) (*RetirementRecord, bool, error) {
	existing, err := FetchRetirement(txn, stream)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		return existing, true, nil
	}

	generation, err := FetchStreamGeneration(txn, stream)
	if err != nil {
		return nil, false, err
	}
	if generation == "" {
		generation = uuid.New().String()
	}

	rec := NewRetirementRecord(stream, generation)

	val, err := json.Marshal(rec)
	if err != nil {
		return nil, false, fmt.Errorf(
			"could not marshal retirement marker for stream '%s': %w",
			stream, err,
		)
	}

	if err := txn.Set(retirementMarkerKey(stream), val); err != nil {
		return nil, false, fmt.Errorf(
			"could not save retirement marker for stream '%s': %w",
			stream, err,
		)
	}

	return rec, false, nil
}

// SaveRetirement persists the (progressing) retirement record. It refuses to
// overwrite a marker belonging to another generation, so a stale cleanup can
// never clobber a newer tombstone.
func SaveRetirement(txn kv.MutationTx, rec *RetirementRecord) error {
	if err := checkRetirementFence(txn, rec.Stream, rec.Generation); err != nil {
		return err
	}

	val, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf(
			"could not marshal retirement marker for stream '%s': %w",
			rec.Stream, err,
		)
	}

	if err := txn.Set(retirementMarkerKey(rec.Stream), val); err != nil {
		return fmt.Errorf(
			"could not save retirement marker for stream '%s': %w",
			rec.Stream, err,
		)
	}

	return nil
}

// FinishRetirement removes the generation tombstone, the very last step of a
// purge. The clear is conditional on the marker still naming this generation:
// if a newer generation owns the name, it returns ErrGenerationMoved instead
// of touching anything.
func FinishRetirement(txn kv.MutationTx, stream, generation string) error {
	if err := checkRetirementFence(txn, stream, generation); err != nil {
		return err
	}

	if err := txn.Clear(retirementMarkerKey(stream)); err != nil {
		return fmt.Errorf(
			"could not delete retirement marker for stream '%s': %w",
			stream, err,
		)
	}

	return nil
}

// EnsureWritable rejects an operation on a fenced stream with
// storage.ErrStreamRetiring. It is the in-transaction counterpart of the
// in-memory writer gate, closing the race window for operations that slipped
// past the gate (or that run in another process).
func EnsureWritable(txn kv.QueryTx, stream string) error {
	rec, err := FetchRetirement(txn, stream)
	if err != nil {
		return err
	}
	if rec != nil {
		return fmt.Errorf(
			"stream '%s' generation '%s': %w",
			stream, rec.Generation, storage.ErrStreamRetiring,
		)
	}

	return nil
}

// FetchStreamGeneration returns the generation token currently owning the
// stream name, or an empty string for a legacy stream that predates
// generations (or a stream whose configuration stage was already cleared).
func FetchStreamGeneration(txn kv.QueryTx, stream string) (string, error) {
	val, err := txn.Get(streamGenerationKey(stream))
	if err != nil {
		return "", fmt.Errorf(
			"could not fetch generation token for stream '%s': %w",
			stream, err,
		)
	}

	return string(val), nil
}

// MintStreamGeneration returns the generation token owning the stream,
// creating a fresh one the first time a non-retiring stream is written. It
// must be called after EnsureWritable, inside the same transaction, so a
// recreated same-named stream always gets a token distinct from the purged
// generation.
func MintStreamGeneration(txn kv.MutationTx, stream string) (string, error) {
	generation, err := FetchStreamGeneration(txn, stream)
	if err != nil {
		return "", err
	}
	if generation != "" {
		return generation, nil
	}

	generation = uuid.New().String()
	if err := txn.Set(streamGenerationKey(stream), kv.Value(generation)); err != nil {
		return "", fmt.Errorf(
			"could not save generation token for stream '%s': %w",
			stream, err,
		)
	}

	return generation, nil
}

// checkRetirementFence asserts that this generation still owns the stream
// name: its tombstone must be present with a matching generation, and no
// recreated stream's token must be in place. Any deviation means a stale
// cleanup and yields ErrGenerationMoved.
func checkRetirementFence(txn kv.QueryTx, stream, generation string) error {
	rec, err := FetchRetirement(txn, stream)
	if err != nil {
		return err
	}
	if rec == nil || rec.Generation != generation {
		return fmt.Errorf(
			"retirement generation '%s' no longer owns stream '%s': %w",
			generation, stream, ErrGenerationMoved,
		)
	}

	current, err := FetchStreamGeneration(txn, stream)
	if err != nil {
		return err
	}
	if current != "" && current != generation {
		return fmt.Errorf(
			"stream '%s' recreated under generation '%s' while '%s' was cleaning up: %w",
			stream, current, generation, ErrGenerationMoved,
		)
	}

	return nil
}

// stagePrefixes maps a stage to the key prefixes it must empty. The config
// stage has no scan prefixes: it owns two fixed keys cleared directly.
func stagePrefixes(stream string, stage RetirementStage) []kv.Key {
	switch stage {
	case StageEntries:
		return []kv.Key{{"entry", stream}}

	case StageIndexes:
		return []kv.Key{{"index", stream}}

	case StageFields:
		return []kv.Key{{"stream", "field", stream}}

	default:
		return nil
	}
}

// CountRetirementStageKeys reports how many keys a stage still owns, straight
// from the keyspace. This is the source of truth after an uncertain delete:
// callers drive the stage to zero based on what the scan observes, not on
// whether a delete reported success.
func CountRetirementStageKeys(txn kv.QueryTx, stream string, stage RetirementStage) (int, error) {
	if stage == StageConfig {
		count := 0

		val, err := txn.Get(streamConfigKey(stream))
		if err != nil {
			return 0, fmt.Errorf(
				"could not inspect stream config '%s': %w", stream, err,
			)
		}
		if val != nil {
			count++
		}

		val, err = txn.Get(streamGenerationKey(stream))
		if err != nil {
			return 0, fmt.Errorf(
				"could not inspect generation token '%s': %w", stream, err,
			)
		}
		if val != nil {
			count++
		}

		return count, nil
	}

	count := 0
	for _, prefix := range stagePrefixes(stream, stage) {
		for range txn.IterKeys(prefix, kv.KeyRange{}) {
			count++
		}
	}

	return count, nil
}

// DeleteRetirementStageBatch deletes up to limit keys of one stage and reports
// how many it cleared. The generation fence is checked in the same
// transaction as the reads and deletes, so an old cleanup whose generation lost
// ownership cannot delete a recreated stream's data, and an interleaving GC
// cannot resurrect or orphan keys.
//
// Deletion is scan-driven: when a previous Clear's outcome was uncertain
// (timeout, retry, crash), the key is simply still observed by the next scan
// and deleted again. A count of zero means nothing this stage owns is visible
// in the keyspace.
func DeleteRetirementStageBatch(
	txn kv.MutationTx,
	stream, generation string,
	stage RetirementStage,
	limit int,
) (int, error) {
	if err := checkRetirementFence(txn, stream, generation); err != nil {
		return 0, err
	}

	if stage == StageConfig {
		deleted := 0

		val, err := txn.Get(streamConfigKey(stream))
		if err != nil {
			return 0, fmt.Errorf(
				"could not inspect stream config '%s': %w", stream, err,
			)
		}
		if val != nil {
			if err := txn.Clear(streamConfigKey(stream)); err != nil {
				return deleted, fmt.Errorf(
					"could not delete stream config '%s': %w", stream, err,
				)
			}
			deleted++
		}

		val, err = txn.Get(streamGenerationKey(stream))
		if err != nil {
			return deleted, fmt.Errorf(
				"could not inspect generation token '%s': %w", stream, err,
			)
		}
		if val != nil {
			if err := txn.Clear(streamGenerationKey(stream)); err != nil {
				return deleted, fmt.Errorf(
					"could not delete generation token '%s': %w", stream, err,
				)
			}
			deleted++
		}

		return deleted, nil
	}

	var keys []kv.Key
	for _, prefix := range stagePrefixes(stream, stage) {
		for key := range txn.IterKeys(prefix, kv.KeyRange{}) {
			keys = append(keys, key)
			if len(keys) >= limit {
				break
			}
		}
		if len(keys) >= limit {
			break
		}
	}

	for _, key := range keys {
		if err := txn.Clear(key); err != nil {
			return len(keys), fmt.Errorf(
				"could not delete key '%s' from generation '%s' of stream '%s': %w",
				key, generation, stream, err,
			)
		}
	}

	return len(keys), nil
}

// streamConfigKey returns "stream:config:<stream>".
func streamConfigKey(stream string) kv.Key {
	return kv.Key{"stream", "config", stream}
}

// StreamExists reports whether the name still holds anything the purge must
// deal with: an active tombstone, a generation token or configuration, or any
// entry, index or field key. It backs purge's idempotent no-op for
// never-existing streams without ever minting a generation.
func StreamExists(txn kv.QueryTx, stream string) (bool, error) {
	rec, err := FetchRetirement(txn, stream)
	if err != nil {
		return false, err
	}
	if rec != nil {
		return true, nil
	}

	val, err := txn.Get(streamConfigKey(stream))
	if err != nil {
		return false, err
	}
	if val != nil {
		return true, nil
	}

	val, err = txn.Get(streamGenerationKey(stream))
	if err != nil {
		return false, err
	}
	if val != nil {
		return true, nil
	}

	prefixes := []kv.Key{
		{"entry", stream},
		{"index", stream},
		{"stream", "field", stream},
	}
	for _, prefix := range prefixes {
		for range txn.IterKeys(prefix, kv.KeyRange{}) {
			return true, nil
		}
	}

	return false, nil
}
