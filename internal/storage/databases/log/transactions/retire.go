package transactions

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"link-society.com/flowg/internal/storage/generic/kv"
	storage "link-society.com/flowg/internal/storage/interfaces"
)

// ErrRetirementNotDrained is returned by [FinalizeRetirement] when the data
// prefixes still hold at least one key: finalization must not remove the fence
// while data survives. Callers keep sweeping and retry.
var ErrRetirementNotDrained = errors.New("retirement cannot finalize while data keys remain")

// ErrStaleRetirement marks a retirement record whose generation no longer
// matches the stream's generation counter. Such a record belongs to an
// obsolete lineage; recovery discards the record without touching any data.
var ErrStaleRetirement = errors.New("stale retirement record: generation mismatch")

// RetirementPhase is the phase of a stream's generation retirement state
// machine. The transition is fencing → draining → deleting → finalizing, and
// the record disappears once finalization commits.
type RetirementPhase string

const (
	// PhaseFencing: the durable fence was just written; new writes and
	// subscriptions are rejected, in-flight writes have not been awaited yet.
	PhaseFencing RetirementPhase = "fencing"
	// PhaseDraining: local in-flight writes are being awaited (or were given
	// up on after the bounded drain window).
	PhaseDraining RetirementPhase = "draining"
	// PhaseDeleting: entries, indexes and field metadata are being swept in
	// persisted batches.
	PhaseDeleting RetirementPhase = "deleting"
	// PhaseFinalizing: data prefixes converged to zero; config and the
	// retirement record are about to be cleared.
	PhaseFinalizing RetirementPhase = "finalizing"
)

// RetirementStageName identifies one of the data deletion stages. The stages
// are swept in this order; the configuration key is handled separately by
// finalization.
type RetirementStageName string

const (
	// StageEntries sweeps "entry:<stream>:*" log shards.
	StageEntries RetirementStageName = "entries"
	// StageIndexes sweeps "index:<stream>:*" inverted-index keys.
	StageIndexes RetirementStageName = "indexes"
	// StageFields sweeps "stream:field:<stream>:*" field metadata markers.
	StageFields RetirementStageName = "fields"
)

// RetirementStage is the persisted, queryable result of one deletion stage:
// the cumulative number of cleared keys and whether a re-scan observed the
// prefix as empty.
type RetirementStage struct {
	Deleted int  `json:"deleted"`
	Done    bool `json:"done"`
}

// Retirement is the durable state of one purged stream generation. It is the
// single authoritative fence that writes, configuration changes, GC, purge
// duplication and restart recovery all consult.
type Retirement struct {
	Stream     string                     `json:"stream"`
	Generation uint64                     `json:"generation"`
	Phase      RetirementPhase            `json:"phase"`
	Stages     map[string]RetirementStage `json:"stages"`
	StartedAt  time.Time                  `json:"started_at"`
	UpdatedAt  time.Time                  `json:"updated_at"`
}

// generationKey builds "stream:generation:<stream>", the monotonic generation
// counter. It is created at the stream's first touch, incremented when a
// purged name is recreated, and deliberately never removed by purge: it is
// what lets old cleanup identify a same-name rebuilt generation.
func generationKey(stream string) kv.Key {
	return kv.Key{"stream", "generation", stream}
}

// retirementKey builds "stream:retire:<stream>", the durable retirement state
// record. It exists only while the generation is retiring. Its prefix does not
// overlap "stream:config:*" (stream listings) or "stream:field:*" metadata.
func retirementKey(stream string) kv.Key {
	return kv.Key{"stream", "retire", stream}
}

// streamDataPrefixes are the per-stream data key prefixes, in sweep order.
func streamDataPrefixes(stream string) []kv.Key {
	return []kv.Key{
		{"entry", stream},
		{"index", stream},
		{"stream", "field", stream},
	}
}

func stagePrefix(stage RetirementStageName, stream string) (kv.Key, error) {
	switch stage {
	case StageEntries:
		return kv.Key{"entry", stream}, nil
	case StageIndexes:
		return kv.Key{"index", stream}, nil
	case StageFields:
		return kv.Key{"stream", "field", stream}, nil
	default:
		return nil, fmt.Errorf("unknown retirement stage %q", stage)
	}
}

// newRetirement initializes a retirement record at the fencing phase with all
// three data stages open.
func newRetirement(stream string, generation uint64, now time.Time) Retirement {
	return Retirement{
		Stream:     stream,
		Generation: generation,
		Phase:      PhaseFencing,
		Stages: map[string]RetirementStage{
			string(StageEntries): {},
			string(StageIndexes): {},
			string(StageFields):  {},
		},
		StartedAt: now,
		UpdatedAt: now,
	}
}

// SaveRetirement persists the retirement record (JSON).
func SaveRetirement(txn kv.MutationTx, r Retirement) error {
	value, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("could not marshal retirement for stream '%s': %w", r.Stream, err)
	}

	if err := txn.Set(retirementKey(r.Stream), value); err != nil {
		return fmt.Errorf("could not save retirement for stream '%s': %w", r.Stream, err)
	}

	return nil
}

// LoadRetirement reads the retirement record of a stream. A missing record
// yields (nil, nil).
func LoadRetirement(txn kv.QueryTx, stream string) (*Retirement, error) {
	value, err := txn.Get(retirementKey(stream))
	if err != nil {
		return nil, fmt.Errorf("could not fetch retirement for stream '%s': %w", stream, err)
	}

	if value == nil {
		return nil, nil
	}

	var r Retirement
	if err := json.Unmarshal(value, &r); err != nil {
		return nil, fmt.Errorf("could not unmarshal retirement for stream '%s': %w", stream, err)
	}

	return &r, nil
}

// ListRetirements enumerates every in-progress retirement record.
func ListRetirements(txn kv.QueryTx) ([]Retirement, error) {
	retirements := []Retirement{}

	for pair := range txn.IterPairs(kv.Key{"stream", "retire"}, kv.KeyRange{}) {
		var r Retirement
		if err := json.Unmarshal(pair.Value(), &r); err != nil {
			key := pair.Key()
			return nil, fmt.Errorf(
				"could not unmarshal retirement record '%s': %w",
				key, err,
			)
		}

		retirements = append(retirements, r)
	}

	return retirements, nil
}

// FetchRetiringStreams returns the generation currently being retired for each
// stream with a retirement record, letting GC skip them without risking an
// interleaving with the purge.
func FetchRetiringStreams(txn kv.QueryTx) (map[string]uint64, error) {
	retirements, err := ListRetirements(txn)
	if err != nil {
		return nil, err
	}

	streams := make(map[string]uint64, len(retirements))
	for _, r := range retirements {
		streams[r.Stream] = r.Generation
	}

	return streams, nil
}

// readGeneration reads the monotonic generation counter; 0 means the stream
// name was never seen.
func readGeneration(txn kv.QueryTx, stream string) (uint64, error) {
	value, err := txn.Get(generationKey(stream))
	if err != nil {
		return 0, fmt.Errorf("could not fetch generation for stream '%s': %w", stream, err)
	}

	if value == nil {
		return 0, nil
	}

	generation, err := strconv.ParseUint(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not decode generation for stream '%s': %w", stream, err)
	}

	return generation, nil
}

// writeGeneration sets the monotonic generation counter.
func writeGeneration(txn kv.MutationTx, stream string, generation uint64) error {
	value := strconv.FormatUint(generation, 10)
	if err := txn.Set(generationKey(stream), []byte(value)); err != nil {
		return fmt.Errorf("could not save generation for stream '%s': %w", stream, err)
	}

	return nil
}

// ReadGeneration exposes the monotonic generation counter for recovery
// bookkeeping (0 when the name was never seen).
func ReadGeneration(txn kv.QueryTx, stream string) (uint64, error) {
	return readGeneration(txn, stream)
}

// streamHasData reports whether at least one key exists under any of the
// stream's data prefixes. TTL-expired entries are not observed (reads honor
// expiry), so an empty result means "nothing to retire".
func streamHasData(txn kv.QueryTx, stream string) bool {
	for _, prefix := range streamDataPrefixes(stream) {
		for range txn.IterKeys(prefix, kv.KeyRange{}) {
			return true
		}
	}

	return false
}

// BeginRetirement opens the retirement generation of a stream inside the
// caller's transaction.
//
// Idempotency: when a retirement record already exists it is returned
// unchanged with existed=true, so a duplicate (or resumed) purge never bumps
// the generation again.
//
// Existence (the compat contract for purging unknown names): a stream exists
// when it has a configuration, any data key, or a past generation counter
// with data. When nothing at all is present (never-seen name, or a fully
// purged generation whose data all converged), existed=false is returned and
// the transaction performs no write.
//
// Otherwise the generation counter is initialized to 1 when this is the first
// generation, and the fencing retirement record is persisted.
func BeginRetirement(
	txn kv.MutationTx,
	stream string,
	now time.Time,
) (Retirement, bool, error) {
	existing, err := LoadRetirement(txn, stream)
	if err != nil {
		return Retirement{}, false, err
	}
	if existing != nil {
		return *existing, true, nil
	}

	generation, err := readGeneration(txn, stream)
	if err != nil {
		return Retirement{}, false, err
	}

	configValue, err := txn.Get(kv.Key{"stream", "config", stream})
	if err != nil {
		return Retirement{}, false, fmt.Errorf(
			"could not fetch stream config '%s': %w",
			stream, err,
		)
	}

	hasData := streamHasData(txn, stream)

	if configValue == nil && !hasData {
		// Nothing live to retire: a never-seen name (no counter) as well as a
		// previously purged generation whose keyspace converged (counter kept,
		// no config or data). Purging it is a side-effect-free success and must
		// neither create a redundant record nor close watchers of the name.
		return Retirement{}, false, nil
	}

	if generation == 0 {
		generation = 1
		if err := writeGeneration(txn, stream, generation); err != nil {
			return Retirement{}, false, err
		}
	}

	retirement := newRetirement(stream, generation, now)
	if err := SaveRetirement(txn, retirement); err != nil {
		return Retirement{}, false, err
	}

	return retirement, true, nil
}

// SetRetirementPhase persists a phase transition (and stamps UpdatedAt).
func SetRetirementPhase(txn kv.MutationTx, r *Retirement, phase RetirementPhase, now time.Time) error {
	r.Phase = phase
	r.UpdatedAt = now
	return SaveRetirement(txn, *r)
}

// DeleteRetirementStageBatch sweeps one data stage by re-observing the
// keyspace: it rescans the stage prefix FROM ITS ROOT (no cursor is trusted
// across transactions, so an uncertain commit simply heals on the next batch),
// clears up to limit keys that actually exist, and persists the updated stage
// counters together with the deletions in the same transaction.
//
// A deleted count below limit means the re-scan found no further keys, so the
// stage is marked done.
func DeleteRetirementStageBatch(
	txn kv.MutationTx,
	stream string,
	stage RetirementStageName,
	limit int,
	r *Retirement,
) (int, error) {
	prefix, err := stagePrefix(stage, stream)
	if err != nil {
		return 0, err
	}

	var keys []kv.Key
	for key := range txn.IterKeys(prefix, kv.KeyRange{}) {
		keys = append(keys, key)
		if len(keys) >= limit {
			break
		}
	}

	for _, key := range keys {
		if err := txn.Clear(key); err != nil {
			return 0, fmt.Errorf(
				"could not delete %s key '%s' from retiring stream '%s': %w",
				stage, key, stream, err,
			)
		}
	}

	stageState := r.Stages[string(stage)]
	stageState.Deleted += len(keys)
	// Done reflects the latest observation only: a straggler committing after
	// the prefix was observed empty makes the next batch find keys again, so
	// the flag is (re)assigned rather than latched.
	stageState.Done = len(keys) < limit
	r.Stages[string(stage)] = stageState
	r.UpdatedAt = time.Now()

	if err := SaveRetirement(txn, *r); err != nil {
		return 0, err
	}

	return len(keys), nil
}

// FinalizeRetirement commits the very end of a retirement, in a single
// transaction: it re-verifies that every data prefix is empty (keyspace
// facts, not counters), that the retirement record still exists and carries
// the expected generation, then clears the configuration key and the
// retirement record. The generation counter is intentionally kept so the
// next creation takes a new generation.
//
// It is idempotent: an already-finalized stream (no record) yields no error.
// A surviving data key yields [ErrRetirementNotDrained] so the caller keeps
// sweeping; a generation mismatch yields [ErrStaleRetirement].
func FinalizeRetirement(
	txn kv.MutationTx,
	stream string,
	expectedGeneration uint64,
	now time.Time,
) error {
	retirement, err := LoadRetirement(txn, stream)
	if err != nil {
		return err
	}

	if retirement == nil {
		// Already finalized (possibly by a duplicate purge racing us).
		return nil
	}

	if retirement.Generation != expectedGeneration {
		return fmt.Errorf(
			"stream '%s': record generation %d, expected %d: %w",
			stream, retirement.Generation, expectedGeneration, ErrStaleRetirement,
		)
	}

	if streamHasData(txn, stream) {
		return ErrRetirementNotDrained
	}

	if err := txn.Clear(kv.Key{"stream", "config", stream}); err != nil {
		return fmt.Errorf("could not delete config of retiring stream '%s': %w", stream, err)
	}

	if err := txn.Clear(retirementKey(stream)); err != nil {
		return fmt.Errorf("could not delete retirement record of stream '%s': %w", stream, err)
	}

	return nil
}

// DiscardRetirement removes the retirement record without touching any other
// key. It is used to drop an obsolete (generation-mismatched) record during
// recovery.
func DiscardRetirement(txn kv.MutationTx, stream string) error {
	if err := txn.Clear(retirementKey(stream)); err != nil {
		return fmt.Errorf("could not discard retirement record of stream '%s': %w", stream, err)
	}

	return nil
}

// FetchStreamState returns the generation-aware state of a stream. The
// retirement record takes precedence (Retiring); otherwise Exists reflects the
// configuration and Generation the counter (0 for a never-seen name).
func FetchStreamState(
	txn kv.QueryTx,
	stream string,
) (storage.StreamState, error) {
	retirement, err := LoadRetirement(txn, stream)
	if err != nil {
		return storage.StreamState{}, err
	}

	if retirement != nil {
		configValue, err := txn.Get(kv.Key{"stream", "config", stream})
		if err != nil {
			return storage.StreamState{}, fmt.Errorf(
				"could not fetch stream config '%s': %w",
				stream, err,
			)
		}

		return storage.StreamState{
			Generation: retirement.Generation,
			Exists:     configValue != nil,
			Retiring:   true,
		}, nil
	}

	generation, err := readGeneration(txn, stream)
	if err != nil {
		return storage.StreamState{}, err
	}

	configValue, err := txn.Get(kv.Key{"stream", "config", stream})
	if err != nil {
		return storage.StreamState{}, fmt.Errorf(
			"could not fetch stream config '%s': %w",
			stream, err,
		)
	}

	return storage.StreamState{
		Generation: generation,
		Exists:     configValue != nil,
		Retiring:   false,
	}, nil
}

// guardRetiring returns [storage.ErrStreamRetiring] when a retirement record
// fences the stream. It must be called by every transaction that creates data
// or configuration for a stream.
func guardRetiring(txn kv.QueryTx, stream string) error {
	retirement, err := LoadRetirement(txn, stream)
	if err != nil {
		return err
	}

	if retirement != nil {
		return fmt.Errorf("stream '%s' generation %d is being purged: %w",
			stream, retirement.Generation, storage.ErrStreamRetiring)
	}

	return nil
}

// guardMaintenanceGeneration fences the multi-transaction index maintenance
// batches a configuration change runs after its config commit. Such a batch
// must stop as soon as the generation it was bound to is being purged (the
// durable retirement fence) or no longer matches the counter (it was finalized
// and the name may already host a rebuilt generation): otherwise a long
// back-fill resumed from its cursor could observe the new generation's entries
// and write index keys into the wrong lineage.
func guardMaintenanceGeneration(
	txn kv.QueryTx,
	stream string,
	expectedGeneration uint64,
) error {
	if err := guardRetiring(txn, stream); err != nil {
		return err
	}

	currentGeneration, err := readGeneration(txn, stream)
	if err != nil {
		return err
	}

	if currentGeneration != expectedGeneration {
		return fmt.Errorf(
			"index maintenance for stream '%s' was bound to generation %d but the stream is at generation %d: %w",
			stream, expectedGeneration, currentGeneration, storage.ErrStreamRetiring,
		)
	}

	return nil
}

// createNextGeneration assigns the next live generation when a stream
// configuration is (re)created: 1 for a never-seen name, counter+1 when a
// purged name is recreated. The write races the purge fence transaction on
// this same key, so OCC/SSI backends abort one side if they commit
// concurrently — and guardRetiring rejects the writer whenever the fence is
// observed.
func createNextGeneration(txn kv.MutationTx, stream string) error {
	generation, err := readGeneration(txn, stream)
	if err != nil {
		return err
	}

	return writeGeneration(txn, stream, generation+1)
}
