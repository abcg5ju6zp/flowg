package badger

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"link-society.com/flowg/internal/models"

	logdb "link-society.com/flowg/internal/storage/databases/log"
	"link-society.com/flowg/internal/storage/databases/log/transactions"
	"link-society.com/flowg/internal/storage/generic/kv"
	storage "link-society.com/flowg/internal/storage/interfaces"
)

// testAdapterOpts is the fx options type used by the in-memory adapter built
// for the retirement tests.
type testAdapterOpts struct{}

// newRetirementTestStore builds an in-memory badger adapter plus the
// backend-agnostic log storage on top of it. The adapter is returned so tests
// can plant raw keyspace state (retirement records, orphan index keys) that a
// crashed process would leave behind.
func newRetirementTestStore(t *testing.T, batchSize int) (
	context.Context,
	*logdb.Storage[*BadgerTx, *BadgerTx],
	*BadgerAdapter,
) {
	t.Helper()

	const channel = "storage.log.retiretest"

	var adapter *BadgerAdapter

	app := fxtest.New(
		t,
		fx.Supply(slog.New(slog.DiscardHandler)),
		fx.Supply(testAdapterOpts{}),
		NewAdapter[testAdapterOpts](channel, func(testAdapterOpts) AdapterOptions {
			return AdapterOptions{InMemory: true}
		}),
		fx.Invoke(func(in struct {
			fx.In
			Adapter *BadgerAdapter `name:"storage.log.retiretest"`
		}) {
			adapter = in.Adapter
		}),
		fx.NopLogger,
	)
	app.RequireStart()
	t.Cleanup(app.RequireStop)

	store := logdb.NewStorage[*BadgerTx, *BadgerTx](
		slog.New(slog.DiscardHandler),
		adapter,
		batchSize,
		50*time.Millisecond,
		nil,
	)

	return t.Context(), store, adapter
}

// MARK: raw keyspace helpers

func rawSet(t *testing.T, adapter *BadgerAdapter, key kv.Key, value []byte) {
	t.Helper()
	err := adapter.Update(context.Background(), func(txn *BadgerTx) error {
		return txn.Set(key, value)
	})
	if err != nil {
		t.Fatalf("raw set %s failed: %v", key, err)
	}
}

func rawGet(t *testing.T, adapter *BadgerAdapter, key kv.Key) []byte {
	t.Helper()
	var value []byte
	err := adapter.View(context.Background(), func(txn *BadgerTx) error {
		var err error
		value, err = txn.Get(key)
		return err
	})
	if err != nil {
		t.Fatalf("raw get %s failed: %v", key, err)
	}
	return value
}

func rawCount(t *testing.T, adapter *BadgerAdapter, prefix kv.Key) int {
	t.Helper()
	count := 0
	err := adapter.View(context.Background(), func(txn *BadgerTx) error {
		for range txn.IterKeys(prefix, kv.KeyRange{}) {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("raw count %s failed: %v", prefix, err)
	}
	return count
}

func generationKey(stream string) kv.Key { return kv.Key{"stream", "generation", stream} }
func retirementKey(stream string) kv.Key { return kv.Key{"stream", "retire", stream} }
func configKey(stream string) kv.Key     { return kv.Key{"stream", "config", stream} }

// plantRetirement writes a durable retirement record directly, simulating a
// purge interrupted (or being run by another process) at the given phase.
func plantRetirement(
	t *testing.T,
	adapter *BadgerAdapter,
	stream string,
	generation uint64,
	phase transactions.RetirementPhase,
) {
	t.Helper()

	r := transactions.Retirement{
		Stream:     stream,
		Generation: generation,
		Phase:      phase,
		Stages: map[string]transactions.RetirementStage{
			string(transactions.StageEntries): {},
			string(transactions.StageIndexes): {},
			string(transactions.StageFields):  {},
		},
		StartedAt: time.Now().Add(-time.Minute),
		UpdatedAt: time.Now(),
	}

	value, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal retirement: %v", err)
	}

	rawSet(t, adapter, retirementKey(stream), value)
}

// plantEntry writes an entry body key directly.
func plantEntry(t *testing.T, adapter *BadgerAdapter, stream, ts, uuid string) {
	t.Helper()
	record := models.NewLogRecord(map[string]string{"level": "info"})
	value, _ := json.Marshal(record)
	rawSet(t, adapter, kv.Key{"entry", stream, ts, uuid}, value)
}

// plantOrphanIndex writes an inverted-index key with no matching entry body,
// the exact residue a purge/GC interleaving could otherwise leave behind.
func plantOrphanIndex(t *testing.T, adapter *BadgerAdapter, stream, field, value, ts, uuid string) {
	t.Helper()
	indexKey := append(
		kv.Key{"index", stream, "field", field, value},
		kv.Key{"entry", stream, ts, uuid}...,
	)
	rawSet(t, adapter, indexKey, []byte{})
}

// MARK: assertions

func assertStreamKeyspaceEmpty(t *testing.T, store storage.LogStorage, adapter *BadgerAdapter, stream string) {
	t.Helper()

	if got := rawCount(t, adapter, kv.Key{"entry", stream}); got != 0 {
		t.Fatalf("expected no entry keys, got %d", got)
	}
	if got := rawCount(t, adapter, kv.Key{"index", stream}); got != 0 {
		t.Fatalf("expected no index keys, got %d", got)
	}
	if got := rawCount(t, adapter, kv.Key{"stream", "field", stream}); got != 0 {
		t.Fatalf("expected no field keys, got %d", got)
	}

	from := time.Now().Add(-time.Hour)
	to := time.Now().Add(time.Hour)

	recs, err := store.FetchLogs(t.Context(), stream, from, to, nil, nil)
	if err != nil {
		t.Fatalf("fetch logs: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected no records, got %d", len(recs))
	}

	fields, err := store.ListStreamFields(t.Context(), stream)
	if err != nil {
		t.Fatalf("list fields: %v", err)
	}
	if len(fields) != 0 {
		t.Fatalf("expected no fields, got %v", fields)
	}

	indices, err := store.Distinct(t.Context(), stream)
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	if len(indices) != 0 {
		t.Fatalf("expected no indices, got %v", indices)
	}

	configs, err := store.ListStreamConfigs(t.Context())
	if err != nil {
		t.Fatalf("list configs: %v", err)
	}
	if _, ok := configs[stream]; ok {
		t.Fatalf("stream config still listed")
	}
}

func ingestN(t *testing.T, store storage.LogStorage, stream string, n int, field string) {
	t.Helper()
	for i := 0; i < n; i++ {
		fields := map[string]string{"seq": strings.Repeat("x", 1)}
		if field != "" {
			fields[field] = "v"
		}
		if _, err := store.Ingest(t.Context(), stream, models.NewLogRecord(fields)); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
}

// MARK: tests

// TestRetireUnknownStreamIsNoOp (AC-1): purging a never-seen name succeeds
// without creating a generation counter or a retirement record.
func TestRetireUnknownStreamIsNoOp(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	if err := store.DeleteStream(ctx, "ghost"); err != nil {
		t.Fatalf("purge unknown stream: %v", err)
	}

	if rawGet(t, adapter, generationKey("ghost")) != nil {
		t.Fatal("purge of an unknown stream created a generation key")
	}
	if rawGet(t, adapter, retirementKey("ghost")) != nil {
		t.Fatal("purge of an unknown stream created a retirement record")
	}

	state, err := store.StreamState(ctx, "ghost")
	if err != nil {
		t.Fatalf("stream state: %v", err)
	}
	if state.Generation != 0 || state.Exists || state.Retiring {
		t.Fatalf("unexpected state for unknown stream: %+v", state)
	}
}

// TestRetireEmptyConfiguredStream (AC-1): a stream with a config but no records
// purges cleanly and leaves its generation counter behind.
func TestRetireEmptyConfiguredStream(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	if _, err := store.GetOrCreateStreamConfig(ctx, "empty"); err != nil {
		t.Fatalf("create config: %v", err)
	}

	state, _ := store.StreamState(ctx, "empty")
	if state.Generation != 1 || !state.Exists || state.Retiring {
		t.Fatalf("unexpected initial state: %+v", state)
	}

	if err := store.DeleteStream(ctx, "empty"); err != nil {
		t.Fatalf("purge empty stream: %v", err)
	}

	assertStreamKeyspaceEmpty(t, store, adapter, "empty")

	if string(rawGet(t, adapter, generationKey("empty"))) != "1" {
		t.Fatal("generation counter should remain at 1 after purge")
	}
	if rawGet(t, adapter, retirementKey("empty")) != nil {
		t.Fatal("retirement record should be gone after finalization")
	}

	state, _ = store.StreamState(ctx, "empty")
	if state.Generation != 1 || state.Exists || state.Retiring {
		t.Fatalf("unexpected post-purge state: %+v", state)
	}
}

// TestRetirePopulatedStreamConverges (AC-3): entries, indexes, field metadata
// and config all converge to zero with a batch size smaller than the data set.
func TestRetirePopulatedStreamConverges(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	if err := store.ConfigureStream(ctx, "test", models.StreamConfig{
		IndexedFields: []string{"level"},
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	ingestN(t, store, "test", 7, "level")

	if err := store.DeleteStream(ctx, "test"); err != nil {
		t.Fatalf("purge: %v", err)
	}

	assertStreamKeyspaceEmpty(t, store, adapter, "test")
}

// TestFenceRejectsWritesConfigGets (AC-2): while the fence is open, ingest,
// GET-or-create and configure are rejected with ErrStreamRetiring and cannot
// recreate the stream; the keyspace gains no new keys.
func TestFenceRejectsWritesConfigGets(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	if _, err := store.GetOrCreateStreamConfig(ctx, "test"); err != nil {
		t.Fatalf("create config: %v", err)
	}
	ingestN(t, store, "test", 2, "")

	entriesBefore := rawCount(t, adapter, kv.Key{"entry", "test"})

	generation, existed, err := store.FenceStream(ctx, "test")
	if err != nil {
		t.Fatalf("fence: %v", err)
	}
	if !existed || generation != 1 {
		t.Fatalf("unexpected fence result: gen=%d existed=%v", generation, existed)
	}

	if _, err := store.Ingest(ctx, "test", models.NewLogRecord(map[string]string{"x": "y"})); !errors.Is(err, storage.ErrStreamRetiring) {
		t.Fatalf("ingest should be rejected, got: %v", err)
	}
	if _, err := store.GetOrCreateStreamConfig(ctx, "test"); !errors.Is(err, storage.ErrStreamRetiring) {
		t.Fatalf("get-or-create should be rejected, got: %v", err)
	}
	if err := store.ConfigureStream(ctx, "test", models.StreamConfig{}); !errors.Is(err, storage.ErrStreamRetiring) {
		t.Fatalf("configure should be rejected, got: %v", err)
	}

	if got := rawCount(t, adapter, kv.Key{"entry", "test"}); got != entriesBefore {
		t.Fatalf("fenced writes changed the keyspace: %d before vs %d after", entriesBefore, got)
	}

	state, _ := store.StreamState(ctx, "test")
	if !state.Retiring || state.Generation != 1 || !state.Exists {
		t.Fatalf("unexpected fenced state: %+v", state)
	}

	if err := store.FinishStreamRetirement(ctx, "test"); err != nil {
		t.Fatalf("finish: %v", err)
	}

	state, _ = store.StreamState(ctx, "test")
	if state.Retiring {
		t.Fatal("stream should no longer be retiring after finish")
	}
}

// TestPurgeConcurrentWrites (AC-3): writes racing a purge either complete and
// get swept with the rest or are rejected; after it returns the keyspace is
// empty in every observed outcome.
func TestPurgeConcurrentWrites(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)
	if _, err := store.GetOrCreateStreamConfig(ctx, "test"); err != nil {
		t.Fatalf("create config: %v", err)
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = store.Ingest(t.Context(), "test", models.NewLogRecord(map[string]string{"i": "1"}))
		}()
	}

	if err := store.DeleteStream(ctx, "test"); err != nil {
		t.Fatalf("purge: %v", err)
	}
	wg.Wait()

	// Any write admitted after the fence was rejected; any write that
	// completed beforehand was swept: the final keyspace must be empty even if
	// a straggler is still returning.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rawCount(t, adapter, kv.Key{"entry", "test"}) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	assertStreamKeyspaceEmpty(t, store, adapter, "test")
}

// TestRecoverDeletingRetirementWithOrphanIndexes (AC-4/AC-5): an interrupted
// deleting-phase retirement, including index-only orphan keys, is driven to
// completion by startup recovery; the generation counter survives.
func TestRecoverDeletingRetirementWithOrphanIndexes(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	rawSet(t, adapter, generationKey("test"), []byte("1"))
	rawSet(t, adapter, configKey("test"), []byte{})
	plantEntry(t, adapter, "test", "00000000000000000001", "a")
	plantEntry(t, adapter, "test", "00000000000000000002", "b")
	plantOrphanIndex(t, adapter, "test", "level", "aW5mbw==", "00000000000000000009", "z")
	rawSet(t, adapter, kv.Key{"stream", "field", "test", "level"}, []byte{})
	plantRetirement(t, adapter, "test", 1, transactions.PhaseDeleting)

	if err := store.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	assertStreamKeyspaceEmpty(t, store, adapter, "test")
	if rawGet(t, adapter, retirementKey("test")) != nil {
		t.Fatal("retirement record should be finalized")
	}
	if string(rawGet(t, adapter, generationKey("test"))) != "1" {
		t.Fatal("generation counter must survive recovery")
	}
}

// TestRecoverResweepsStaleDoneStage (AC-4): a stage persisted as Done must not
// be trusted — if keys are present again (a late commit or an uncertain delete
// response), recovery rescans the prefix from its root and converges.
func TestRecoverResweepsStaleDoneStage(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	rawSet(t, adapter, generationKey("test"), []byte("1"))
	rawSet(t, adapter, configKey("test"), []byte{})
	plantEntry(t, adapter, "test", "00000000000000000001", "a")
	plantEntry(t, adapter, "test", "00000000000000000002", "b")

	r := transactions.Retirement{
		Stream:     "test",
		Generation: 1,
		Phase:      transactions.PhaseDeleting,
		Stages: map[string]transactions.RetirementStage{
			// Entries incorrectly believed done while bodies still exist.
			string(transactions.StageEntries): {Deleted: 40, Done: true},
			string(transactions.StageIndexes): {Done: true},
			string(transactions.StageFields):  {Done: true},
		},
		StartedAt: time.Now().Add(-time.Minute),
		UpdatedAt: time.Now(),
	}
	value, _ := json.Marshal(r)
	rawSet(t, adapter, retirementKey("test"), value)

	if err := store.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	assertStreamKeyspaceEmpty(t, store, adapter, "test")
}

// TestRecoverEarlyPhases (AC-5): retirements interrupted while fencing or
// draining recover to completion.
func TestRecoverEarlyPhases(t *testing.T) {
	for _, phase := range []transactions.RetirementPhase{
		transactions.PhaseFencing,
		transactions.PhaseDraining,
		transactions.PhaseFinalizing,
	} {
		t.Run(string(phase), func(t *testing.T) {
			ctx, store, adapter := newRetirementTestStore(t, 2)

			rawSet(t, adapter, generationKey("test"), []byte("1"))
			rawSet(t, adapter, configKey("test"), []byte{})
			for i := range 5 {
				plantEntry(
					t, adapter, "test",
					"000000000000000000"+string(rune('0'+i+1)), "u",
				)
			}
			plantRetirement(t, adapter, "test", 1, phase)

			if err := store.RecoverRetirements(ctx); err != nil {
				t.Fatalf("recover at %s: %v", phase, err)
			}

			assertStreamKeyspaceEmpty(t, store, adapter, "test")
		})
	}
}

// TestRecoverDiscardsStaleRetirement (AC-5/AC-6): a retirement record whose
// generation is behind the counter belongs to an obsolete lineage; recovery
// removes only the record and leaves the rebuilt stream's data intact.
func TestRecoverDiscardsStaleRetirement(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	// Rebuilt generation 2: config and data present.
	rawSet(t, adapter, generationKey("test"), []byte("2"))
	rawSet(t, adapter, configKey("test"), []byte{})
	plantEntry(t, adapter, "test", "00000000000000000010", "g2")
	plantEntry(t, adapter, "test", "00000000000000000011", "g2")

	// A zombie retirement for generation 1.
	plantRetirement(t, adapter, "test", 1, transactions.PhaseDeleting)

	if err := store.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if rawGet(t, adapter, retirementKey("test")) != nil {
		t.Fatal("stale retirement record should be discarded")
	}
	if got := rawCount(t, adapter, kv.Key{"entry", "test"}); got != 2 {
		t.Fatalf("rebuilt stream data must be preserved, got %d entries", got)
	}
	if rawGet(t, adapter, configKey("test")) == nil {
		t.Fatal("rebuilt stream config must be preserved")
	}

	state, _ := store.StreamState(ctx, "test")
	if state.Generation != 2 || !state.Exists || state.Retiring {
		t.Fatalf("unexpected rebuilt state: %+v", state)
	}
}

// TestSameNameRebuildIsolatedFromOldCleanup (AC-6): after gen 1 is purged and a
// same-name gen 2 is written, replaying an old (gen 1) cleanup never touches
// gen 2 data; GC keeps working on the new generation.
func TestSameNameRebuildIsolatedFromOldCleanup(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	if err := store.ConfigureStream(ctx, "test", models.StreamConfig{
		IndexedFields: []string{"level"},
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	ingestN(t, store, "test", 3, "level")

	if err := store.DeleteStream(ctx, "test"); err != nil {
		t.Fatalf("purge gen1: %v", err)
	}

	// Rebuild under the same name: the first write allocates generation 2.
	if _, err := store.Ingest(ctx, "test", models.NewLogRecord(map[string]string{
		"level": "warn",
	})); err != nil {
		t.Fatalf("rebuild ingest: %v", err)
	}

	state, _ := store.StreamState(ctx, "test")
	if state.Generation != 2 || !state.Exists {
		t.Fatalf("rebuilt stream should be generation 2, got %+v", state)
	}

	if err := store.ConfigureStream(ctx, "test", models.StreamConfig{
		IndexedFields: []string{"level"},
		RetentionSize: 1,
		RetentionTime: 0,
	}); err != nil {
		t.Fatalf("configure rebuilt stream: %v", err)
	}

	from := time.Now().Add(-time.Hour)
	to := time.Now().Add(time.Hour)
	recs, err := store.FetchLogs(ctx, "test", from, to, nil, nil)
	if err != nil {
		t.Fatalf("fetch rebuilt: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 rebuilt record, got %d", len(recs))
	}

	// Replay an obsolete gen-1 cleanup (restart finding a stale record after a
	// hypothetical rebuild): recovery must discard it without deleting gen 2.
	plantRetirement(t, adapter, "test", 1, transactions.PhaseDeleting)
	if err := store.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover stale: %v", err)
	}

	recs, err = store.FetchLogs(ctx, "test", from, to, nil, nil)
	if err != nil {
		t.Fatalf("fetch rebuilt after old cleanup replay: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("old cleanup deleted rebuilt-stream data: %d records remain", len(recs))
	}

	// GC must run normally on the new generation.
	if err := store.CollectGarbage(ctx); err != nil {
		t.Fatalf("gc on rebuilt stream: %v", err)
	}
}

// TestDuplicateConcurrentPurge (AC-7): duplicate purges open a single
// generation, both succeed, and the keyspace converges once.
func TestDuplicateConcurrentPurge(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)
	if _, err := store.GetOrCreateStreamConfig(ctx, "test"); err != nil {
		t.Fatalf("create config: %v", err)
	}
	ingestN(t, store, "test", 5, "")

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = store.DeleteStream(t.Context(), "test")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("purge %d failed: %v", i, err)
		}
	}

	assertStreamKeyspaceEmpty(t, store, adapter, "test")
	if gen := string(rawGet(t, adapter, generationKey("test"))); gen != "1" {
		t.Fatalf("generation must be incremented exactly once, got %q", gen)
	}
}

// TestGCSkipsRetiringStream (AC-8): GC does not evict a fenced generation,
// while its normal budget enforcement is unaffected for other streams.
func TestGCSkipsRetiringStream(t *testing.T) {
	ctx, store, adapter := newRetirementTestStore(t, 3)

	// Over-budget retiring stream.
	if err := store.ConfigureStream(ctx, "fenced", models.StreamConfig{
		RetentionSize: 1,
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	blob := strings.Repeat("x", 50_000)
	for range 20 {
		if _, err := store.Ingest(ctx, "fenced", models.NewLogRecord(map[string]string{"b": blob})); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}

	entriesBefore := rawCount(t, adapter, kv.Key{"entry", "fenced"})
	plantRetirement(t, adapter, "fenced", 1, transactions.PhaseDraining)

	if err := store.CollectGarbage(ctx); err != nil {
		t.Fatalf("gc: %v", err)
	}

	if got := rawCount(t, adapter, kv.Key{"entry", "fenced"}); got != entriesBefore {
		t.Fatalf("GC must not evict a retiring stream: %d before vs %d after", entriesBefore, got)
	}

	// The fence can still be driven to completion by a duplicate purge.
	if err := store.DeleteStream(ctx, "fenced"); err != nil {
		t.Fatalf("purge fenced stream: %v", err)
	}
	assertStreamKeyspaceEmpty(t, store, adapter, "fenced")
}
