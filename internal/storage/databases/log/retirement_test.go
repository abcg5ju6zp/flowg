package log

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"link-society.com/flowg/internal/models"

	badgerbackend "link-society.com/flowg/internal/storage/backends/badger"
	"link-society.com/flowg/internal/storage/databases/log/transactions"
	"link-society.com/flowg/internal/storage/generic/kv"
	storage "link-society.com/flowg/internal/storage/interfaces"
)

// testAdapterOptions is the option type driving the badger adapter fx module
// directly (the concrete/log module cannot be imported here without creating an
// import cycle, since it wraps this very package).
type testAdapterOptions struct{}

// newRetirementTestStorage builds an in-memory badger-backed Storage directly,
// with a tiny batch size so retirement stages span several transactions. No
// recovery actor or terminator is attached, so tests drive retirements and
// recovery deterministically themselves.
func newRetirementTestStorage(t *testing.T, batchSize int) *Storage[*badgerbackend.BadgerTx, *badgerbackend.BadgerTx] {
	t.Helper()

	var adapter *badgerbackend.BadgerAdapter

	app := fxtest.New(
		t,
		fx.Supply(slog.New(slog.DiscardHandler)),
		fx.Supply(testAdapterOptions{}),
		badgerbackend.NewAdapter("storage.log", func(testAdapterOptions) badgerbackend.AdapterOptions {
			return badgerbackend.AdapterOptions{InMemory: true}
		}),
		fx.Invoke(func(params struct {
			fx.In
			Adapter *badgerbackend.BadgerAdapter `name:"storage.log"`
		}) {
			adapter = params.Adapter
		}),
		fx.NopLogger,
	)
	app.RequireStart()
	t.Cleanup(app.RequireStop)

	return NewStorage(slog.New(slog.DiscardHandler), adapter, batchSize, nil)
}

// putRetirementMarker writes a retirement tombstone directly, simulating a
// purge interrupted by a crash at a specific stage.
func putRetirementMarker(
	t *testing.T,
	s *Storage[*badgerbackend.BadgerTx, *badgerbackend.BadgerTx],
	ctx context.Context,
	stream, generation string,
	mutate func(rec *transactions.RetirementRecord),
) {
	t.Helper()

	rec := transactions.NewRetirementRecord(stream, generation)
	if mutate != nil {
		mutate(rec)
	}

	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal retirement record: %v", err)
	}

	err = s.adapter.Update(ctx, func(txn *badgerbackend.BadgerTx) error {
		return txn.Set(kv.Key{"stream", "retire", stream}, raw)
	})
	if err != nil {
		t.Fatalf("inject retirement marker: %v", err)
	}
}

func streamGeneration(
	t *testing.T,
	s *Storage[*badgerbackend.BadgerTx, *badgerbackend.BadgerTx],
	ctx context.Context,
	stream string,
) string {
	t.Helper()

	var generation string

	err := s.adapter.View(ctx, func(txn *badgerbackend.BadgerTx) error {
		var err error
		generation, err = transactions.FetchStreamGeneration(txn, stream)
		return err
	})
	if err != nil {
		t.Fatalf("fetch generation: %v", err)
	}

	return generation
}

func hasRetirementMarker(
	t *testing.T,
	s *Storage[*badgerbackend.BadgerTx, *badgerbackend.BadgerTx],
	ctx context.Context,
	stream string,
) bool {
	t.Helper()

	var present bool

	err := s.adapter.View(ctx, func(txn *badgerbackend.BadgerTx) error {
		rec, err := transactions.FetchRetirement(txn, stream)
		if err != nil {
			return err
		}
		present = rec != nil
		return nil
	})
	if err != nil {
		t.Fatalf("fetch retirement marker: %v", err)
	}

	return present
}

// Purging a name that never held anything stays an idempotent no-op that does
// not even mint a generation.
func TestPurgeNonexistentStreamIsNoop(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)

	if err := s.DeleteStream(ctx, "ghost"); err != nil {
		t.Fatalf("purging a nonexistent stream should succeed: %v", err)
	}
	if err := s.DeleteStream(ctx, "ghost"); err != nil {
		t.Fatalf("re-purging a nonexistent stream should be idempotent: %v", err)
	}

	if hasRetirementMarker(t, s, ctx, "ghost") {
		t.Fatalf("no tombstone should exist for a never-existing stream")
	}
	if gen := streamGeneration(t, s, ctx, "ghost"); gen != "" {
		t.Fatalf("no generation should be minted for a never-existing stream, got %q", gen)
	}

	configs, err := s.ListStreamConfigs(ctx)
	if err != nil {
		t.Fatalf("list configs: %v", err)
	}
	if len(configs) != 0 {
		t.Fatalf("expected no configs, got %v", configs)
	}
}

// An empty, configured stream (only config + generation keys) is purged like
// any other, and watching its data stays empty.
func TestPurgeEmptyConfiguredStream(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)
	const stream = "empty"

	if err := s.ConfigureStream(ctx, stream, models.StreamConfig{
		IndexedFields: []string{"level"},
	}); err != nil {
		t.Fatalf("configure stream: %v", err)
	}
	if gen := streamGeneration(t, s, ctx, stream); gen == "" {
		t.Fatalf("expected a generation token after configure")
	}

	if err := s.DeleteStream(ctx, stream); err != nil {
		t.Fatalf("purge empty stream: %v", err)
	}

	if hasRetirementMarker(t, s, ctx, stream) {
		t.Fatalf("tombstone should be removed on completion")
	}
	if gen := streamGeneration(t, s, ctx, stream); gen != "" {
		t.Fatalf("generation token should be removed, got %q", gen)
	}

	configs, err := s.ListStreamConfigs(ctx)
	if err != nil {
		t.Fatalf("list configs: %v", err)
	}
	if _, ok := configs[stream]; ok {
		t.Fatalf("empty stream config should be gone")
	}
}

// Full purge across all stages with a tiny batch size, followed by recreation
// under a distinct generation whose data is fully readable.
func TestPurgeStreamThenRecreateUnderNewGeneration(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)
	const stream = "test"

	if err := s.ConfigureStream(ctx, stream, models.StreamConfig{
		IndexedFields: []string{"level"},
	}); err != nil {
		t.Fatalf("configure stream: %v", err)
	}

	for i := 0; i < 7; i++ {
		if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{
			"level": fmt.Sprintf("l%d", i),
		})); err != nil {
			t.Fatalf("ingest record: %v", err)
		}
	}

	oldGeneration := streamGeneration(t, s, ctx, stream)
	if oldGeneration == "" {
		t.Fatalf("expected generation token")
	}

	if err := s.DeleteStream(ctx, stream); err != nil {
		t.Fatalf("purge stream: %v", err)
	}

	// Every stage boundary must be empty.
	for _, stage := range transactions.OrderedRetirementStages {
		var count int
		if err := s.adapter.View(ctx, func(txn *badgerbackend.BadgerTx) error {
			var err error
			count, err = transactions.CountRetirementStageKeys(txn, stream, stage)
			return err
		}); err != nil {
			t.Fatalf("count stage %s: %v", stage, err)
		}
		if count != 0 {
			t.Fatalf("stage %s should be empty after purge, %d keys remain", stage, count)
		}
	}

	if hasRetirementMarker(t, s, ctx, stream) {
		t.Fatalf("tombstone should be gone")
	}

	from := time.Now().Add(-time.Minute)
	to := time.Now().Add(time.Minute)
	recs, err := s.FetchLogs(ctx, stream, from, to, nil, nil)
	if err != nil {
		t.Fatalf("fetch after purge: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected no records after purge, got %d", len(recs))
	}

	// Recreate the same name: writes succeed under a new generation.
	for i := 0; i < 3; i++ {
		if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{
			"level": "new",
		})); err != nil {
			t.Fatalf("ingest into recreated stream: %v", err)
		}
	}

	newGeneration := streamGeneration(t, s, ctx, stream)
	if newGeneration == "" || newGeneration == oldGeneration {
		t.Fatalf("recreated stream must own a new generation, old=%q new=%q", oldGeneration, newGeneration)
	}

	recs, err = s.FetchLogs(ctx, stream, from, to.Add(time.Minute), nil, nil)
	if err != nil {
		t.Fatalf("fetch after recreate: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 records of the new generation, got %d", len(recs))
	}
}

// While a generation tombstone is present, late writes and reconfiguration
// fail with a definite error instead of landing behind the purge.
func TestWritesRejectedWhileGenerationRetiring(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)
	const stream = "retiring"

	if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{"a": "b"})); err != nil {
		t.Fatalf("initial ingest: %v", err)
	}
	generation := streamGeneration(t, s, ctx, stream)

	putRetirementMarker(t, s, ctx, stream, generation, nil)

	if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{"a": "late"})); !errors.Is(err, storage.ErrStreamRetiring) {
		t.Fatalf("late ingest should fail with ErrStreamRetiring, got: %v", err)
	}

	if err := s.ConfigureStream(ctx, stream, models.StreamConfig{
		IndexedFields: []string{"a"},
	}); !errors.Is(err, storage.ErrStreamRetiring) {
		t.Fatalf("configure during purge should fail with ErrStreamRetiring, got: %v", err)
	}

	if _, err := s.GetOrCreateStreamConfig(ctx, stream); !errors.Is(err, storage.ErrStreamRetiring) {
		t.Fatalf("get-or-create during purge should fail with ErrStreamRetiring, got: %v", err)
	}

	// Recovery finishes the interrupted purge and the name becomes writable
	// again.
	if err := s.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover retirements: %v", err)
	}
	if hasRetirementMarker(t, s, ctx, stream) {
		t.Fatalf("marker should be cleared after recovery")
	}
	if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{"a": "ok"})); err != nil {
		t.Fatalf("ingest after recovered purge should succeed: %v", err)
	}
}

// The core safety property: a cleanup belonging to a generation that no
// longer owns the name cannot delete anything, even when driven directly.
func TestStaleGenerationCannotDeleteRebuiltStream(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)
	const stream = "rebuilt"

	if err := s.ConfigureStream(ctx, stream, models.StreamConfig{
		IndexedFields: []string{"level"},
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{
		"level": "v",
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	if err := s.DeleteStream(ctx, stream); err != nil {
		t.Fatalf("purge: %v", err)
	}

	// Same-named stream, new generation, fresh data.
	if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{
		"level": "v2",
	})); err != nil {
		t.Fatalf("recreate ingest: %v", err)
	}

	// An old-generation batch (resumed worker from a dead process, duplicate
	// cleanup holding the old token) must hit the fence before touching keys.
	err := s.adapter.Update(ctx, func(txn *badgerbackend.BadgerTx) error {
		_, err := transactions.DeleteRetirementStageBatch(
			txn, stream, "stale-old-generation", transactions.StageEntries, 100,
		)
		return err
	})
	if !errors.Is(err, transactions.ErrGenerationMoved) {
		t.Fatalf("stale entries batch should fail with ErrGenerationMoved, got: %v", err)
	}

	err = s.adapter.Update(ctx, func(txn *badgerbackend.BadgerTx) error {
		return transactions.FinishRetirement(txn, stream, "stale-old-generation")
	})
	if !errors.Is(err, transactions.ErrGenerationMoved) {
		t.Fatalf("stale finalize should fail with ErrGenerationMoved, got: %v", err)
	}

	// The new generation's data is intact.
	from := time.Now().Add(-time.Minute)
	to := time.Now().Add(time.Minute)
	recs, err := s.FetchLogs(ctx, stream, from, to, nil, nil)
	if err != nil {
		t.Fatalf("fetch rebuilt stream: %v", err)
	}
	if len(recs) != 1 || recs[0].Fields["level"] != "v2" {
		t.Fatalf("rebuilt stream data was corrupted by stale cleanup: %v", recs)
	}
}

// A crash after the entries stage was marked done but keys remained (or an
// interleaving left orphans) is resumed from the keyspace facts: the stage is
// reopened, everything is deleted and the tombstone removed.
func TestRecoverInterruptedRetirement(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 2)
	const stream = "interrupted"

	if err := s.ConfigureStream(ctx, stream, models.StreamConfig{
		IndexedFields: []string{"level"},
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	for i := 0; i < 6; i++ {
		if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{
			"level": fmt.Sprintf("l%d", i),
		})); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	generation := streamGeneration(t, s, ctx, stream)

	// Crash state: entries stage claims to be done while entries and every
	// later stage still hold keys; indexes stage was running.
	putRetirementMarker(t, s, ctx, stream, generation, func(rec *transactions.RetirementRecord) {
		rec.Stages[transactions.StageEntries].Status = transactions.StageStatusDone
		rec.Stages[transactions.StageIndexes].Status = transactions.StageStatusRunning
	})

	if err := s.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if hasRetirementMarker(t, s, ctx, stream) {
		t.Fatalf("recovered purge should remove the marker")
	}

	for _, stage := range transactions.OrderedRetirementStages {
		var count int
		if err := s.adapter.View(ctx, func(txn *badgerbackend.BadgerTx) error {
			var err error
			count, err = transactions.CountRetirementStageKeys(txn, stream, stage)
			return err
		}); err != nil {
			t.Fatalf("count stage: %v", err)
		}
		if count != 0 {
			t.Fatalf("stage %s still has %d keys after recovery", stage, count)
		}
	}

	// Recovery is idempotent.
	if err := s.RecoverRetirements(ctx); err != nil {
		t.Fatalf("second recovery should be a no-op: %v", err)
	}
}

// A crash after all data and the configuration were cleared but before the
// tombstone itself was removed is finalized by recovery without touching
// anything else.
func TestRecoverFinalizesMarkerWhenStagesAlreadyEmpty(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)
	const stream = "almost-done"

	putRetirementMarker(t, s, ctx, stream, "retirement-legacy-id", func(rec *transactions.RetirementRecord) {
		for _, stage := range transactions.OrderedRetirementStages {
			rec.Stages[stage].Status = transactions.StageStatusDone
		}
	})

	if err := s.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if hasRetirementMarker(t, s, ctx, stream) {
		t.Fatalf("final recovery must remove the tombstone")
	}
}

// A legacy stream that holds data but never had a generation token (as
// pre-generation builds could leave behind) is purged by recovery under a
// freshly minted retirement id.
func TestRecoverLegacyDataOnlyStream(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 2)
	const stream = "legacy"

	entryKey := kv.Key{
		"entry", stream,
		fmt.Sprintf("%020d", time.Now().UnixMilli()),
		"legacy-uuid",
	}
	rawRecord, err := json.Marshal(models.NewLogRecord(map[string]string{"level": "x"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := s.adapter.Update(ctx, func(txn *badgerbackend.BadgerTx) error {
		return txn.Set(entryKey, rawRecord)
	}); err != nil {
		t.Fatalf("seed legacy entry: %v", err)
	}

	if gen := streamGeneration(t, s, ctx, stream); gen != "" {
		t.Fatalf("legacy stream should have no generation token")
	}

	if err := s.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover with no marker should be a no-op: %v", err)
	}

	// A live purge mints the retirement id itself when the token is missing.
	if err := s.DeleteStream(ctx, stream); err != nil {
		t.Fatalf("purge legacy stream: %v", err)
	}

	var count int
	if err := s.adapter.View(ctx, func(txn *badgerbackend.BadgerTx) error {
		var err error
		count, err = transactions.CountRetirementStageKeys(txn, stream, transactions.StageEntries)
		return err
	}); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if count != 0 {
		t.Fatalf("legacy entries should be purged, %d remain", count)
	}
}

// Normal retention GC must not evict from a retiring stream (interleaving is
// what left index-only records behind); once recovery completes the stream is
// fully gone.
func TestGCSkipsRetiringStreamAndRecoveryFinishes(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)
	const stream = "overbudget"
	const retentionMB = int64(1)

	if err := s.ConfigureStream(ctx, stream, models.StreamConfig{
		IndexedFields: []string{"seq"},
		RetentionSize: retentionMB,
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}

	blob := strings.Repeat("x", 50_000)
	const n = 40
	for i := 0; i < n; i++ {
		if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{
			"seq":  fmt.Sprintf("%03d", i),
			"blob": blob,
		})); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}

	generation := streamGeneration(t, s, ctx, stream)
	putRetirementMarker(t, s, ctx, stream, generation, nil)

	if err := s.CollectGarbage(ctx); err != nil {
		t.Fatalf("gc: %v", err)
	}

	usage, err := s.StreamUsage(ctx, stream)
	if err != nil {
		t.Fatalf("usage: %v", err)
	}
	if usage == 0 {
		t.Fatalf("GC must not evict a retiring stream, usage=%d", usage)
	}

	indices, err := s.Distinct(ctx, stream)
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	if len(indices["seq"]) != n {
		t.Fatalf("retiring stream indexes must be untouched, got %d of %d", len(indices["seq"]), n)
	}

	if err := s.RecoverRetirements(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	usage, err = s.StreamUsage(ctx, stream)
	if err != nil {
		t.Fatalf("usage after recovery: %v", err)
	}
	if usage != 0 {
		t.Fatalf("expected empty stream after recovery, usage=%d", usage)
	}
}

// The purge's index boundary scans the whole index prefix, so inverted-index
// keys orphaned without an entry body (the GC/purge interleaving artifact)
// are swept as well.
func TestPurgeSweepsOrphanIndexKeys(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)
	const stream = "orphans"

	if err := s.ConfigureStream(ctx, stream, models.StreamConfig{
		IndexedFields: []string{"level"},
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{
		"level": "real",
	})); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Fabricate an index-only reference whose entry body does not exist, as
	// an interleaved GC/crash could leave behind.
	encoded := base64.StdEncoding.EncodeToString([]byte("ghost"))
	orphanIndexKey := kv.Key{
		"index", stream, "field", "level", encoded,
		"entry", stream, fmt.Sprintf("%020d", time.Now().UnixMilli()), "ghost-uuid",
	}
	if err := s.adapter.Update(ctx, func(txn *badgerbackend.BadgerTx) error {
		return txn.Set(orphanIndexKey, kv.Value{})
	}); err != nil {
		t.Fatalf("inject orphan index: %v", err)
	}

	if err := s.DeleteStream(ctx, stream); err != nil {
		t.Fatalf("purge: %v", err)
	}

	indices, err := s.Distinct(ctx, stream)
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	if len(indices) != 0 {
		t.Fatalf("orphan index keys must be swept, got %v", indices)
	}
}

// Duplicate purges join the in-flight generation; exactly one cleanup runs and
// every caller succeeds.
func TestConcurrentDuplicatePurges(t *testing.T) {
	ctx := t.Context()
	s := newRetirementTestStorage(t, 3)
	const stream = "dupes"

	if err := s.ConfigureStream(ctx, stream, models.StreamConfig{
		IndexedFields: []string{"level"},
	}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	for i := 0; i < 9; i++ {
		if _, err := s.Ingest(ctx, stream, models.NewLogRecord(map[string]string{
			"level": fmt.Sprintf("l%d", i),
		})); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)

	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.DeleteStream(ctx, stream)
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("duplicate purge failed: %v", err)
		}
	}

	if hasRetirementMarker(t, s, ctx, stream) {
		t.Fatalf("tombstone should be gone after joined purges")
	}
	configs, err := s.ListStreamConfigs(ctx)
	if err != nil {
		t.Fatalf("list configs: %v", err)
	}
	if _, ok := configs[stream]; ok {
		t.Fatalf("stream should be purged")
	}
}
