package interfaces

import (
	"context"
	"errors"
	"time"

	"link-society.com/flowg/internal/models"
	"link-society.com/flowg/internal/storage/generic/kv"
	"link-society.com/flowg/internal/utils/langs/filtering"
)

// ErrStreamRetiring is returned by writes and configuration changes that land
// while a stream's generation is being retired by a purge. The stream is
// fenced: the request is rejected outright rather than racing the deletion.
// Callers may retry against the next generation once the stream has been
// recreated.
var ErrStreamRetiring = errors.New("stream is retiring")

// ErrStreamRetired is returned when an operation targets a generation that has
// already been closed by a completed purge (e.g. subscribing to a stream that
// no longer exists but was known in the past). A never-created stream is not
// considered retired.
var ErrStreamRetired = errors.New("stream generation has been retired")

// StreamState is the generation-aware view of a stream used to decide whether
// writes and subscriptions are admissible.
type StreamState struct {
	// Generation is the stream's current generation: 0 when the name was
	// never seen, otherwise the generation of the live stream or, once it has
	// been purged, the generation of the latest retired one.
	Generation uint64
	// Exists reports whether a live stream configuration is present.
	Exists bool
	// Retiring reports whether the current generation is mid-purge: a durable
	// retirement record exists and all writes/subscriptions are fenced.
	Retiring bool
}

// LogStorage is the contract for persisting and querying FlowG's log records,
// organized into streams, along with each stream's configuration and field
// indices.
//
// It embeds [Streamable] so the whole store can be backed up and restored as a
// stream. Implementations live under internal/storage/backends.
type LogStorage interface {
	Streamable

	// ListStreamConfigs returns the configuration of every known stream, keyed
	// by stream name.
	ListStreamConfigs(ctx context.Context) (map[string]models.StreamConfig, error)
	// ListStreamFields returns the set of field names seen in the named stream.
	ListStreamFields(ctx context.Context, stream string) ([]string, error)
	// GetOrCreateStreamConfig returns the configuration of the named stream,
	// creating a default one if it does not exist yet.
	GetOrCreateStreamConfig(ctx context.Context, stream string) (models.StreamConfig, error)
	// ConfigureStream replaces the configuration of the named stream.
	ConfigureStream(ctx context.Context, stream string, config models.StreamConfig) error

	// FenceStream opens the retirement generation of the named stream: it
	// blocks new local writes (the in-memory freeze gate) and persists the
	// durable retirement fence, without deleting anything yet. The returned
	// generation identifies the retired generation; existed is false when the
	// stream had no configuration and no data keys, in which case the call is
	// a side-effect-free success and FinishStreamRetirement must be a no-op.
	// Fencing the same stream again while a retirement is in progress returns
	// the in-progress generation instead of opening a new one.
	FenceStream(ctx context.Context, stream string) (generation uint64, existed bool, err error)
	// FinishStreamRetirement drains in-flight writes, then deletes the fenced
	// generation's entries, indexes, field metadata and configuration in
	// persisted stages, and finally releases the fence. It is idempotent: an
	// interrupted finish can be driven again (including after a restart) until
	// the keyspace converges to zero data keys.
	FinishStreamRetirement(ctx context.Context, stream string) error
	// StreamState returns the stream's generation and whether it currently
	// exists or is mid-retirement.
	StreamState(ctx context.Context, stream string) (StreamState, error)

	// DeleteStream removes the named stream and all of its records. It is the
	// synchronous convenience composition of FenceStream followed by
	// FinishStreamRetirement; callers that need to notify watchers between the
	// fence and the deletion should drive the two steps themselves.
	DeleteStream(ctx context.Context, stream string) error
	// StreamUsage returns an estimate, in bytes, of the storage used by the
	// named stream.
	StreamUsage(ctx context.Context, stream string) (int64, error)

	// IndexField starts indexing the given field of the named stream.
	IndexField(ctx context.Context, stream, field string) error
	// UnindexField stops indexing the given field of the named stream.
	UnindexField(ctx context.Context, stream, field string) error
	// Distinct returns, for each indexed field of the named stream, the set of
	// distinct values observed.
	Distinct(ctx context.Context, stream string) (map[string][]string, error)

	// Ingest stores a log record in the named stream and returns the key it was
	// stored under.
	Ingest(ctx context.Context, stream string, logRecord *models.LogRecord) (kv.Key, error)

	// FetchLogs returns the records of the named stream within the [from, to]
	// time range that satisfy the given filter and indexing constraints.
	FetchLogs(
		ctx context.Context,
		stream string,
		from, to time.Time,
		filter filtering.Filter,
		indexing map[string][]string,
	) ([]models.LogRecord, error)
}
