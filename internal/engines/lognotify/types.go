package lognotify

import (
	"errors"

	"github.com/vladopajic/go-actor/actor"

	"link-society.com/flowg/internal/models"
)

// ErrStreamRetired is returned when a subscription targets a generation that
// has already been closed by a purge. Callers should not retry the same
// generation; watching a never-created stream (generation 0) is still allowed.
var ErrStreamRetired = errors.New("stream generation has been retired")

// TerminationReason explains why a watcher's stream was closed.
type TerminationReason string

const (
	// TerminationPurged is delivered when the stream's generation is purged.
	TerminationPurged TerminationReason = "stream_purged"
)

// StreamTermination is the definitive terminal notice handed to a watcher: the
// generation it watched and why it ended.
type StreamTermination struct {
	Generation uint64
	Reason     TerminationReason
}

// LogMessage is a single record delivered to subscribers, carrying both the
// record and the storage key it was persisted under. When Terminated is set,
// the message is not a log record but the stream's definitive termination
// notice; the per-subscriber mailbox is stopped right after it is queued, so a
// ranging watcher receives the notice first and then observes the stream end.
type LogMessage struct {
	Stream     string
	LogKey     string
	LogRecord  models.LogRecord
	Terminated *StreamTermination
}

// SubscribeMessage requests a new subscription on the actor. SenderM is the
// mailbox the actor should push matching records to, Generation is the stream
// generation observed by the caller (0 for a never-created stream), ReadyC
// reports when the registration is complete (or is rejected because the
// generation was already retired), and DoneC signals that the subscription
// should be dropped.
type SubscribeMessage struct {
	Stream     string
	Generation uint64
	SenderM    actor.Mailbox[LogMessage]
	ReadyC     chan<- ReadyResponse
	DoneC      <-chan struct{}
}

// CloseStreamMessage requests the definite termination of every subscriber of
// a stream whose generation is at most Generation: the actor raises its
// retired-generation watermark (so later subscriptions to the old generation
// are rejected), queues a termination notice on each subscriber mailbox and
// stops those mailboxes. ReadyC acknowledges the close once it is registered.
type CloseStreamMessage struct {
	Stream     string
	Generation uint64
	Reason     TerminationReason
	ReadyC     chan<- ReadyResponse
}

// ReadyResponse acknowledges a SubscribeMessage or CloseStreamMessage,
// reporting any registration error.
type ReadyResponse struct {
	Err error
}
