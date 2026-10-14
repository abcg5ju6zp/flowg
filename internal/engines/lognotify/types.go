package lognotify

import (
	"errors"
	"fmt"

	"github.com/vladopajic/go-actor/actor"

	"link-society.com/flowg/internal/models"
)

// ErrStreamRetired is returned by Subscribe while the requested stream is
// closed by an in-progress purge generation. Callers can errors.Is it to report
// a definite rejection instead of silently watching a stream whose data is
// being deleted.
var ErrStreamRetired = errors.New("stream retired")

// StreamRetiredError wraps ErrStreamRetired with the definite termination
// reason reported to the client.
type StreamRetiredError struct {
	Reason string
}

func (e *StreamRetiredError) Error() string {
	return fmt.Sprintf("%s: %s", ErrStreamRetired, e.Reason)
}

func (e *StreamRetiredError) Unwrap() error {
	return ErrStreamRetired
}

// LogMessage is a single record delivered to subscribers, carrying both the
// record and the storage key it was persisted under.
type LogMessage struct {
	Stream    string
	LogKey    string
	LogRecord models.LogRecord

	// Terminated marks the final, synthetic message of a subscription, sent
	// when the stream is retired by a purge. Reason carries the definite
	// termination cause. No further message ever follows it.
	Terminated bool
	Reason     string
}

// SubscribeMessage requests a new subscription on the actor. SenderM is the
// mailbox the actor should push matching records to, ReadyC reports when the
// registration is complete, and DoneC signals that the subscription should be
// dropped.
type SubscribeMessage struct {
	Stream  string
	SenderM actor.Mailbox[LogMessage]
	ReadyC  chan<- ReadyResponse
	DoneC   <-chan struct{}
}

// ReadyResponse acknowledges a SubscribeMessage, reporting any registration
// error.
type ReadyResponse struct {
	Err error
}

// StreamControlMessage changes the lifecycle state of a stream's subscriber
// registry:
//
//   - Close (Close=true) retires the stream: new subscriptions fail with
//     ErrStreamRetired and every current subscriber receives one final,
//     terminated LogMessage carrying Reason,
//   - Reopen (Close=false) clears the retired state when a same-named stream
//     has been recreated, so subscriptions succeed again.
type StreamControlMessage struct {
	Stream string
	Reason string
	Close  bool
}
