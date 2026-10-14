package confignotify

import (
	"github.com/vladopajic/go-actor/actor"
)

// EventKind discriminates configuration change events.
type EventKind int

const (
	// PipelineChanged reports that a pipeline was created or updated.
	PipelineChanged EventKind = iota
	// PipelineDeleted reports that a pipeline was removed.
	PipelineDeleted
	// DependenciesChanged reports a change that may affect every pipeline build:
	// a transformer or forwarder mutation, or a configuration restore.
	DependenciesChanged
)

// Event is a single configuration change notification. Pipeline carries the
// affected pipeline name for the pipeline-scoped kinds and is empty for
// DependenciesChanged.
//
// Version is the global configuration version committed with the change:
// storage commit, notification, dependency resolution, candidate build and
// instance switch are all correlated through it. Consumers skip events whose
// version they already acknowledged. A zero Version means "unknown" (legacy
// producers) and must be processed unconditionally.
type Event struct {
	Kind     EventKind
	Pipeline string
	Version  uint64
}

// SubscribeMessage requests a new subscription on the actor. SenderM is the
// mailbox the actor should push events to, ReadyC reports when the
// registration is complete, and DoneC signals that the subscription should be
// dropped.
type SubscribeMessage struct {
	SenderM actor.Mailbox[Event]
	ReadyC  chan<- ReadyResponse
	DoneC   <-chan struct{}
}

// ReadyResponse acknowledges a SubscribeMessage, reporting any registration
// error.
type ReadyResponse struct {
	Err error
}
