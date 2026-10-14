package log

import (
	"context"
	"errors"
	"testing"
	"time"

	storage "link-society.com/flowg/internal/storage/interfaces"
)

func TestRetirementGateAdmitsAndCountsWriters(t *testing.T) {
	gate := newRetirementGate()
	const stream = "s"

	if err := gate.enter(stream); err != nil {
		t.Fatalf("first enter should succeed: %v", err)
	}
	if err := gate.enter(stream); err != nil {
		t.Fatalf("second enter should succeed: %v", err)
	}

	gate.leave(stream)
	gate.leave(stream)

	// A fence acquired after every admitted writer left drains immediately.
	if !gate.tryBecomeOwner(stream) {
		t.Fatalf("expected to become fence owner")
	}
	defer gate.release(stream)

	ctx := t.Context()
	if err := gate.drainWriters(ctx, stream); err != nil {
		t.Fatalf("drain should succeed with no in-flight writer: %v", err)
	}
}

func TestRetirementGateRejectsWritesAndDrainsAdmitted(t *testing.T) {
	gate := newRetirementGate()
	const stream = "s"

	// A batch admitted before the fence is in flight...
	if err := gate.enter(stream); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// ...so fencing the stream succeeds but must wait for that batch.
	if !gate.tryBecomeOwner(stream) {
		t.Fatalf("expected to become fence owner")
	}
	defer gate.release(stream)

	// New writes are rejected immediately.
	if err := gate.enter(stream); !errors.Is(err, storage.ErrStreamRetiring) {
		t.Fatalf("expected ErrStreamRetiring, got %v", err)
	}

	drained := make(chan error, 1)
	go func() {
		drained <- gate.drainWriters(context.Background(), stream)
	}()

	select {
	case err := <-drained:
		t.Fatalf("drain should block while a writer is in flight, returned: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// The admitted batch completes: drain unblocks.
	gate.leave(stream)

	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("drain returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("drain did not unblock after the in-flight writer left")
	}
}

func TestRetirementGateDuplicateOwnerJoinsAndContextCancel(t *testing.T) {
	gate := newRetirementGate()
	const stream = "s"

	if !gate.tryBecomeOwner(stream) {
		t.Fatalf("expected to become fence owner")
	}

	// A second owner cannot fence the same stream concurrently.
	if gate.tryBecomeOwner(stream) {
		t.Fatalf("did not expect a second concurrent owner")
	}

	// waitIdle blocks while fenced and returns on context cancellation.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := gate.waitIdle(ctx, stream); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Once released, the joined waiter proceeds.
	gate.release(stream)
	if err := gate.waitIdle(t.Context(), stream); err != nil {
		t.Fatalf("waitIdle after release should succeed: %v", err)
	}
}
