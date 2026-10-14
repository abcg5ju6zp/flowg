package lognotify_test

import (
	"errors"
	"testing"
	"time"

	"log/slog"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"link-society.com/flowg/internal/engines/lognotify"
)

// newTestNotifier starts a LogNotifier actor graph for the duration of the
// test.
func newTestNotifier(t *testing.T) lognotify.LogNotifier {
	t.Helper()

	var notifier lognotify.LogNotifier

	app := fxtest.New(
		t,
		fx.Supply(slog.New(slog.DiscardHandler)),
		lognotify.NewLogNotifier(),
		fx.Populate(&notifier),
		fx.NopLogger,
	)
	app.RequireStart()
	t.Cleanup(app.RequireStop)

	return notifier
}

// TestCloseStreamTerminatesGenerationWatchers verifies that purging a
// generation delivers a definitive termination notice carrying the reason and
// generation, then closes the watcher's mailbox, and that no further records
// arrive through it.
func TestCloseStreamTerminatesGenerationWatchers(t *testing.T) {
	notifier := newTestNotifier(t)
	ctx := t.Context()

	logM, err := notifier.Subscribe(ctx, "test", 1)
	if err != nil {
		t.Fatalf("unexpected error while subscribing: %v", err)
	}

	if err := notifier.CloseStream(ctx, "test", 1, lognotify.TerminationPurged); err != nil {
		t.Fatalf("unexpected error while closing stream: %v", err)
	}

	// The termination notice must be delivered before the channel closes (the
	// subscription mailbox is configured to stop after receiving all queued
	// messages).
	var gotTermination bool
	for msg := range logM.ReceiveC() {
		if msg.Terminated == nil {
			t.Fatalf("unexpected log record after purge: %+v", msg)
		}

		if msg.Terminated.Generation != 1 {
			t.Fatalf("expected retired generation 1, got %d", msg.Terminated.Generation)
		}
		if msg.Terminated.Reason != lognotify.TerminationPurged {
			t.Fatalf("expected termination reason %q, got %q",
				lognotify.TerminationPurged, msg.Terminated.Reason)
		}

		gotTermination = true
	}

	if !gotTermination {
		t.Fatal("watcher was closed without receiving a termination notice")
	}
}

// TestSubscribeGenerationAdmission verifies the retired-generation watermark:
// subscriptions at or below the retired generation fail definitively, the next
// generation is admitted, and a never-seen name can still be watched at
// generation 0.
func TestSubscribeGenerationAdmission(t *testing.T) {
	notifier := newTestNotifier(t)
	ctx := t.Context()

	// A never-seen stream name stays watchable at generation 0.
	if _, err := notifier.Subscribe(ctx, "future", 0); err != nil {
		t.Fatalf("generation-0 subscription to a never-seen stream should succeed: %v", err)
	}

	if err := notifier.CloseStream(ctx, "test", 1, lognotify.TerminationPurged); err != nil {
		t.Fatalf("unexpected error while closing stream: %v", err)
	}

	if _, err := notifier.Subscribe(ctx, "test", 1); !errors.Is(err, lognotify.ErrStreamRetired) {
		t.Fatalf("gen-1 subscription should fail with ErrStreamRetired, got: %v", err)
	}

	if _, err := notifier.Subscribe(ctx, "test", 0); !errors.Is(err, lognotify.ErrStreamRetired) {
		t.Fatalf("gen-0 subscription to a known purged stream should fail, got: %v", err)
	}

	gen2M, err := notifier.Subscribe(ctx, "test", 2)
	if err != nil {
		t.Fatalf("gen-2 subscription (rebuilt stream) should succeed: %v", err)
	}

	// A later purge of generation 2 must not close the generation-2 mailbox
	// while a previous-generation close stays effective.
	if err := notifier.CloseStream(ctx, "test", 2, lognotify.TerminationPurged); err != nil {
		t.Fatalf("unexpected error while closing rebuilt stream: %v", err)
	}

	select {
	case msg, ok := <-gen2M.ReceiveC():
		if !ok {
			t.Fatal("gen-2 mailbox closed without termination notice")
		}
		if msg.Terminated == nil || msg.Terminated.Generation != 2 {
			t.Fatalf("expected gen-2 termination, got: %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for gen-2 termination notice")
	}
}
