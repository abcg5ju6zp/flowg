package lognotify_test

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"link-society.com/flowg/internal/engines/lognotify"
	"link-society.com/flowg/internal/models"
)

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

// eventually polls condition until it holds or the deadline expires. Control
// messages (close/reopen) have no acknowledgement, so effects are observed by
// polling.
func eventually(t *testing.T, condition func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %s", what)
}

// Closing a stream delivers a definite termination event to existing watchers
// and rejects new subscriptions with ErrStreamRetired.
func TestCloseStreamTerminatesWatchersAndRejectsSubscriptions(t *testing.T) {
	ctx := t.Context()
	notifier := newTestNotifier(t)
	const stream = "purged"

	logM, err := notifier.Subscribe(ctx, stream)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := notifier.CloseStream(ctx, stream, "stream purged"); err != nil {
		t.Fatalf("close stream: %v", err)
	}

	select {
	case msg, ok := <-logM.ReceiveC():
		if !ok {
			t.Fatalf("subscription channel closed without termination message")
		}
		if !msg.Terminated {
			t.Fatalf("expected terminated message, got %+v", msg)
		}
		if msg.Reason != "stream purged" {
			t.Fatalf("unexpected termination reason: %q", msg.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("watcher did not receive a termination event")
	}

	eventually(t, func() bool {
		m, err := notifier.Subscribe(ctx, stream)
		if err == nil {
			_ = m
			return false
		}
		return errors.Is(err, lognotify.ErrStreamRetired)
	}, "subscription rejection with ErrStreamRetired")
}

// ReopenStream clears the retired state, so a same-named recreated stream can
// be subscribed and notified again.
func TestReopenStreamAllowsResubscription(t *testing.T) {
	ctx := t.Context()
	notifier := newTestNotifier(t)
	const stream = "recreated"

	if err := notifier.CloseStream(ctx, stream, "stream purged"); err != nil {
		t.Fatalf("close: %v", err)
	}

	eventually(t, func() bool {
		_, err := notifier.Subscribe(ctx, stream)
		return errors.Is(err, lognotify.ErrStreamRetired)
	}, "retired state")

	if err := notifier.ReopenStream(ctx, stream); err != nil {
		t.Fatalf("reopen: %v", err)
	}

	eventually(t, func() bool {
		m, err := notifier.Subscribe(ctx, stream)
		if err != nil {
			return false
		}
		_ = m
		return true
	}, "subscription after reopen")
}

// A fresh Notify proves the name was recreated and implicitly reopens it:
// subscriptions succeed without an explicit ReopenStream call.
func TestNotifyReopensRetiredStream(t *testing.T) {
	ctx := t.Context()
	notifier := newTestNotifier(t)
	const stream = "recreated-by-write"

	if err := notifier.CloseStream(ctx, stream, "stream purged"); err != nil {
		t.Fatalf("close: %v", err)
	}

	eventually(t, func() bool {
		_, err := notifier.Subscribe(ctx, stream)
		return errors.Is(err, lognotify.ErrStreamRetired)
	}, "retired state")

	// A live record can only arrive once storage accepted a write into a
	// recreated same-named stream.
	if err := notifier.Notify(ctx, stream, "key", *models.NewLogRecord(map[string]string{})); err != nil {
		t.Fatalf("notify: %v", err)
	}

	eventually(t, func() bool {
		m, err := notifier.Subscribe(ctx, stream)
		if err != nil {
			return false
		}

		// The post-close subscriber must not receive the reopen notification
		// that was broadcast before it subscribed.
		select {
		case unexpected := <-m.ReceiveC():
			t.Errorf("new subscriber received an unexpected message: %+v", unexpected)
			return false
		default:
		}

		return true
	}, "subscription after implicit reopen")
}
