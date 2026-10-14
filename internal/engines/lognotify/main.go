package lognotify

import (
	"context"
	"log/slog"

	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/fx"

	"link-society.com/flowg/internal/models"
)

// LogNotifier is the live fan-out bus for ingested log records. Subscribers
// register their interest in a stream and receive every record routed to it
// afterwards, which is what powers the live tail in the web UI.
type LogNotifier interface {
	// Subscribe registers the caller as a listener on a stream and returns a
	// mailbox that receives every subsequent LogMessage for it. The subscription
	// is torn down automatically when ctx is cancelled. Subscribing to a
	// stream closed by an in-progress purge fails with ErrStreamRetired.
	Subscribe(ctx context.Context, stream string) (actor.MailboxReceiver[LogMessage], error)
	// Notify broadcasts a freshly ingested record to every current subscriber of
	// the stream; it is a no-op when nobody is listening. It also clears the
	// stream's retired state, since a record can only be notified once a
	// same-named stream exists again.
	Notify(ctx context.Context, stream string, logKey string, logRecord models.LogRecord) error
	// CloseStream retires a stream: existing subscribers receive a final
	// terminated message carrying reason and new subscriptions are rejected
	// until ReopenStream (or a new Notify) recreates the name. Used by the
	// purge generation protocol.
	CloseStream(ctx context.Context, stream, reason string) error
	// ReopenStream clears the retired state of a stream after a same-named
	// stream has been recreated.
	ReopenStream(ctx context.Context, stream string) error
}

type logNotifierImpl struct {
	actor.Actor

	subMbox     actor.MailboxSender[SubscribeMessage]
	logMbox     actor.MailboxSender[LogMessage]
	controlMbox actor.MailboxSender[StreamControlMessage]
}

var _ LogNotifier = (*logNotifierImpl)(nil)

// NewLogNotifier returns an fx module providing a LogNotifier backed by a single
// actor, logging through the root logger provided to the graph. The actor and
// its two mailboxes (subscriptions and log broadcasts) are started and stopped
// with the application lifecycle.
func NewLogNotifier() fx.Option {
	return fx.Module(
		"lognotifier",
		fx.Provide(func(lc fx.Lifecycle) actor.Mailbox[SubscribeMessage] {
			mbox := actor.NewMailbox[SubscribeMessage]()

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					mbox.Start()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					mbox.Stop()
					return nil
				},
			})

			return mbox
		}),
		fx.Provide(func(lc fx.Lifecycle) actor.Mailbox[LogMessage] {
			mbox := actor.NewMailbox[LogMessage]()

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					mbox.Start()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					mbox.Stop()
					return nil
				},
			})

			return mbox
		}),
		fx.Provide(func(lc fx.Lifecycle) actor.Mailbox[StreamControlMessage] {
			mbox := actor.NewMailbox[StreamControlMessage]()

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					mbox.Start()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					mbox.Stop()
					return nil
				},
			})

			return mbox
		}),
		fx.Provide(func(
			lc fx.Lifecycle,
			rootLogger *slog.Logger,
			subMbox actor.Mailbox[SubscribeMessage],
			logMbox actor.Mailbox[LogMessage],
			controlMbox actor.Mailbox[StreamControlMessage],
		) LogNotifier {
			logNotifier := &logNotifierImpl{
				Actor: actor.New(&worker{
					logger:      rootLogger.With(slog.String("channel", "lognotify")),
					subscribers: make(map[string]subscriberSet),
					closed:      make(map[string]string),
					subMbox:     subMbox,
					logMbox:     logMbox,
					controlMbox: controlMbox,
				}),

				subMbox:     subMbox,
				logMbox:     logMbox,
				controlMbox: controlMbox,
			}

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logNotifier.Start()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					logNotifier.Stop()
					return nil
				},
			})

			return logNotifier
		}),
	)
}

// Subscribe asks the actor to register a new per-subscriber mailbox for the
// stream and blocks until the actor confirms the registration (or fails),
// guaranteeing no record is missed between the call and the first delivery.
func (n *logNotifierImpl) Subscribe(ctx context.Context, stream string) (actor.MailboxReceiver[LogMessage], error) {
	logM := actor.NewMailbox[LogMessage]()
	logM.Start()

	readyC := make(chan ReadyResponse, 1)
	msg := SubscribeMessage{
		Stream:  stream,
		SenderM: logM,
		ReadyC:  readyC,
		DoneC:   ctx.Done(),
	}

	err := n.subMbox.Send(ctx, msg)
	if err != nil {
		logM.Stop()
		return nil, err
	}

	resp := <-readyC
	if resp.Err != nil {
		logM.Stop()
		return nil, resp.Err
	}

	return logM, nil
}

// Notify hands a record to the actor for broadcasting; it returns as soon as the
// message is queued, without waiting for delivery to subscribers.
func (n *logNotifierImpl) Notify(ctx context.Context, stream string, logKey string, logRecord models.LogRecord) error {
	msg := LogMessage{
		Stream:    stream,
		LogKey:    logKey,
		LogRecord: logRecord,
	}
	return n.logMbox.Send(ctx, msg)
}

// CloseStream asks the actor to retire a stream, blocking until the control
// message is queued: existing subscribers then receive a final terminated
// message and new subscriptions are rejected with ErrStreamRetired.
func (n *logNotifierImpl) CloseStream(ctx context.Context, stream, reason string) error {
	return n.controlMbox.Send(ctx, StreamControlMessage{
		Stream: stream,
		Reason: reason,
		Close:  true,
	})
}

// ReopenStream clears the retired state of a stream after a same-named stream
// has been recreated, so subscriptions succeed again.
func (n *logNotifierImpl) ReopenStream(ctx context.Context, stream string) error {
	return n.controlMbox.Send(ctx, StreamControlMessage{
		Stream: stream,
		Close:  false,
	})
}
