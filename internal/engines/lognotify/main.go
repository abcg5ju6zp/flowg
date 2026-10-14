package lognotify

import (
	"context"
	"log/slog"

	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/fx"

	"link-society.com/flowg/internal/models"
)

// LogNotifier is the live fan-out bus for ingested log records. Subscribers
// register their interest in a stream generation and receive every record
// routed to it afterwards, which is what powers the live tail in the web UI.
// Purging a generation terminates its watchers definitively and rejects
// subscriptions targeting the retired generation.
type LogNotifier interface {
	// Subscribe registers the caller as a listener on a stream generation and
	// returns a mailbox that receives every subsequent LogMessage for it. The
	// subscription is torn down automatically when ctx is cancelled. It fails
	// with ErrStreamRetired when the generation was already purged; pass 0 to
	// watch a stream name that has never existed.
	Subscribe(ctx context.Context, stream string, generation uint64) (actor.MailboxReceiver[LogMessage], error)
	// Notify broadcasts a freshly ingested record to every current subscriber
	// of the stream; it is a no-op when nobody is listening.
	Notify(ctx context.Context, stream string, logKey string, logRecord models.LogRecord) error
	// CloseStream raises the stream's retired-generation watermark, sends the
	// given termination notice to every subscriber of a generation at or below
	// generation and stops their mailboxes. It blocks until the actor
	// registered the close, so later subscriptions to the old generation are
	// rejected immediately after it returns.
	CloseStream(ctx context.Context, stream string, generation uint64, reason TerminationReason) error
}

type logNotifierImpl struct {
	actor.Actor

	subMbox   actor.MailboxSender[SubscribeMessage]
	logMbox   actor.MailboxSender[LogMessage]
	closeMbox actor.MailboxSender[CloseStreamMessage]
}

var _ LogNotifier = (*logNotifierImpl)(nil)

// NewLogNotifier returns an fx module providing a LogNotifier backed by a
// single actor, logging through the root logger provided to the graph. The
// actor and its three mailboxes (subscriptions, log broadcasts and stream
// closes) are started and stopped with the application lifecycle.
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
		fx.Provide(func(lc fx.Lifecycle) actor.Mailbox[CloseStreamMessage] {
			mbox := actor.NewMailbox[CloseStreamMessage]()

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
			closeMbox actor.Mailbox[CloseStreamMessage],
		) LogNotifier {
			logNotifier := &logNotifierImpl{
				Actor: actor.New(&worker{
					logger: rootLogger.With(slog.String("channel", "lognotify")),
					subscribers: make(
					map[string]map[actor.Mailbox[LogMessage]]subscriber,
				),
					retired:   make(map[string]uint64),
					subMbox:   subMbox,
					logMbox:   logMbox,
					closeMbox: closeMbox,
				}),

				subMbox:   subMbox,
				logMbox:   logMbox,
				closeMbox: closeMbox,
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
// stream generation and blocks until the actor confirms the registration (or
// rejects a retired generation), guaranteeing no record is missed between the
// call and the first delivery. The mailbox delivers everything queued before
// being stopped so a termination notice is never lost.
func (n *logNotifierImpl) Subscribe(
	ctx context.Context,
	stream string,
	generation uint64,
) (actor.MailboxReceiver[LogMessage], error) {
	logM := actor.NewMailbox[LogMessage](actor.OptStopAfterReceivingAll())
	logM.Start()

	readyC := make(chan ReadyResponse, 1)
	msg := SubscribeMessage{
		Stream:     stream,
		Generation: generation,
		SenderM:    logM,
		ReadyC:     readyC,
		DoneC:      ctx.Done(),
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

// CloseStream asks the actor to terminate the stream's retired generation and
// blocks until that is registered, so the purge that follows cannot race a
// subscription to the generation it just closed.
func (n *logNotifierImpl) CloseStream(
	ctx context.Context,
	stream string,
	generation uint64,
	reason TerminationReason,
) error {
	readyC := make(chan ReadyResponse, 1)
	msg := CloseStreamMessage{
		Stream:     stream,
		Generation: generation,
		Reason:     reason,
		ReadyC:     readyC,
	}

	err := n.closeMbox.Send(ctx, msg)
	if err != nil {
		return err
	}

	resp := <-readyC
	return resp.Err
}
