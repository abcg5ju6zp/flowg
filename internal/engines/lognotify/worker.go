package lognotify

import (
	"errors"
	"log/slog"

	"github.com/vladopajic/go-actor/actor"
)

type subscriberSet map[actor.MailboxSender[LogMessage]]struct{}

// worker is the single actor that owns the subscriber registry. Keeping all
// mutations on one goroutine means the per-stream subscriber sets need no
// locking.
//
// Besides live subscribers it tracks closed streams: while a purge
// generation retires a stream, new subscriptions on its name fail with
// ErrStreamRetired and existing subscribers receive a terminated message.
// The closed state is cleared when the stream is explicitly reopened or when
// a record is notified again (which proves a same-named stream was
// recreated).
type worker struct {
	logger      *slog.Logger
	subscribers map[string]subscriberSet
	closed      map[string]string
	subMbox     actor.MailboxReceiver[SubscribeMessage]
	logMbox     actor.MailboxReceiver[LogMessage]
	controlMbox actor.MailboxReceiver[StreamControlMessage]
}

var _ actor.Worker = (*worker)(nil)

// DoWork handles one subscription, broadcast or lifecycle control message per
// invocation.
func (w *worker) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd

	case msg, ok := <-w.controlMbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}

		if msg.Close {
			w.closeStream(ctx, msg.Stream, msg.Reason)
		} else {
			delete(w.closed, msg.Stream)
		}

		return actor.WorkerContinue

	case msg, ok := <-w.subMbox.ReceiveC():
		if !ok {
			go func() {
				msg.ReadyC <- ReadyResponse{errors.New("mailbox closed")}
				close(msg.ReadyC)
			}()
			return actor.WorkerEnd
		}

		if reason, retired := w.closed[msg.Stream]; retired {
			// A purge generation owns this name: reject the subscription with
			// a definite reason instead of buffering events for a stream
			// being deleted.
			go func() {
				msg.ReadyC <- ReadyResponse{&StreamRetiredError{Reason: reason}}
				close(msg.ReadyC)
			}()
			return actor.WorkerContinue
		}

		if _, exists := w.subscribers[msg.Stream]; !exists {
			w.subscribers[msg.Stream] = make(subscriberSet)
		}

		w.subscribers[msg.Stream][msg.SenderM] = struct{}{}

		go func() {
			<-msg.DoneC
			delete(w.subscribers[msg.Stream], msg.SenderM)
			msg.SenderM.Stop()
		}()

		go func() {
			msg.ReadyC <- ReadyResponse{nil}
			close(msg.ReadyC)
		}()

		return actor.WorkerContinue

	case msg, ok := <-w.logMbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}

		if msg.Terminated {
			// Defensive: a terminated message is only produced internally by
			// closeStream, but never fan it out as a regular record.
			return actor.WorkerContinue
		}

		// A live record for the name proves a same-named stream was
		// recreated: clear any retired state before delivering.
		delete(w.closed, msg.Stream)

		if _, exists := w.subscribers[msg.Stream]; !exists {
			w.subscribers[msg.Stream] = make(subscriberSet)
		}

		for sub := range w.subscribers[msg.Stream] {
			if err := sub.Send(ctx, msg); err != nil {
				w.logger.ErrorContext(
					ctx,
					"failed to send log message",
					"stream", msg.Stream,
					"error", err.Error(),
				)
			}
		}

		return actor.WorkerContinue
	}
}

// closeStream retires a stream: it records the definite termination reason
// for future subscription attempts, delivers one final terminated message to
// every current subscriber and drops the registry for the name.
//
// Subscriber mailboxes are not stopped here: the SSE handlers return right
// after reading the terminated message and their own teardown (wired to their
// request context) stops them. This also guarantees the terminal message
// cannot be lost to a stop-before-delivery race.
func (w *worker) closeStream(ctx actor.Context, stream, reason string) {
	if reason == "" {
		reason = "stream retired"
	}

	w.closed[stream] = reason

	terminal := LogMessage{
		Stream:     stream,
		Terminated: true,
		Reason:     reason,
	}

	for sub := range w.subscribers[stream] {
		if err := sub.Send(ctx, terminal); err != nil {
			w.logger.ErrorContext(
				ctx,
				"failed to send stream termination message",
				"stream", stream,
				"error", err.Error(),
			)
		}
	}

	delete(w.subscribers, stream)
}
