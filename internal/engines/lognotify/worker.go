package lognotify

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/vladopajic/go-actor/actor"
)

// subscriber is one watcher registration: the generation it subscribed to and
// the channel that tears it down when its request context is canceled.
type subscriber struct {
	generation uint64
	doneC      <-chan struct{}
}

// worker is the single actor that owns the subscriber registry and the
// retired-generation watermarks. Keeping all mutations on one goroutine means
// the registry needs no locking; canceled subscriptions are swept on every
// invocation instead of being mutated by detached goroutines.
type worker struct {
	logger *slog.Logger

	// subscribers maps a stream to every current subscriber mailbox and its
	// subscription metadata.
	subscribers map[string]map[actor.Mailbox[LogMessage]]subscriber
	// retired is the retired-generation high-water mark per stream. A new
	// subscription to a generation at or below it is rejected outright.
	retired map[string]uint64

	subMbox   actor.MailboxReceiver[SubscribeMessage]
	logMbox   actor.MailboxReceiver[LogMessage]
	closeMbox actor.MailboxReceiver[CloseStreamMessage]
}

var _ actor.Worker = (*worker)(nil)

// DoWork handles one subscription, broadcast or stream close per invocation.
func (w *worker) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd

	case msg, ok := <-w.subMbox.ReceiveC():
		if !ok {
			go func() {
				msg.ReadyC <- ReadyResponse{errors.New("mailbox closed")}
				close(msg.ReadyC)
			}()
			return actor.WorkerEnd
		}

		w.sweep()

		// Reject subscriptions to a generation the purge already closed. The
		// watermark only exists for streams that were actually purged (always
		// generation >= 1); a never-created stream has none, so generation 0
		// there still waits for future logs.
		if watermark, retired := w.retired[msg.Stream]; retired && msg.Generation <= watermark {
			w.logger.Debug(
				"rejecting subscription to retired stream generation",
				"stream", msg.Stream,
				"generation", msg.Generation,
				"retired_generation", watermark,
			)

			err := fmt.Errorf(
				"stream %q generation %d is closed (retired generation %d): %w",
				msg.Stream, msg.Generation, watermark, ErrStreamRetired,
			)
			go func() {
				msg.ReadyC <- ReadyResponse{err}
				close(msg.ReadyC)
			}()
			return actor.WorkerContinue
		}

		if _, exists := w.subscribers[msg.Stream]; !exists {
			w.subscribers[msg.Stream] = map[actor.Mailbox[LogMessage]]subscriber{}
		}

		w.subscribers[msg.Stream][msg.SenderM] = subscriber{
			generation: msg.Generation,
			doneC:      msg.DoneC,
		}

		go func() {
			msg.ReadyC <- ReadyResponse{nil}
			close(msg.ReadyC)
		}()

		return actor.WorkerContinue

	case closeMsg, ok := <-w.closeMbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}

		w.sweep()

		// Raise the watermark first so no later subscription can rejoin the
		// retired generation.
		if closeMsg.Generation > w.retired[closeMsg.Stream] {
			w.retired[closeMsg.Stream] = closeMsg.Generation
		}

		termination := LogMessage{
			Stream: closeMsg.Stream,
			Terminated: &StreamTermination{
				Generation: closeMsg.Generation,
				Reason:     closeMsg.Reason,
			},
		}

		for sender, sub := range w.subscribers[closeMsg.Stream] {
			if sub.generation > closeMsg.Generation {
				continue
			}

			if err := sender.Send(ctx, termination); err != nil {
				w.logger.ErrorContext(
					ctx,
					"failed to send stream termination notice",
					"stream", closeMsg.Stream,
					"generation", closeMsg.Generation,
					"error", err.Error(),
				)
			}

			// StopAfterReceivingAll guarantees the termination notice queued
			// above is delivered before the watcher's channel closes.
			sender.Stop()
			delete(w.subscribers[closeMsg.Stream], sender)
		}

		go func() {
			closeMsg.ReadyC <- ReadyResponse{nil}
			close(closeMsg.ReadyC)
		}()

		return actor.WorkerContinue

	case msg, ok := <-w.logMbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}

		w.sweep()

		if msg.Terminated != nil {
			// Termination notices are only produced by the close branch.
			return actor.WorkerContinue
		}

		for sender := range w.subscribers[msg.Stream] {
			if err := sender.Send(ctx, msg); err != nil {
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

// sweep drops subscribers whose done channel fired and stops their mailboxes.
// It runs on the actor goroutine at the start of every message, keeping all
// registry mutation single-threaded.
func (w *worker) sweep() {
	for stream, senders := range w.subscribers {
		for sender, sub := range senders {
			select {
			case <-sub.doneC:
				delete(senders, sender)
				sender.Stop()
			default:
			}
		}

		if len(senders) == 0 {
			delete(w.subscribers, stream)
		}
	}
}
