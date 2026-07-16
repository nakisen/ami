package ami

import (
	"context"
	"io"
	"iter"
	"sync/atomic"
)

// A List is one list action's correlated item stream after its initial
// success response: items in observed wire order through a bounded
// queue, the stored completion event after clean completion, and one
// stable first-winner terminal result. It is single-consumer.
//
// Provisional items must not be published as a complete snapshot until
// clean terminal completion: Next has returned io.EOF, or All ended
// without an error.
type List struct {
	c    *Client
	b    *branchState
	resp Response

	busy    atomic.Bool
	adapted atomic.Bool
}

// Response returns the list action's initial success response.
func (l *List) Response() Response {
	return l.resp
}

// Next blocks for the next item event, bounded by ctx. Canceling ctx
// ends only this wait. After clean completion's queue drains, Next
// returns io.EOF with a nil Err; a failed list returns its stable
// *ListError, ErrClosed after local Close, or the client root cause.
func (l *List) Next(ctx context.Context) (Event, error) {
	if !l.busy.CompareAndSwap(false, true) {
		return Event{}, errConcurrentConsumer
	}
	defer l.busy.Store(false)
	msg, err := l.c.takeBranch(ctx, l.b)
	if err != nil {
		return Event{}, err
	}
	return Event{msg}, nil
}

// All returns a single-use iterator over the list's items with the
// same ownership rules as Subscription.All: once iteration begins,
// every exit path closes the list exactly once. A clean completion
// ends iteration without yielding an error; a terminal failure yields
// exactly one final (zero Event, error) pair.
func (l *List) All(ctx context.Context) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		if !l.adapted.CompareAndSwap(false, true) {
			yield(Event{}, errAdapterUsed)
			return
		}
		defer l.Close()
		for {
			ev, err := l.Next(ctx)
			switch {
			case err == io.EOF:
				return
			case err != nil:
				yield(Event{}, err)
				return
			}
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// Completion returns the terminal completion event after clean
// completion and reports whether one is available. The declared count,
// when configured, was already verified before the completion
// committed.
func (l *List) Completion() (Event, bool) {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	if _, open := l.c.branches[l.b.id]; !open {
		return Event{}, false
	}
	msg, ok := l.c.machine.ListCompletion(l.b.id)
	if !ok {
		return Event{}, false
	}
	return Event{msg}, true
}

// Done returns a channel that closes at the list's first terminal
// result. A clean completion's queued items remain consumable through
// Next after Done closes.
func (l *List) Done() <-chan struct{} {
	return l.b.done
}

// Err returns the list's stable terminal error: nil while active and
// after clean completion, a *ListError for cancellation, overflow, or
// a count mismatch, ErrClosed after local Close, or the client root
// cause.
func (l *List) Err() error {
	l.c.mu.Lock()
	defer l.c.mu.Unlock()
	return l.b.err
}

// Close releases the list locally: immediate, idempotent, and local.
// A still-streaming list's remote traffic enters the client's bounded
// drain — never delivered anywhere — until the remote completes or the
// drain expires. Queued items are discarded and ErrClosed commits as
// the terminal result unless one already won.
func (l *List) Close() error {
	l.c.closeBranch(l.b)
	return nil
}
