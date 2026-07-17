package ami

import (
	"context"
	"errors"
	"io"
	"iter"
	"sync/atomic"

	"github.com/nakisen/ami/internal/demux"
)

// Consumption-discipline errors: stable rejections for a second or
// concurrent consumer on single-consumer, single-use surfaces.
var (
	errConcurrentConsumer = errors.New("ami: handle is already being consumed")
	errAdapterUsed        = errors.New("ami: single-use adapter already consumed")
)

// A SubOption configures one subscription.
type SubOption func(*subOptions)

type subOptions struct {
	names []string
	items int
}

// MatchEvents restricts the subscription to the named events, matched
// case-insensitively. Without it, the subscription receives every
// unsolicited event.
func MatchEvents(names ...string) SubOption {
	ns := make([]string, len(names))
	copy(ns, names)
	return func(o *subOptions) {
		o.names = append(o.names, ns...)
	}
}

// Buffer overrides the subscription queue's item bound; the byte bound
// stays at Limits.SubscriptionQueueBytes. Size it to the consumer's
// worst pull gap times the matched event rate: overflow closes the
// subscription with ErrLagged rather than dropping events silently.
func Buffer(items int) SubOption {
	return func(o *subOptions) {
		o.items = items
	}
}

// A Subscription is one explicit, bounded event stream: ordinary
// unsolicited events registered through Client.Subscribe, or the
// correlated follow stream a successful Do transferred through
// DoResult. It preserves matching events in observed wire order and is
// single-consumer.
type Subscription struct {
	c *Client
	b *branchState

	busy    atomic.Bool // one Next/All/Consume at a time
	adapted atomic.Bool // All/Consume are single-use
}

// Subscribe validates opts, registers the subscription eagerly — there
// is no library-created delivery gap after it returns — and transfers
// ownership to the caller, who must Close it.
func (c *Client) Subscribe(opts ...SubOption) (*Subscription, error) {
	o := subOptions{items: c.sess.subItems}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.items <= 0 {
		return nil, errors.New("ami: Buffer requires a positive item bound")
	}
	caps := demux.Caps{Items: o.items, Bytes: c.sess.subBytes}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminated {
		return nil, ErrClosed
	}
	id, err := c.machine.Subscribe(demux.Matcher{Events: o.names}, caps)
	if err != nil {
		return nil, subscribeError(err)
	}
	c.diag.info("subscription registered", "names", len(o.names))
	return &Subscription{c: c, b: c.registerBranchLocked(id)}, nil
}

// subscribeError maps a machine registration rejection onto the public
// surface.
func subscribeError(err error) error {
	switch {
	case errors.Is(err, demux.ErrDead):
		return ErrClosed
	case errors.Is(err, demux.ErrSubscriptionLimit):
		return errors.New("ami: subscription limit reached")
	case errors.Is(err, demux.ErrMatcherLimit):
		return errors.New("ami: declared name set exceeds the matcher limits")
	}
	return errors.New("ami: invalid subscription options")
}

// Next blocks for the next event, bounded by ctx. Canceling ctx ends
// only this wait — the subscription stays registered. After a clean
// terminal's queue drains, Next returns io.EOF with a nil Err;
// otherwise a terminal subscription returns its stable first-winner
// error: ErrLagged on overflow, ErrClosed after local Close, or the
// client root cause. Next is single-consumer.
func (s *Subscription) Next(ctx context.Context) (Event, error) {
	if !s.busy.CompareAndSwap(false, true) {
		return Event{}, errConcurrentConsumer
	}
	defer s.busy.Store(false)
	msg, err := s.c.takeBranch(ctx, s.b)
	if err != nil {
		return Event{}, err
	}
	return Event{msg}, nil
}

// All returns a single-use iterator over the subscription. It holds
// the consumer slot for the entire iteration — yields included — so a
// concurrent Next is rejected with the consumer-discipline error
// instead of stealing queued events from between yields. Once
// iteration begins, every exit path — clean end, terminal error, ctx
// end, or an early break — closes the subscription exactly once; if
// iteration never begins, the handle stays caller-owned and must still
// be closed. A clean terminal ends iteration without yielding an
// error; a terminal failure yields exactly one final (zero Event,
// error) pair.
func (s *Subscription) All(ctx context.Context) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		if !s.busy.CompareAndSwap(false, true) {
			yield(Event{}, errConcurrentConsumer)
			return
		}
		if !s.adapted.CompareAndSwap(false, true) {
			s.busy.Store(false)
			yield(Event{}, errAdapterUsed)
			return
		}
		defer s.busy.Store(false)
		defer s.Close()
		for {
			msg, err := s.c.takeBranch(ctx, s.b)
			switch {
			case err == io.EOF:
				return
			case err != nil:
				yield(Event{}, err)
				return
			}
			if !yield(Event{msg}, nil) {
				return
			}
		}
	}
}

// Consume runs handler serially on the calling goroutine for each
// event until ctx ends, the subscription terminates, or handler
// returns a non-nil error, which stops consumption and is returned.
// The handler runs off the read loop and may safely call Do. Consume
// holds the consumer slot for its entire run — handler calls included
// — so a concurrent Next is rejected instead of stealing events. It is
// single-use; once it begins, every exit path closes the subscription
// exactly once. A clean terminal returns nil.
func (s *Subscription) Consume(ctx context.Context, handler func(Event) error) error {
	if !s.busy.CompareAndSwap(false, true) {
		return errConcurrentConsumer
	}
	if !s.adapted.CompareAndSwap(false, true) {
		s.busy.Store(false)
		return errAdapterUsed
	}
	defer s.busy.Store(false)
	defer s.Close()
	for {
		msg, err := s.c.takeBranch(ctx, s.b)
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		}
		if err := handler(Event{msg}); err != nil {
			return err
		}
	}
}

// Done returns a channel that closes at the subscription's first
// terminal result. A clean terminal's queued events remain consumable
// through Next after Done closes.
func (s *Subscription) Done() <-chan struct{} {
	return s.b.done
}

// Err returns the subscription's stable terminal error: nil while
// active and after a clean terminal, ErrLagged after overflow,
// ErrClosed after local Close, or the client root cause.
func (s *Subscription) Err() error {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	return s.b.err
}

// Close unregisters the subscription: immediate, idempotent, and
// local. Queued events are discarded and ErrClosed commits as the
// terminal result unless one already won; an earlier terminal is
// preserved.
func (s *Subscription) Close() error {
	s.c.closeBranch(s.b)
	return nil
}

// takeBranch is the shared bounded consumption core: one dequeue-or-
// park cycle per iteration, linearized against routing through the
// session lock, with wake coalescing through the branch's notify
// channel.
func (c *Client) takeBranch(ctx context.Context, b *branchState) (Message, error) {
	for {
		c.mu.Lock()
		if _, open := c.branches[b.id]; !open {
			// Closed: the machine bookkeeping is gone. A clean terminal
			// that was closed reads as end-of-stream; anything else
			// returns the committed first-winner error.
			err := b.err
			c.mu.Unlock()
			if err == nil {
				return Message{}, io.EOF
			}
			return Message{}, err
		}
		msg, res := c.machine.Take(b.id)
		if res.State == demux.TakeTerminal && !b.terminal {
			c.commitTerminalLocked(b, res.Reason)
		}
		err := b.err
		c.mu.Unlock()

		switch res.State {
		case demux.TakeItem:
			return msg, nil
		case demux.TakeEOF:
			return Message{}, io.EOF
		case demux.TakeTerminal:
			if err == nil {
				err = ErrClosed
			}
			return Message{}, err
		}
		select {
		case <-b.notify:
		case <-ctx.Done():
			return Message{}, context.Cause(ctx)
		}
	}
}

// closeBranch releases a branch: the machine drops its bookkeeping —
// converting a still-streaming list's reserved slot into a bounded
// drain — and ErrClosed commits unless a terminal already won.
func (c *Client) closeBranch(b *branchState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.branches[b.id]; !ok {
		return
	}
	c.resolveDeadLocked(func() { c.machine.Close(b.id, c.now()) })
	delete(c.branches, b.id)
	c.commitTerminalLocked(b, demux.ReasonClosed)
	c.pokeExpiry()
}
