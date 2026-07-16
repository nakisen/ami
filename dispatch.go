package ami

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nakisen/ami/internal/demux"
)

// errWriteAdmission is the stable cause for an admission that timed
// out on the configured WriteAdmission bound rather than the caller's
// context.
var errWriteAdmission = errors.New("ami: write admission timed out")

// A FollowSpec requests an ActionID-correlated follow subscription
// registered atomically with a Do dispatch, before the first action
// byte is written. The client supplies the ActionID; the spec cannot
// override it.
type FollowSpec struct {
	// EventNames selects the nonterminal correlated events to deliver;
	// empty selects every correlated nonterminal event.
	EventNames []string

	// CompletionEvents declares the terminal event names. Every
	// declared completion is implicitly eligible for delivery even when
	// absent from EventNames: it is charged and enqueued before clean
	// completion commits, and ErrLagged wins if it cannot be. Empty
	// means the follow ends only by explicit Close.
	CompletionEvents []string

	// BufferItems overrides the follow queue's item bound; zero selects
	// Limits.SubscriptionQueueItems.
	BufferItems int
}

// A DoOption configures one Do dispatch.
type DoOption func(*doOptions)

type doOptions struct {
	follow *FollowSpec
}

// WithFollow registers spec's follow subscription atomically with the
// dispatch. Only a successful Do transfers the follow to the caller
// through DoResult; on every non-nil error the client releases the
// provisional branch itself.
func WithFollow(spec FollowSpec) DoOption {
	s := FollowSpec{
		EventNames:       slices.Clone(spec.EventNames),
		CompletionEvents: slices.Clone(spec.CompletionEvents),
		BufferItems:      spec.BufferItems,
	}
	return func(o *doOptions) {
		o.follow = &s
	}
}

// DoResult is a successful dispatch's outcome: the immediate response,
// the client-assigned opaque ActionID, and the adopted follow
// subscription when one was requested. Do returns it only with a nil
// error; no caller-owned resource escapes a failed dispatch.
type DoResult struct {
	Response Response
	ActionID string
	Follow   *Subscription
}

// Do sends one action and waits for its immediate AMI response, both
// bounded by ctx. The error contract preserves whether the server may
// have executed the action: a *RequestError locates the failure and
// reports MayHaveExecuted, and a *ResponseError carries an AMI error
// response. The library never retries an action.
func (c *Client) Do(ctx context.Context, action Action, opts ...DoOption) (DoResult, error) {
	var o doOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	var admit demux.AdmitOptions[Message]
	if o.follow != nil {
		admit.Follow = &demux.FollowOptions{
			Events:      o.follow.EventNames,
			Completions: o.follow.CompletionEvents,
			Caps:        c.followCaps(o.follow.BufferItems),
		}
	}
	id := c.newActionID(demux.KindRequest)
	cpl, err := c.dispatch(ctx, action, id, demux.KindRequest, admit)
	if err != nil {
		return DoResult{}, err
	}

	if !cpl.Delivered {
		c.mu.Lock()
		if o.follow != nil {
			c.resolveDeadLocked(func() { c.machine.CloseFollow(cpl.Ticket) })
		}
		cause := c.causeLocked()
		c.mu.Unlock()
		return DoResult{}, &RequestError{Phase: PhaseResponse, ActionID: id, mayHaveExecuted: true, cause: cause}
	}
	if !responseSuccess(cpl.Response) {
		if o.follow != nil {
			c.mu.Lock()
			c.machine.CloseFollow(cpl.Ticket)
			c.mu.Unlock()
		}
		return DoResult{}, &ResponseError{resp: Response{cpl.Response}}
	}
	res := DoResult{Response: Response{cpl.Response}, ActionID: id}
	if o.follow != nil {
		c.mu.Lock()
		b := c.registerBranchLocked(c.machine.AdoptFollow(cpl.Ticket))
		c.mu.Unlock()
		res.Follow = &Subscription{c: c, b: b}
	}
	return res, nil
}

// A ListSpec declares how a list action completes. Completion
// detection is hybrid: a correlated event carrying EventList: Complete
// always commits clean completion, and CompletionEvents adds the
// terminal names of actions predating the header convention. It is
// declarative data; no user function runs on the read loop.
type ListSpec struct {
	// CompletionEvents declares terminal event names; empty selects the
	// pure EventList-header convention.
	CompletionEvents []string

	// CountFields names response fields that may declare the expected
	// item count, checked in order; the first present field is verified
	// against the observed items. Empty declares no count.
	CountFields []string
}

// StartList dispatches one list action. ctx governs admission, the
// action write, and the initial response only: on initial success,
// ownership transfers through the returned List — which may already be
// cleanly complete — and further consumption is bounded by the List's
// own methods. On every non-nil error no handle escapes; the client
// owns any required bounded drain.
func (c *Client) StartList(ctx context.Context, action Action, spec ListSpec) (*List, error) {
	admit := demux.AdmitOptions[Message]{List: &demux.ListOptions[Message]{
		Completions:   spec.CompletionEvents,
		Caps:          demux.Caps{Items: c.sess.listItems, Bytes: c.sess.listBytes},
		ObservedBytes: c.sess.listObserved,
		Count:         countExtractor(spec.CountFields),
	}}
	id := c.newActionID(demux.KindList)
	cpl, err := c.dispatch(ctx, action, id, demux.KindList, admit)
	if err != nil {
		return nil, err
	}

	if !cpl.Delivered {
		c.mu.Lock()
		c.resolveDeadLocked(func() { c.machine.CloseList(cpl.Ticket) })
		cause := c.causeLocked()
		c.mu.Unlock()
		return nil, &RequestError{Phase: PhaseResponse, ActionID: id, mayHaveExecuted: true, cause: cause}
	}
	if !responseSuccess(cpl.Response) {
		// The machine released the list branch with the error response;
		// no resolution call is due.
		return nil, &ResponseError{resp: Response{cpl.Response}}
	}
	c.mu.Lock()
	b := c.registerBranchLocked(c.machine.AdoptList(cpl.Ticket))
	c.mu.Unlock()
	return &List{c: c, b: b, resp: Response{cpl.Response}}, nil
}

// dispatch runs the shared action pipeline: writer admission, atomic
// registration before the first byte, the bounded write with its
// resolution, and the response wait linearized against cancellation.
// It returns a completion — delivered or death — with every other
// obligation already resolved, or a final public error.
func (c *Client) dispatch(ctx context.Context, action Action, id string, kind demux.Kind, admit demux.AdmitOptions[Message]) (demux.Completion[Message], error) {
	var zero demux.Completion[Message]
	if err := c.admitWriter(ctx); err != nil {
		return zero, err
	}

	c.mu.Lock()
	if c.terminated {
		c.mu.Unlock()
		c.releaseWriter()
		return zero, ErrClosed
	}
	tkt, err := c.machine.Admit(id, kind, admit)
	if err != nil {
		c.mu.Unlock()
		c.releaseWriter()
		return zero, c.admitError(err, id)
	}
	w := make(chan demux.Completion[Message], 1)
	c.waiters[tkt] = w
	c.mu.Unlock()

	if err := c.writeAdmitted(ctx, action, id, tkt, w, admit); err != nil {
		return zero, err
	}
	return c.await(ctx, id, tkt, w)
}

// writeAdmitted performs the bounded socket write and resolves the
// ticket's write obligation, releasing the writer before waiting on
// anything else. A non-nil error is final: every ticket obligation has
// been resolved and any connection failure has already committed the
// client's root cause.
func (c *Client) writeAdmitted(ctx context.Context, action Action, id string, tkt demux.Ticket, w chan demux.Completion[Message], admit demux.AdmitOptions[Message]) error {
	wctx, cancel := context.WithTimeout(ctx, c.sess.writeAttempt)
	n, err := c.conn.writeAction(wctx, action, id)
	cancel()
	c.releaseWriter()
	if err == nil {
		c.mu.Lock()
		c.machine.CommitWrite(tkt)
		c.mu.Unlock()
		return nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// Cleanly abandoned: provably zero bytes, connection usable.
		if c.resolveNotSentLocked(tkt, w) {
			return &RequestError{Phase: PhaseWrite, ActionID: id, cause: err}
		}
		return nil // a completion won the race; the caller consumes it
	}

	// The connection is closed. Zero written bytes is still definitely
	// not sent; any written byte leaves the outcome unknown.
	if n == 0 {
		raced := !c.resolveNotSentLocked(tkt, w)
		c.die(err)
		if raced {
			return nil
		}
		return &RequestError{Phase: PhaseWrite, ActionID: id, cause: err}
	}

	// Partial write: the client dies with the transport cause, and the
	// ticket's remaining obligations resolve as tolerated post-death
	// bookkeeping — the write resolution and the provisional branch.
	c.die(err)
	c.mu.Lock()
	delete(c.waiters, tkt)
	c.resolveDeadLocked(func() { c.machine.CommitWrite(tkt) })
	if admit.Follow != nil {
		c.resolveDeadLocked(func() { c.machine.CloseFollow(tkt) })
	}
	if admit.List != nil {
		c.resolveDeadLocked(func() { c.machine.CloseList(tkt) })
	}
	c.mu.Unlock()
	return &RequestError{Phase: PhaseWrite, ActionID: id, mayHaveExecuted: true, cause: err}
}

// resolveNotSentLocked resolves a failed write as definitely-not-sent,
// releasing every reservation and provisional branch. It reports false
// when a completion won the race instead — a concurrent death, or a
// server so broken it answered an unsent action — in which case the
// write resolution is bookkeeping only and the completion, left in the
// waiter channel, is the committed outcome.
func (c *Client) resolveNotSentLocked(tkt demux.Ticket, w chan demux.Completion[Message]) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case cpl := <-w:
		c.resolveDeadLocked(func() { c.machine.CommitWrite(tkt) })
		w <- cpl
		return false
	default:
	}
	delete(c.waiters, tkt)
	c.machine.AbortNotSent(tkt)
	return true
}

// await blocks for the request's outcome. A completion and a context
// end race through the session lock with one linearized winner: a
// committed response is never replaced by a concurrent cancellation.
// On the context path the request is abandoned — the reserved slot
// becomes a live retirement record and any provisional branch is
// released — and the outcome is unknown.
func (c *Client) await(ctx context.Context, id string, tkt demux.Ticket, w chan demux.Completion[Message]) (demux.Completion[Message], error) {
	select {
	case cpl := <-w:
		return cpl, nil
	case <-ctx.Done():
	}
	c.mu.Lock()
	select {
	case cpl := <-w:
		c.mu.Unlock()
		return cpl, nil
	default:
	}
	delete(c.waiters, tkt)
	if !c.terminated {
		c.machine.Abandon(tkt, c.now())
		c.mu.Unlock()
		c.pokeExpiry()
	} else {
		// Unreachable in practice: termination completes every waiter
		// before releasing the lock. Kept as the safe default.
		c.mu.Unlock()
	}
	c.diag.info("request abandoned", "phase", "awaiting response")
	return demux.Completion[Message]{}, &RequestError{
		Phase:           PhaseResponse,
		ActionID:        id,
		mayHaveExecuted: true,
		cause:           context.Cause(ctx),
	}
}

// admitWriter acquires write ownership, bounded by the caller's
// context, the client's lifetime, and the configured WriteAdmission.
// Failure is a clean definitely-not-sent.
func (c *Client) admitWriter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return &RequestError{Phase: PhaseAdmission, cause: context.Cause(ctx)}
	}
	timer := time.NewTimer(c.sess.writeAdmission)
	defer timer.Stop()
	select {
	case c.writeSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return &RequestError{Phase: PhaseAdmission, cause: context.Cause(ctx)}
	case <-c.ctx.Done():
		return ErrClosed
	case <-timer.C:
		return &RequestError{Phase: PhaseAdmission, cause: errWriteAdmission}
	}
}

// releaseWriter returns write ownership.
func (c *Client) releaseWriter() {
	<-c.writeSem
}

// admitError maps a machine admission rejection onto the public
// surface. Every rejection is definitely-not-sent.
func (c *Client) admitError(err error, id string) error {
	switch {
	case errors.Is(err, demux.ErrDead):
		return ErrClosed
	case errors.Is(err, demux.ErrPendingLimit):
		return &RequestError{Phase: PhaseAdmission, ActionID: id, cause: errors.New("ami: pending limit reached")}
	case errors.Is(err, demux.ErrRetirementLimit):
		return &RequestError{Phase: PhaseAdmission, ActionID: id, cause: errors.New("ami: retirement capacity exhausted")}
	case errors.Is(err, demux.ErrListLimit):
		return &RequestError{Phase: PhaseAdmission, ActionID: id, cause: errors.New("ami: list limit reached")}
	case errors.Is(err, demux.ErrMatcherLimit):
		return errors.New("ami: declared name set exceeds the matcher limits")
	}
	return errors.New("ami: invalid dispatch options")
}

// resolveDeadLocked runs post-death machine bookkeeping under the
// already-held session lock, tolerating a machine wrecked by the panic
// that killed the client: the client is already terminal with its real
// cause, and unresolved bookkeeping on a dead machine has no effect.
func (c *Client) resolveDeadLocked(f func()) {
	defer func() { _ = recover() }()
	f()
}

// causeLocked returns the committed root cause under the session lock,
// defaulting to ErrClosed.
func (c *Client) causeLocked() error {
	if c.cause != nil {
		return c.cause
	}
	return ErrClosed
}

// followCaps builds a follow branch's queue caps from the session
// defaults and the spec's item override.
func (c *Client) followCaps(items int) demux.Caps {
	caps := demux.Caps{Items: c.sess.subItems, Bytes: c.sess.subBytes}
	if items > 0 {
		caps.Items = items
	}
	return caps
}

// countExtractor builds the pure, bounded count function a ListSpec
// declares: the first present field wins, and an unparseable value
// counts as absent. It runs during Route and touches nothing but the
// message.
func countExtractor(fields []string) func(Message) (int64, bool) {
	if len(fields) == 0 {
		return nil
	}
	fs := slices.Clone(fields)
	return func(m Message) (int64, bool) {
		for _, f := range fs {
			v, ok := m.Lookup(f)
			if !ok {
				continue
			}
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				return 0, false
			}
			return n, true
		}
		return 0, false
	}
}
