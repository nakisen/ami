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
	cpl, release, err := c.dispatch(ctx, action, id, demux.KindRequest, admit)
	// Released after every machine touch below — follow adoption or
	// release included — so Done cannot close in the middle of them.
	defer release()
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

	// CountFields names completion-event fields that may declare the
	// expected item count, checked in order; the first present field is
	// verified against the observed items, and a present-but-unusable
	// value fails the list with a malformed-count *ListError instead of
	// silently skipping the declared check. The declaration is bounded —
	// at most 16 names totaling at most 1 KiB, validated before
	// dispatch — because the extractor runs on the read loop. Empty
	// declares no count.
	CountFields []string
}

// ListSpec.CountFields bounds: the extractor scans these names against
// every completion event on the read loop.
const (
	maxCountFields    = 16
	maxCountFieldSize = 1 << 10
)

// StartList dispatches one list action. ctx governs admission, the
// action write, and the initial response only: on initial success,
// ownership transfers through the returned List — which may already be
// terminal: cleanly complete, cancelled, or failed before the response
// arrived, with the typed result observed through Err, Next, and Done —
// and further consumption is bounded by the List's own methods. On
// every non-nil error no handle escapes; the client owns any required
// bounded drain.
func (c *Client) StartList(ctx context.Context, action Action, spec ListSpec) (*List, error) {
	if len(spec.CountFields) > maxCountFields {
		return nil, errors.New("ami: ListSpec.CountFields exceeds 16 names")
	}
	total := 0
	for _, f := range spec.CountFields {
		total += len(f)
	}
	if total > maxCountFieldSize {
		return nil, errors.New("ami: ListSpec.CountFields exceeds 1 KiB of names")
	}
	admit := demux.AdmitOptions[Message]{List: &demux.ListOptions[Message]{
		Completions:   spec.CompletionEvents,
		Caps:          demux.Caps{Items: c.sess.listItems, Bytes: c.sess.listBytes},
		ObservedBytes: c.sess.listObserved,
		Count:         countExtractor(spec.CountFields),
	}}
	id := c.newActionID(demux.KindList)
	cpl, release, err := c.dispatch(ctx, action, id, demux.KindList, admit)
	// Released after every machine touch below — list adoption or
	// release included — so Done cannot close in the middle of them.
	defer release()
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
// obligation already resolved, or a final public error. The returned
// release function is never nil: once the caller's machine bookkeeping
// for this dispatch is complete, calling it drops the in-flight hold
// that keeps Done from closing early.
func (c *Client) dispatch(ctx context.Context, action Action, id string, kind demux.Kind, admit demux.AdmitOptions[Message]) (demux.Completion[Message], func(), error) {
	var zero demux.Completion[Message]
	noop := func() {}
	if err := c.admitWriter(ctx); err != nil {
		return zero, noop, err
	}

	c.mu.Lock()
	if c.terminated {
		c.mu.Unlock()
		c.releaseWriter()
		return zero, noop, ErrClosed
	}
	tkt, err := c.machine.Admit(id, kind, admit)
	if err != nil {
		c.mu.Unlock()
		c.releaseWriter()
		return zero, noop, c.admitError(err, id)
	}
	// Taken under the lock, atomically with the liveness check: from
	// here to release, Done waits for this dispatch's bookkeeping.
	c.inflight.Add(1)
	w := make(chan demux.Completion[Message], 1)
	c.waiters[tkt] = w
	c.mu.Unlock()

	release := c.inflight.Done
	if err := c.writeAdmitted(ctx, action, id, tkt, w, admit); err != nil {
		return zero, release, err
	}
	cpl, err := c.await(ctx, id, tkt, w)
	return cpl, release, err
}

// writeAdmitted performs the bounded socket write and resolves the
// ticket's write obligation. Successful writes release ownership after
// their write commitment; failed writes retain it until every ticket
// obligation and any death caused by this write have committed. A
// non-nil error is final: every ticket obligation has been resolved,
// and a failure of this write has already committed the client's root
// cause. A connection found already closed is the exception — its root
// cause belongs to the terminal path that closed it and may commit
// only after this return.
func (c *Client) writeAdmitted(ctx context.Context, action Action, id string, tkt demux.Ticket, w chan demux.Completion[Message], admit demux.AdmitOptions[Message]) error {
	wctx, cancel := context.WithTimeout(ctx, c.sess.writeAttempt)
	disposition, err := c.conn.writeAction(wctx, action, id)
	cancel()
	defer c.releaseWriter()
	if disposition == writeComplete {
		c.mu.Lock()
		c.machine.CommitWrite(tkt)
		c.mu.Unlock()
		return nil
	}

	if disposition != writeOutcomeUnknown {
		// Only the Conn's private disposition establishes that no byte
		// reached the transport. Error identity is deliberately ignored:
		// a custom transport can return any error after transferring data.
		cause := err
		if fatal := c.resolveNotSent(tkt, w, admit); fatal != nil {
			// A response to a provably unsent action is a correlation
			// contradiction, never a successful user outcome. resolveNotSent
			// commits client death atomically with detecting it.
			cause = fatal
		} else if disposition == writeNotSent {
			// A zero-byte transport failure keeps the action retry-safe but
			// the poisoned connection still terminates the client.
			c.die(err)
		}
		return &RequestError{Phase: PhaseWrite, ActionID: id, cause: cause}
	}

	// A write that transferred any bytes dies with the transport cause,
	// regardless of what that error happens to match. The action outcome
	// is unknown, and the ticket's remaining obligations resolve as
	// tolerated post-death bookkeeping.
	//
	// The client dies with the transport cause, and the
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

// resolveNotSent resolves a failed write as definitely-not-sent,
// releasing every reservation and provisional branch. A raced death
// completion does not flip the outcome. A delivered response is instead
// returned as a fatal correlation contradiction: a server cannot answer
// an action whose first byte never reached the transport.
func (c *Client) resolveNotSent(tkt demux.Ticket, w chan demux.Completion[Message], admit demux.AdmitOptions[Message]) error {
	c.mu.Lock()
	select {
	case cpl := <-w:
		if !cpl.Delivered {
			// AbortNotSent is explicitly legal after a death completion and
			// releases its provisional branch bookkeeping as one operation.
			c.resolveDeadLocked(func() { c.machine.AbortNotSent(tkt) })
			c.mu.Unlock()
			return nil
		}

		// Route already committed the contradictory response. Resolve the
		// trailing write obligation and dispose of every provisional branch
		// before committing the protocol cause below.
		c.machine.CommitWrite(tkt)
		if admit.Follow != nil {
			c.machine.CloseFollow(tkt)
		}
		if admit.List != nil && responseSuccess(cpl.Response) {
			id := c.machine.AdoptList(tkt)
			c.machine.Close(id, c.now())
		}

		// Commit the protocol fatality under the same lock that observed
		// the impossible response. No newly admitted dispatch can slip
		// between detection and the terminal state.
		fx := c.machine.Kill(demux.ReasonResponseUnmatched)
		fx.Fatal = &demux.Fatality{Reason: demux.ReasonResponseUnmatched}
		c.applyLockedFx(fx)
		fatal := c.causeLocked()
		c.mu.Unlock()
		c.finishTeardown()
		return fatal
	default:
	}
	delete(c.waiters, tkt)
	c.machine.AbortNotSent(tkt)
	c.mu.Unlock()
	return nil
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
// declares: the first present field wins. A present value that cannot
// be used — empty, non-numeric, negative, or out of range — is
// malformed rather than absent, so the integrity check the caller
// declared can never be skipped silently. It runs during Route and
// touches nothing but the message.
func countExtractor(fields []string) func(Message) (int64, demux.CountVerdict) {
	if len(fields) == 0 {
		return nil
	}
	fs := slices.Clone(fields)
	return func(m Message) (int64, demux.CountVerdict) {
		for _, f := range fs {
			v, ok := m.Lookup(f)
			if !ok {
				continue
			}
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil || n < 0 {
				return 0, demux.CountMalformed
			}
			return n, demux.CountDeclared
		}
		return 0, demux.CountAbsent
	}
}
