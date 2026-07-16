// Package demux implements the demultiplexer state machine the Client
// session builds on: classification of parsed inbound messages,
// pending-request correlation, follow/list/subscription registries with
// bounded queues, outcome-unknown retirement and abandoned-list drain
// records, and all count/byte accounting. The contract is
// docs/demux.md; on any divergence the document wins.
//
// The machine is a passive, synchronous data structure with no internal
// locking: the session serializes every call under one lock. The single
// reader goroutine is the machine's only caller of Route; control-plane
// calls enter briefly at their linearization points under the same
// lock. No call blocks, waits on a consumer, or runs user code, and
// every effect is returned as data (Effects) for the session to apply
// after releasing the lock. Time is an input: callers supply monotonic
// timestamps and the machine never reads a clock.
//
// The package is generic over the routed payload T and never imports
// the root package; the root instantiates it with immutable messages
// while model tests drive it with trivial payloads.
//
// # Session obligations
//
// Every admitted ticket follows one write resolution and one outcome
// resolution:
//
//   - Write: exactly one of CommitWrite (the action was fully written)
//     or AbortNotSent (zero bytes reached the wire). AbortNotSent
//     releases every reservation and provisional branch immediately and
//     ends the ticket.
//   - Outcome, after CommitWrite: the correlated response completes the
//     ticket through Effects.Complete with Delivered true; client death
//     completes it with Delivered false; or the session calls Abandon
//     when the request context ends without either, converting the
//     reserved retirement slot into a live record and releasing any
//     provisional follow or list branch.
//
// A ticket admitted with a follow or list branch carries one more
// obligation, decided from knowledge the session already holds:
//
//   - Follow: AdoptFollow on the successful path, CloseFollow on every
//     completed-but-failed path (AMI error response, post-response
//     failure, death completion). Abandon and AbortNotSent resolve the
//     provisional branch themselves; CloseFollow after either is a
//     session bug.
//   - List: AdoptList after a Success response. After an Error response
//     the machine has already released the branch and no call is due.
//     CloseList after a death completion. Abandon and AbortNotSent
//     likewise resolve the branch themselves.
//
// Adopted branches and subscriptions are released with Close, which is
// also the acknowledgment that removes a terminal branch's
// bookkeeping: the machine retains a terminal branch — and a clean
// terminal's still-draining queue — until Close.
//
// Invariant violations panic with a stable message; the session's read
// loop converts any read-loop panic into client death with the cause
// preserved.
package demux

// Machine is the demultiplexer state machine. It must be constructed
// with New and accessed under the session's lock; the zero value is not
// usable.
type Machine[T any] struct {
	lim Limits

	// now is the machine's logical clock: the maximum timestamp any
	// call has supplied. It dates drain records created by calls that
	// carry no timestamp of their own (Close, and overflow during
	// Route).
	now int64

	dead       bool
	deadReason Reason

	nextTicket uint64
	nextBranch uint64

	pendByAction map[string]*pending[T]
	pendByTicket map[uint64]*pending[T]

	// publicPending counts admitted, not-yet-completed public actions;
	// the internal keepalive slot is tracked separately.
	publicPending   int
	internalActive  bool
	internalRetHeld bool

	subByID  map[uint64]*subscription[T]
	subOrder []*subscription[T] // active subscriptions in registration order

	folByID     map[uint64]*follow[T]
	folByAction map[string]*follow[T] // routing-active follows only

	listByID     map[uint64]*list[T]
	listByAction map[string]*list[T] // routing-active lists only

	records map[string]*record

	// Aggregates: the sum of live charges, re-verified after every
	// mutating call.
	subBytes  int // queued subscription-family bytes (subscriptions and follows)
	listBytes int // retained list bytes (queued items and stored completion events)
	retHeld   int // admission-held public retirement reservations

	ctr Counters
}

// New returns an empty machine bounded by lim. Like the wire layer, the
// machine trusts its limits: the session validates them once with
// Limits.Validate, and an unvalidated non-positive limit fails closed
// at the first admission or delivery it bounds.
func New[T any](lim Limits) *Machine[T] {
	return &Machine[T]{
		lim:          lim,
		nextTicket:   1,
		nextBranch:   1,
		pendByAction: make(map[string]*pending[T]),
		pendByTicket: make(map[uint64]*pending[T]),
		subByID:      make(map[uint64]*subscription[T]),
		folByID:      make(map[uint64]*follow[T]),
		folByAction:  make(map[string]*follow[T]),
		listByID:     make(map[uint64]*list[T]),
		listByAction: make(map[string]*list[T]),
		records:      make(map[string]*record),
	}
}

// Dead reports whether the machine has been killed, and by what.
func (m *Machine[T]) Dead() (Reason, bool) {
	return m.deadReason, m.dead
}

// Counters returns a snapshot of the machine's discard accounting.
func (m *Machine[T]) Counters() Counters {
	return m.ctr
}

// Kill terminates the machine with a session-supplied cause: every
// uncompleted pending is completed with Delivered false, every active
// branch commits ClientDead and discards its queue, and every
// retirement record is released. A branch already in clean terminal
// drain keeps its queue and drains to end-of-stream. Kill is
// idempotent; a second call returns empty Effects.
func (m *Machine[T]) Kill(cause Reason) Effects[T] {
	if m.dead {
		return Effects[T]{}
	}
	fx := m.kill(cause)
	m.check()
	return fx
}

// fatal kills the machine for an internally detected protocol or
// retirement violation and reports it through Effects.Fatal.
func (m *Machine[T]) fatal(reason Reason, kind Kind) Effects[T] {
	fx := m.kill(reason)
	fx.Fatal = &Fatality{Reason: reason, Kind: kind}
	return fx
}

// kill runs the death cascade. The caller has checked m.dead.
func (m *Machine[T]) kill(cause Reason) Effects[T] {
	m.dead = true
	m.deadReason = cause
	var fx Effects[T]

	// Complete every waiter of a pending that has no committed outcome
	// yet. Resolved obligations (CommitWrite, adoption) stay with the
	// session, so ticket entries persist until the session resolves
	// them.
	for _, p := range m.pendByTicket {
		if p.state == pCompleted {
			continue
		}
		p.state = pCompleted
		p.deadDone = true
		m.releaseRetHold(p)
		delete(m.pendByAction, p.actionID)
		if p.internal {
			m.internalActive = false
		} else {
			m.publicPending--
		}
		fx.Complete = append(fx.Complete, Completion[T]{Ticket: Ticket{p.ticket}, Delivered: false})
		m.maybeReleaseTicket(p)
	}

	for _, s := range m.subOrder {
		m.terminalSub(s, ReasonClientDead, &fx)
	}
	m.subOrder = nil

	for _, f := range m.folByID {
		if f.phase == bActive {
			m.terminalFollow(f, ReasonClientDead, &fx)
		}
	}

	for _, l := range m.listByID {
		if l.phase == lDraining || l.phase == lDead {
			continue
		}
		m.discardList(l)
		if l.retHeld {
			l.retHeld = false
			m.retHeld--
		}
		l.phase = lDead
		l.reason = ReasonClientDead
		delete(m.listByAction, l.actionID)
		fx.wake(l.id)
	}

	clear(m.records)
	m.internalRetHeld = false
	if m.retHeld != 0 {
		violated("retirement reservations survived client death")
	}
	return fx
}

// clock advances the machine's logical clock; supplied timestamps are
// treated as monotonic and never move it backward.
func (m *Machine[T]) clock(now int64) {
	if now > m.now {
		m.now = now
	}
}

// releaseRetHold returns a pending's admission-held retirement
// reservation, public or internal, if it still holds one. List-kind
// reservations live on the list branch instead.
func (m *Machine[T]) releaseRetHold(p *pending[T]) {
	if !p.retHeld {
		return
	}
	p.retHeld = false
	if p.internal {
		m.internalRetHeld = false
	} else {
		m.retHeld--
	}
}

// violated reports a broken machine invariant. The message is stable
// and carries no remote or caller data; the session's read loop
// converts the panic into client death.
func violated(what string) {
	panic("ami/demux: invariant violated: " + what)
}

// check re-verifies the accounting invariants after a mutating call:
// every aggregate equals the sum of its live charges, no counter is
// negative, and every cap holds. Its cost is linear in the number of
// registered branches, not in queued items.
func (m *Machine[T]) check() {
	subSum := 0
	for _, s := range m.subByID {
		if s.q.bytes < 0 {
			violated("negative subscription queue bytes")
		}
		if s.phase == bActive && (s.q.len() > s.caps.Items || s.q.bytes > s.caps.Bytes) {
			violated("subscription queue exceeds its caps")
		}
		subSum += s.q.bytes
	}
	for _, f := range m.folByID {
		if f.q.bytes < 0 {
			violated("negative follow queue bytes")
		}
		if f.q.len() > f.caps.Items || f.q.bytes > f.caps.Bytes {
			violated("follow queue exceeds its caps")
		}
		subSum += f.q.bytes
	}
	if subSum != m.subBytes {
		violated("subscription aggregate diverged from live charges")
	}
	if m.subBytes > m.lim.MaxSubscriptionBytes && m.lim.MaxSubscriptionBytes > 0 {
		violated("subscription aggregate exceeds its cap")
	}

	listSum := 0
	for _, l := range m.listByID {
		if l.q.bytes < 0 {
			violated("negative list queue bytes")
		}
		retained := l.q.bytes + l.completionSize
		if retained > l.caps.Bytes || l.q.len() > l.caps.Items {
			violated("list queue exceeds its caps")
		}
		listSum += retained
	}
	if listSum != m.listBytes {
		violated("list aggregate diverged from live charges")
	}
	if m.listBytes > m.lim.MaxListBytes && m.lim.MaxListBytes > 0 {
		violated("list aggregate exceeds its cap")
	}

	held := 0
	for _, p := range m.pendByTicket {
		if p.retHeld && !p.internal {
			held++
		}
	}
	for _, l := range m.listByID {
		if l.retHeld {
			held++
		}
	}
	if held != m.retHeld {
		violated("retirement holds diverged from live reservations")
	}
	public := 0
	for _, r := range m.records {
		if !r.internal {
			public++
		}
	}
	if m.retHeld+public > m.lim.MaxRetirement && m.lim.MaxRetirement > 0 {
		violated("retirement pool exceeds its cap")
	}

	pub := 0
	for _, p := range m.pendByTicket {
		if !p.internal && p.state != pCompleted {
			pub++
		}
	}
	if pub != m.publicPending {
		violated("pending count diverged from live pendings")
	}

	if m.dead {
		if len(m.records) != 0 || len(m.subOrder) != 0 ||
			len(m.folByAction) != 0 || len(m.listByAction) != 0 ||
			len(m.pendByAction) != 0 {
			violated("routing state survived client death")
		}
	}
}

// fold returns s with ASCII uppercase letters lowered, allocating only
// when a change is needed. Event and completion names function as
// protocol identifiers and are folded once at registration; Envelope
// names arrive pre-folded from the session.
func fold(s string) string {
	lower := func(c byte) bool { return 'A' <= c && c <= 'Z' }
	i := 0
	for i < len(s) && !lower(s[i]) {
		i++
	}
	if i == len(s) {
		return s
	}
	b := []byte(s)
	for ; i < len(b); i++ {
		if lower(b[i]) {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// foldSet folds and collects a declarative name set, enforcing the
// registration bounds. Empty input returns a nil map; what nil means —
// match everything for a subscription matcher and follow selection,
// nothing declared for completion sets — is the lookup site's contract.
func (m *Machine[T]) foldSet(names []string) (map[string]struct{}, error) {
	if len(names) == 0 {
		return nil, nil
	}
	if len(names) > m.lim.MaxMatcherNames {
		return nil, ErrMatcherLimit
	}
	set := make(map[string]struct{}, len(names))
	bytes := 0
	for _, n := range names {
		if n == "" {
			return nil, ErrInvalidOptions
		}
		bytes += len(n)
		if bytes > m.lim.MaxMatcherBytes {
			return nil, ErrMatcherLimit
		}
		set[fold(n)] = struct{}{}
	}
	return set, nil
}
