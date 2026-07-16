package demux

// pstate is a pending action's outcome progress.
type pstate uint8

const (
	pReserved  pstate = 1 + iota // admitted, write not yet resolved
	pInFlight                    // fully written, awaiting its outcome
	pCompleted                   // outcome committed: response, death, abort, or abandonment
)

// pending is one admitted action, the internal keepalive Ping included.
type pending[T any] struct {
	ticket   uint64
	actionID string
	kind     Kind
	internal bool

	state     pstate
	committed bool // CommitWrite or AbortNotSent resolved the write
	deadDone  bool // completed by client death rather than a response

	// retHeld marks the admission-held retirement reservation of a
	// request-kind pending. A list-kind admission's reservation lives
	// on the list branch, which outlives the pending.
	retHeld bool

	fol     *follow[T]
	folOpen bool // awaiting AdoptFollow or CloseFollow
	lst     *list[T]
	lstOpen bool // awaiting AdoptList or CloseList
}

// Admit reserves one pending slot and the retirement/drain record the
// action could ever need, and registers any follow or list branch
// routing-active, all before the first action byte can be written. A
// non-nil error means the action was rejected definitely-not-sent;
// live records are never evicted to admit new work.
func (m *Machine[T]) Admit(actionID string, kind Kind, o AdmitOptions[T]) (Ticket, error) {
	if m.dead {
		return Ticket{}, ErrDead
	}
	if actionID == "" || (kind != KindRequest && kind != KindList) {
		return Ticket{}, ErrInvalidOptions
	}
	if (kind == KindList) != (o.List != nil) || (kind == KindList && o.Follow != nil) {
		return Ticket{}, ErrInvalidOptions
	}
	if o.Internal && (o.Follow != nil || o.List != nil) {
		return Ticket{}, ErrInvalidOptions
	}

	// Validate branch options before consuming any capacity.
	var folEvents, folCompletions, lstCompletions map[string]struct{}
	var err error
	if o.Follow != nil {
		if !o.Follow.Caps.valid() {
			return Ticket{}, ErrInvalidOptions
		}
		if folEvents, err = m.foldSet(o.Follow.Events); err != nil {
			return Ticket{}, err
		}
		if folCompletions, err = m.foldSet(o.Follow.Completions); err != nil {
			return Ticket{}, err
		}
	}
	if o.List != nil {
		if !o.List.Caps.valid() || o.List.ObservedBytes <= 0 {
			return Ticket{}, ErrInvalidOptions
		}
		if lstCompletions, err = m.foldSet(o.List.Completions); err != nil {
			return Ticket{}, err
		}
	}

	if o.Internal {
		if m.internalActive || m.internalRetHeld {
			return Ticket{}, ErrInternalBusy
		}
	} else {
		if m.publicPending >= m.lim.MaxPending {
			return Ticket{}, ErrPendingLimit
		}
		if m.retHeld+m.publicRecords() >= m.lim.MaxRetirement {
			return Ticket{}, ErrRetirementLimit
		}
		if kind == KindList && len(m.listByAction) >= m.lim.MaxLists {
			return Ticket{}, ErrListLimit
		}
	}

	if m.pendByAction[actionID] != nil || m.folByAction[actionID] != nil ||
		m.listByAction[actionID] != nil || m.records[actionID] != nil {
		violated("duplicate ActionID admitted")
	}

	p := &pending[T]{
		ticket:   m.nextTicket,
		actionID: actionID,
		kind:     kind,
		internal: o.Internal,
		state:    pReserved,
	}
	m.nextTicket++
	m.pendByAction[actionID] = p
	m.pendByTicket[p.ticket] = p
	switch {
	case o.Internal:
		m.internalActive = true
		m.internalRetHeld = true
		p.retHeld = true
	case kind == KindRequest:
		m.publicPending++
		p.retHeld = true
		m.retHeld++
	default:
		m.publicPending++
	}

	switch {
	case o.Follow != nil:
		f := &follow[T]{
			id:          m.nextBranch,
			actionID:    actionID,
			phase:       bActive,
			events:      folEvents,
			completions: folCompletions,
			caps:        o.Follow.Caps,
		}
		m.nextBranch++
		m.folByID[f.id] = f
		m.folByAction[actionID] = f
		p.fol = f
		p.folOpen = true
	case o.List != nil:
		l := &list[T]{
			id:          m.nextBranch,
			actionID:    actionID,
			phase:       lBuffering,
			completions: lstCompletions,
			caps:        o.List.Caps,
			observedCap: o.List.ObservedBytes,
			count:       o.List.Count,
			retHeld:     true,
		}
		m.nextBranch++
		m.retHeld++
		m.listByID[l.id] = l
		m.listByAction[actionID] = l
		p.lst = l
		p.lstOpen = true
	}

	m.check()
	return Ticket{p.ticket}, nil
}

// CommitWrite resolves the ticket's write as fully emitted. A response
// that outran this call, or client death, may already have completed
// the pending; the commit is then only the write resolution.
func (m *Machine[T]) CommitWrite(t Ticket) {
	p := m.ticket(t)
	if p.committed {
		violated("write resolved twice")
	}
	p.committed = true
	if p.state == pReserved {
		p.state = pInFlight
	}
	m.maybeReleaseTicket(p)
	m.check()
}

// AbortNotSent resolves the ticket's write as never begun: zero bytes
// reached the wire. Every reservation and provisional branch is
// released outright — the action does not exist remotely, so no
// retirement record is needed. Aborting after a correlated response is
// an invariant violation; aborting after a death completion is a legal
// race and only releases the remaining bookkeeping.
func (m *Machine[T]) AbortNotSent(t Ticket) {
	p := m.ticket(t)
	if p.committed {
		violated("write resolved twice")
	}
	if p.state == pCompleted && !p.deadDone {
		violated("abort after a correlated response")
	}
	p.committed = true
	if p.state != pCompleted {
		p.state = pCompleted
		m.releaseRetHold(p)
		delete(m.pendByAction, p.actionID)
		if p.internal {
			m.internalActive = false
		} else {
			m.publicPending--
		}
	}
	if p.folOpen {
		m.releaseFollowEntry(p.fol)
		p.folOpen = false
	}
	if p.lstOpen {
		l := p.lst
		if l.retHeld {
			l.retHeld = false
			m.retHeld--
		}
		m.dropListEntry(l)
		p.lstOpen = false
	}
	m.maybeReleaseTicket(p)
	m.check()
}

// Abandon resolves a fully written request whose context ended with no
// outcome: the reserved slot becomes a live record quarantining the
// ActionID until correlated terminal evidence or expiry, and any
// provisional follow or list branch is released with it. Abandon is
// legal only between CommitWrite and completion; the session must not
// call it after consuming a Completion.
func (m *Machine[T]) Abandon(t Ticket, now int64) {
	p := m.ticket(t)
	if p.state != pInFlight {
		violated("abandon without a committed write and no outcome")
	}
	m.clock(now)
	p.state = pCompleted
	delete(m.pendByAction, p.actionID)
	if p.internal {
		m.internalActive = false
	} else {
		m.publicPending--
	}

	switch p.kind {
	case KindRequest:
		if p.folOpen {
			m.releaseFollowEntry(p.fol)
			p.folOpen = false
		}
		if !p.retHeld {
			violated("abandon without a reserved retirement slot")
		}
		p.retHeld = false
		if !p.internal {
			m.retHeld--
		}
		// For an internal pending the reserved slot itself becomes the
		// record; internalRetHeld clears when the record releases.
		m.addRecord(p.actionID, KindRequest, nil, p.internal, true, false)
	case KindList:
		// The response is outstanding by definition; the terminal mark
		// may already have been seen. An earlier overflow may already
		// have converted the reservation into a live record, which
		// then continues unchanged.
		m.dropListEntry(p.lst)
		m.finishListSlot(p.lst)
		p.lstOpen = false
	}
	m.maybeReleaseTicket(p)
	m.check()
}

// AdoptFollow transfers the follow branch to the session after a
// successful Do. The branch may already be terminal — completed while
// provisional, lagged, or dead — and adoption hands it over as is.
func (m *Machine[T]) AdoptFollow(t Ticket) BranchID {
	p := m.ticket(t)
	if p.fol == nil || !p.folOpen {
		violated("adopting a follow the ticket does not hold")
	}
	p.folOpen = false
	p.fol.adopted = true
	m.maybeReleaseTicket(p)
	m.check()
	return BranchID{p.fol.id}
}

// CloseFollow releases a provisional follow that will never be adopted:
// Do returned non-nil, so no caller-owned resource escapes.
func (m *Machine[T]) CloseFollow(t Ticket) {
	p := m.ticket(t)
	if p.fol == nil || !p.folOpen {
		violated("closing a follow the ticket does not hold")
	}
	p.folOpen = false
	m.releaseFollowEntry(p.fol)
	m.maybeReleaseTicket(p)
	m.check()
}

// AdoptList transfers the list branch to the session after a Success
// response. The branch may already be complete, cancelled, or failed;
// adoption hands it over as is.
func (m *Machine[T]) AdoptList(t Ticket) BranchID {
	p := m.ticket(t)
	if p.lst == nil || !p.lstOpen {
		violated("adopting a list the ticket does not hold")
	}
	p.lstOpen = false
	p.lst.adopted = true
	m.maybeReleaseTicket(p)
	m.check()
	return BranchID{p.lst.id}
}

// CloseList releases a list branch after a death completion, the one
// completed outcome that leaves the branch unadopted and unreleased.
// An Error response releases the branch machine-side, and Abandon and
// AbortNotSent resolve it themselves, so no call is due after those.
func (m *Machine[T]) CloseList(t Ticket) {
	p := m.ticket(t)
	if p.lst == nil || !p.lstOpen {
		violated("closing a list the ticket does not hold")
	}
	if p.lst.phase != lDead {
		violated("closing a live unadopted list")
	}
	p.lstOpen = false
	m.dropListEntry(p.lst)
	m.maybeReleaseTicket(p)
	m.check()
}

// ticket resolves a Ticket to its live pending.
func (m *Machine[T]) ticket(t Ticket) *pending[T] {
	p := m.pendByTicket[t.n]
	if p == nil {
		violated("unknown ticket")
	}
	return p
}

// maybeReleaseTicket drops the ticket entry once every obligation is
// resolved: the outcome committed, the write resolved, and any follow
// or list branch adopted or released.
func (m *Machine[T]) maybeReleaseTicket(p *pending[T]) {
	if p.state == pCompleted && p.committed && !p.folOpen && !p.lstOpen {
		delete(m.pendByTicket, p.ticket)
	}
}

// publicRecords counts live records charged against the public
// retirement pool.
func (m *Machine[T]) publicRecords() int {
	n := 0
	for _, r := range m.records {
		if !r.internal {
			n++
		}
	}
	return n
}
