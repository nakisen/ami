package demux

// phase is the life stage shared by subscriptions and follows.
type phase uint8

const (
	bActive   phase = 1 + iota
	bDraining       // clean terminal: the queue drains to end-of-stream
	bDead           // terminal with a reason: the queue was discarded
)

// lphase is a list branch's life stage. The three buffered phases are
// routing-active: a list tolerates items and even its terminal mark
// before the initial response.
type lphase uint8

const (
	lBuffering         lphase = 1 + iota
	lBufferedComplete         // terminal mark buffered, awaiting the response
	lBufferedCancelled        // cancellation buffered, awaiting the response
	lStreaming                // response Success seen
	lDraining                 // clean terminal: queued items drain to end-of-stream
	lDead                     // terminal with a reason: retained state was discarded
)

// subscription is one ordinary event subscription.
type subscription[T any] struct {
	id     uint64
	phase  phase
	reason Reason
	names  map[string]struct{} // folded selection; nil matches every event
	caps   Caps
	q      queue[T]
}

// follow is one request-correlated follow branch, routing-active from
// admission so nothing outruns registration.
type follow[T any] struct {
	id          uint64
	actionID    string
	phase       phase
	reason      Reason
	adopted     bool
	events      map[string]struct{} // folded nonterminal selection; nil selects all
	completions map[string]struct{} // folded terminal names; nil means explicit close only
	caps        Caps
	q           queue[T]
}

// list is one list-action branch, routing-active from admission.
type list[T any] struct {
	id       uint64
	actionID string
	phase    lphase
	reason   Reason
	adopted  bool

	completions map[string]struct{}
	count       func(T) (int64, bool)
	caps        Caps
	observedCap int

	observed int   // cumulative correlated wire bytes while routing-active
	items    int64 // items ever enqueued, verified against a declared count

	completion     T
	completionSize int
	hasCompletion  bool

	// Release evidence for the reserved retirement slot: the slot is
	// held until both the response and the terminal mark are resolved,
	// an Error response resolving both.
	responseSeen bool
	markSeen     bool
	retHeld      bool

	q queue[T]
}

func (l *list[T]) routingActive() bool {
	switch l.phase {
	case lBuffering, lBufferedComplete, lBufferedCancelled, lStreaming:
		return true
	}
	return false
}

// Subscribe registers an ordinary subscription, eagerly routing-active
// in stable registration order.
func (m *Machine[T]) Subscribe(match Matcher, caps Caps) (BranchID, error) {
	if m.dead {
		return BranchID{}, ErrDead
	}
	if !caps.valid() {
		return BranchID{}, ErrInvalidOptions
	}
	names, err := m.foldSet(match.Events)
	if err != nil {
		return BranchID{}, err
	}
	if len(m.subOrder) >= m.lim.MaxSubscriptions {
		return BranchID{}, ErrSubscriptionLimit
	}
	s := &subscription[T]{
		id:    m.nextBranch,
		phase: bActive,
		names: names,
		caps:  caps,
	}
	m.nextBranch++
	m.subByID[s.id] = s
	m.subOrder = append(m.subOrder, s)
	m.check()
	return BranchID{s.id}, nil
}

// Close releases a branch by ID. On an active branch it commits the
// local-close terminal first: a subscription or follow discards its
// queue; a streaming list additionally converts its reserved slot into
// a drain record, because the remote may still stream. On an already
// terminal branch, Close is the acknowledgment that removes the
// retained bookkeeping, discarding any undrained clean-terminal queue.
// Closing an unknown (already closed) branch is a no-op.
func (m *Machine[T]) Close(id BranchID) {
	if s := m.subByID[id.n]; s != nil {
		if s.phase == bActive {
			s.phase = bDead
			s.reason = ReasonClosed
			m.dropFromOrder(s)
		}
		m.subBytes -= s.q.reset()
		delete(m.subByID, id.n)
		m.check()
		return
	}
	if f := m.folByID[id.n]; f != nil {
		if !f.adopted {
			violated("closing an unadopted follow by branch ID")
		}
		if f.phase == bActive {
			f.phase = bDead
			f.reason = ReasonClosed
			delete(m.folByAction, f.actionID)
		}
		m.subBytes -= f.q.reset()
		delete(m.folByID, id.n)
		m.check()
		return
	}
	if l := m.listByID[id.n]; l != nil {
		if !l.adopted {
			violated("closing an unadopted list by branch ID")
		}
		switch l.phase {
		case lStreaming:
			l.phase = lDead
			l.reason = ReasonClosed
			delete(m.listByAction, l.actionID)
			m.discardList(l)
			m.finishListSlot(l)
		case lDraining, lDead:
			m.discardList(l)
		default:
			violated("adopted list in a pre-response phase")
		}
		delete(m.listByID, id.n)
		m.check()
		return
	}
}

// Take removes one queued item from a branch, releasing its charges,
// or reports the branch state. Single-consumer discipline is the
// session's contract; the machine only executes it.
func (m *Machine[T]) Take(id BranchID) (T, TakeResult) {
	var zero T
	if s := m.subByID[id.n]; s != nil {
		switch {
		case s.phase == bDead:
			return zero, TakeResult{State: TakeTerminal, Reason: s.reason}
		case s.q.len() > 0:
			msg, size := s.q.pop()
			m.subBytes -= size
			m.check()
			return msg, TakeResult{State: TakeItem}
		default:
			return zero, TakeResult{State: TakeEmpty}
		}
	}
	if f := m.folByID[id.n]; f != nil {
		if !f.adopted {
			violated("taking from an unadopted follow")
		}
		switch {
		case f.phase == bDead:
			return zero, TakeResult{State: TakeTerminal, Reason: f.reason}
		case f.q.len() > 0:
			msg, size := f.q.pop()
			m.subBytes -= size
			m.check()
			return msg, TakeResult{State: TakeItem}
		case f.phase == bDraining:
			return zero, TakeResult{State: TakeEOF}
		default:
			return zero, TakeResult{State: TakeEmpty}
		}
	}
	if l := m.listByID[id.n]; l != nil {
		if !l.adopted {
			violated("taking from an unadopted list")
		}
		switch {
		case l.phase == lDead:
			return zero, TakeResult{State: TakeTerminal, Reason: l.reason}
		case l.q.len() > 0:
			msg, size := l.q.pop()
			m.listBytes -= size
			m.check()
			return msg, TakeResult{State: TakeItem}
		case l.phase == lDraining:
			return zero, TakeResult{State: TakeEOF}
		case l.phase == lStreaming:
			return zero, TakeResult{State: TakeEmpty}
		default:
			violated("taking from a pre-response list")
		}
	}
	violated("taking from an unknown branch")
	return zero, TakeResult{}
}

// ListCompletion exposes a list's stored terminal completion event
// after clean completion.
func (m *Machine[T]) ListCompletion(id BranchID) (T, bool) {
	var zero T
	l := m.listByID[id.n]
	if l == nil || !l.hasCompletion {
		return zero, false
	}
	return l.completion, true
}

// terminalSub commits a subscription terminal and discards its queue.
// The caller maintains subOrder.
func (m *Machine[T]) terminalSub(s *subscription[T], reason Reason, fx *Effects[T]) {
	if s.phase != bActive {
		violated("second terminal for a subscription")
	}
	s.phase = bDead
	s.reason = reason
	m.subBytes -= s.q.reset()
	fx.wake(s.id)
}

// terminalFollow commits a follow terminal, discards its queue, and
// removes it from routing.
func (m *Machine[T]) terminalFollow(f *follow[T], reason Reason, fx *Effects[T]) {
	if f.phase != bActive {
		violated("second terminal for a follow")
	}
	f.phase = bDead
	f.reason = reason
	m.subBytes -= f.q.reset()
	delete(m.folByAction, f.actionID)
	fx.wake(f.id)
}

// releaseFollowEntry removes a follow branch that never escaped to the
// session, releasing its charges.
func (m *Machine[T]) releaseFollowEntry(f *follow[T]) {
	m.subBytes -= f.q.reset()
	delete(m.folByAction, f.actionID)
	delete(m.folByID, f.id)
}

// discardList releases a list branch's retained state: queued items
// and the stored completion event.
func (m *Machine[T]) discardList(l *list[T]) {
	m.listBytes -= l.q.reset()
	m.listBytes -= l.completionSize
	l.completionSize = 0
	l.hasCompletion = false
	var zero T
	l.completion = zero
}

// dropListEntry removes a list branch that never escaped to the
// session, releasing its retained state.
func (m *Machine[T]) dropListEntry(l *list[T]) {
	m.discardList(l)
	delete(m.listByAction, l.actionID)
	delete(m.listByID, l.id)
}

// finishListSlot settles a list's reserved retirement slot when the
// branch stops accounting for future correlated traffic. Evidence
// already in hand releases the slot outright; anything outstanding
// converts it into a live drain record.
func (m *Machine[T]) finishListSlot(l *list[T]) {
	if !l.retHeld {
		return
	}
	l.retHeld = false
	m.retHeld--
	if l.responseSeen && l.markSeen {
		return
	}
	m.addRecord(l.actionID, KindList, l.completions, false, !l.responseSeen, !l.markSeen)
}

// dropFromOrder removes an active subscription from the registration
// order.
func (m *Machine[T]) dropFromOrder(s *subscription[T]) {
	for i, x := range m.subOrder {
		if x == s {
			m.subOrder = append(m.subOrder[:i], m.subOrder[i+1:]...)
			return
		}
	}
	violated("subscription missing from the registration order")
}
