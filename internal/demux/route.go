package demux

// Route classifies one inbound message and delivers, absorbs, or
// discards it. It is total: every envelope reaches exactly one
// classification arm. Only the session's reader goroutine calls Route,
// and never after a fatality or Kill.
func (m *Machine[T]) Route(env Envelope, msg T) Effects[T] {
	if m.dead {
		violated("route on a dead machine")
	}
	if env.Size <= 0 {
		violated("non-positive envelope size")
	}
	m.clock(env.Now)

	var fx Effects[T]
	switch env.Class {
	case ClassResponse:
		// Responses are exactly-once request accounting: strict.
		if env.ActionID == "" {
			return m.fatal(ReasonResponseNoID, 0)
		}
		if !env.Own {
			return m.fatal(ReasonResponseForeign, 0)
		}
		if !m.routeResponse(env, msg, &fx) {
			return m.fatal(ReasonResponseUnmatched, 0)
		}
	case ClassEvent:
		// Events are broadcast: lenient.
		m.routeEvent(env, msg, &fx)
	default:
		return m.fatal(ReasonEnvelopeInvalid, 0)
	}
	m.check()
	return fx
}

// routeResponse correlates an own response against the active pending
// or a live record. It reports false when neither matches — unknown or
// duplicate — which is fatal.
func (m *Machine[T]) routeResponse(env Envelope, msg T, fx *Effects[T]) bool {
	if p := m.pendByAction[env.ActionID]; p != nil {
		if p.kind != env.Kind {
			violated("ActionID kind diverged between admission and routing")
		}
		if p.state == pCompleted {
			violated("completed pending still indexed for routing")
		}
		p.state = pCompleted
		delete(m.pendByAction, env.ActionID)
		if p.internal {
			m.internalActive = false
		} else {
			m.publicPending--
		}
		fx.Complete = append(fx.Complete, Completion[T]{
			Ticket:    Ticket{p.ticket},
			Response:  msg,
			Delivered: true,
		})
		switch p.kind {
		case KindRequest:
			// The response is the request's terminal evidence.
			m.releaseRetHold(p)
		case KindList:
			m.listResponse(p, env, fx)
		}
		m.maybeReleaseTicket(p)
		// A pre-response overflow may have left a live drain record for
		// the same ActionID; the response is evidence for it too.
		if r := m.records[env.ActionID]; r != nil {
			r.responseEvidence(env.Success)
			m.maybeDropRecord(r)
		}
		return true
	}
	if r := m.records[env.ActionID]; r != nil {
		r.absorbed++
		m.ctr.Quarantined++
		r.responseEvidence(env.Success)
		m.maybeDropRecord(r)
		return true
	}
	return false
}

// listResponse arms or resolves the list branch when its initial
// response is routed.
func (m *Machine[T]) listResponse(p *pending[T], env Envelope, fx *Effects[T]) {
	l := p.lst
	l.responseSeen = true
	if env.Success {
		switch l.phase {
		case lBuffering:
			l.phase = lStreaming
		case lBufferedComplete:
			m.commitListComplete(l, fx)
		case lBufferedCancelled:
			m.commitListCancelled(l, fx)
		case lDead:
			// Failed before the response (overflow, count mismatch):
			// adoption hands over the already-terminal branch.
		default:
			violated("list already streaming before its response")
		}
		return
	}
	// An Error response ends the exchange: the remote never streams
	// items for a rejected list. The branch never escapes; buffered
	// data is discarded and stray later events fall to late-list
	// discard.
	l.markSeen = true
	m.dropListEntry(l)
	m.finishListSlot(l)
	p.lstOpen = false
}

// routeEvent fans an event out to its list or follow branch, a live
// record's quarantine, and ordinary subscriptions, per the routing
// table.
func (m *Machine[T]) routeEvent(env Envelope, msg T, fx *Effects[T]) {
	if !env.Own {
		m.fanout(env, msg, fx)
		return
	}
	switch env.Kind {
	case KindList:
		if l := m.listByAction[env.ActionID]; l != nil {
			m.routeListEvent(l, env, msg, fx)
			return
		}
		if r := m.records[env.ActionID]; r != nil {
			r.absorbed++
			m.ctr.Quarantined++
			if env.Mark == MarkComplete || env.Mark == MarkCancelled {
				r.markEvidence()
			} else if _, ok := r.completions[env.Name]; ok {
				r.markEvidence()
			}
			m.maybeDropRecord(r)
			return
		}
		// Late traffic for a completed or retired list: the kind
		// discriminator keeps it away from ordinary subscriptions
		// without retaining any per-ID state, forever.
		m.ctr.LateListDiscards++
	case KindRequest:
		if r := m.records[env.ActionID]; r != nil {
			// Quarantined: absorbed and counted, never ordinary
			// delivery. Events are not evidence for a request record.
			r.absorbed++
			m.ctr.Quarantined++
			return
		}
		if f := m.folByAction[env.ActionID]; f != nil {
			m.routeFollowEvent(f, env, msg, fx)
		}
		// Correlated events remain eligible for ordinary delivery in
		// addition to their follow.
		m.fanout(env, msg, fx)
	default:
		violated("own envelope without a parsed kind")
	}
}

// routeFollowEvent delivers one correlated event to its follow branch.
func (m *Machine[T]) routeFollowEvent(f *follow[T], env Envelope, msg T, fx *Effects[T]) {
	if _, terminal := f.completions[env.Name]; terminal {
		// The completion event is charged and enqueued before clean
		// completion commits; if its reservation fails, Lagged wins
		// and the terminal event is not silently dropped behind a
		// clean end-of-stream.
		if !m.reserveSubFamily(&f.q, f.caps, env.Size) {
			m.terminalFollow(f, ReasonLagged, fx)
			return
		}
		f.q.push(msg, env.Size)
		m.subBytes += env.Size
		f.phase = bDraining
		delete(m.folByAction, f.actionID)
		fx.wake(f.id)
		return
	}
	if f.events != nil {
		if _, ok := f.events[env.Name]; !ok {
			return // not selected: ordinary fan-out only
		}
	}
	if !m.reserveSubFamily(&f.q, f.caps, env.Size) {
		m.terminalFollow(f, ReasonLagged, fx)
		return
	}
	f.q.push(msg, env.Size)
	m.subBytes += env.Size
	fx.wake(f.id)
}

// routeListEvent delivers one correlated event to its routing-active
// list branch.
func (m *Machine[T]) routeListEvent(l *list[T], env Envelope, msg T, fx *Effects[T]) {
	// The cumulative observed budget bounds what the remote may stream
	// through one list regardless of drain rate; every correlated
	// event counts while the branch is routing-active.
	l.observed += env.Size
	over := l.observed > l.observedCap

	cancel := env.Mark == MarkCancelled
	complete := env.Mark == MarkComplete
	if !complete && !cancel {
		_, complete = l.completions[env.Name]
	}
	if cancel || complete {
		// Terminal evidence arrives with the event that carries it, even
		// when that same event overflows the observed budget: the slot
		// settlement in failList must not convert into a drain record
		// waiting for a second mark the remote will never send — that
		// record could only expire and kill a healthy client.
		l.markSeen = true
	}

	if l.phase == lBufferedComplete || l.phase == lBufferedCancelled {
		// Traffic after a buffered terminal mark, before the response:
		// absorbed and counted, never delivered.
		m.ctr.Quarantined++
		if over {
			m.failList(l, ReasonOverflow, fx)
		}
		return
	}
	if over {
		m.failList(l, ReasonOverflow, fx)
		return
	}

	switch {
	case cancel:
		if l.phase == lBuffering {
			l.phase = lBufferedCancelled
		} else {
			m.commitListCancelled(l, fx)
		}
	case complete:
		// A declared count, when configured and present, is verified
		// against the items observed before anything commits.
		if l.count != nil {
			if want, ok := l.count(msg); ok && want != l.items {
				m.failList(l, ReasonCountMismatch, fx)
				return
			}
		}
		// The completion event is charged and stored for
		// ListCompletion, not enqueued as an item.
		if !m.reserveList(l, env.Size, false) {
			m.failList(l, ReasonOverflow, fx)
			return
		}
		l.completion = msg
		l.completionSize = env.Size
		l.hasCompletion = true
		m.listBytes += env.Size
		if l.phase == lBuffering {
			l.phase = lBufferedComplete
		} else {
			m.commitListComplete(l, fx)
		}
	default:
		if !m.reserveList(l, env.Size, true) {
			m.failList(l, ReasonOverflow, fx)
			return
		}
		l.q.push(msg, env.Size)
		l.items++
		m.listBytes += env.Size
		fx.wake(l.id)
	}
}

// commitListComplete commits clean completion: queued items are
// preserved and drain to end-of-stream.
func (m *Machine[T]) commitListComplete(l *list[T], fx *Effects[T]) {
	if !l.routingActive() {
		violated("second terminal for a list")
	}
	l.phase = lDraining
	delete(m.listByAction, l.actionID)
	m.finishListSlot(l)
	fx.wake(l.id)
}

// commitListCancelled commits remote cancellation: queued items are
// discarded at commit time.
func (m *Machine[T]) commitListCancelled(l *list[T], fx *Effects[T]) {
	if !l.routingActive() {
		violated("second terminal for a list")
	}
	l.phase = lDead
	l.reason = ReasonCancelled
	delete(m.listByAction, l.actionID)
	m.discardList(l)
	m.finishListSlot(l)
	fx.wake(l.id)
}

// failList commits a local list failure — overflow or count mismatch —
// discarding retained state and settling the reserved slot.
func (m *Machine[T]) failList(l *list[T], reason Reason, fx *Effects[T]) {
	if !l.routingActive() {
		violated("second terminal for a list")
	}
	l.phase = lDead
	l.reason = reason
	delete(m.listByAction, l.actionID)
	m.discardList(l)
	m.finishListSlot(l)
	fx.wake(l.id)
}

// reserveList checks a list delivery against the branch caps and the
// client-wide retained list aggregate. Stored completion bytes count
// against the branch byte cap; only items count against the item cap.
func (m *Machine[T]) reserveList(l *list[T], size int, item bool) bool {
	if item && l.q.len() >= l.caps.Items {
		return false
	}
	return l.q.bytes+l.completionSize+size <= l.caps.Bytes &&
		m.listBytes+size <= m.lim.MaxListBytes
}

// reserveSubFamily checks a delivery against a subscription-family
// branch's caps and the client-wide queued subscription aggregate.
func (m *Machine[T]) reserveSubFamily(q *queue[T], caps Caps, size int) bool {
	return q.len() < caps.Items &&
		q.bytes+size <= caps.Bytes &&
		m.subBytes+size <= m.lim.MaxSubscriptionBytes
}

// fanout delivers an event to every matching ordinary subscription in
// stable registration order. A recipient that cannot reserve its local
// and aggregate charges commits Lagged alone and fan-out continues; an
// event matching no subscription is dropped at zero queue cost.
func (m *Machine[T]) fanout(env Envelope, msg T, fx *Effects[T]) {
	matched := false
	anyLagged := false
	for _, s := range m.subOrder {
		if s.names != nil {
			if _, ok := s.names[env.Name]; !ok {
				continue
			}
		}
		matched = true
		if m.reserveSubFamily(&s.q, s.caps, env.Size) {
			s.q.push(msg, env.Size)
			m.subBytes += env.Size
			fx.wake(s.id)
		} else {
			// The victim's terminal commits — and its charges release —
			// before routing continues, so a later recipient sees the
			// freed aggregate capacity.
			m.terminalSub(s, ReasonLagged, fx)
			anyLagged = true
		}
	}
	if anyLagged {
		m.compactOrder()
	}
	if !matched {
		m.ctr.Unmatched++
	}
}

// compactOrder removes terminal subscriptions from the registration
// order after a fan-out that lagged at least one recipient.
func (m *Machine[T]) compactOrder() {
	live := m.subOrder[:0]
	for _, s := range m.subOrder {
		if s.phase == bActive {
			live = append(live, s)
		}
	}
	for i := len(live); i < len(m.subOrder); i++ {
		m.subOrder[i] = nil
	}
	m.subOrder = live
}
