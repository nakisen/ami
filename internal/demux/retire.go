package demux

// record is one live retirement/drain record, occupying the slot
// reserved at its action's admission. While live it quarantines its
// ActionID: correlated traffic is absorbed and counted, never
// delivered. It releases only when its outstanding evidence resolves,
// through expiry fatality, or at client death — the machine never
// forgets a live record, so a later message can never be reclassified
// as an ordinary event after an unproven quarantine.
type record struct {
	actionID string
	kind     Kind
	deadline int64
	internal bool

	// Outstanding evidence. A request record needs only its response.
	// A list record needs its terminal mark and — so a late response
	// always has a home — its response; an Error response resolves
	// both, because a rejected list never streams.
	needResponse bool
	needMark     bool

	completions map[string]struct{} // list kind: declared terminal names
	absorbed    uint64
}

func (r *record) responseEvidence(success bool) {
	r.needResponse = false
	if !success {
		r.needMark = false
	}
}

func (r *record) markEvidence() {
	r.needMark = false
}

// addRecord creates a live record in the slot its action reserved at
// admission, dated by the machine's logical clock.
func (m *Machine[T]) addRecord(actionID string, kind Kind, completions map[string]struct{}, internal, needResponse, needMark bool) {
	if !needResponse && !needMark {
		violated("record without outstanding evidence")
	}
	if m.records[actionID] != nil {
		violated("duplicate retirement record")
	}
	m.records[actionID] = &record{
		actionID:     actionID,
		kind:         kind,
		deadline:     m.now + m.lim.RetirementLifetime,
		internal:     internal,
		needResponse: needResponse,
		needMark:     needMark,
		completions:  completions,
	}
}

// maybeDropRecord releases a record whose outstanding evidence has
// fully resolved.
func (m *Machine[T]) maybeDropRecord(r *record) {
	if r.needResponse || r.needMark {
		return
	}
	if r.internal {
		m.internalRetHeld = false
	}
	delete(m.records, r.actionID)
}

// NextDeadline reports the earliest live record deadline, telling the
// session when to arm its retirement timer.
func (m *Machine[T]) NextDeadline() (int64, bool) {
	if m.dead || len(m.records) == 0 {
		return 0, false
	}
	var earliest int64
	first := true
	for _, r := range m.records {
		if first || r.deadline < earliest {
			earliest = r.deadline
			first = false
		}
	}
	return earliest, true
}

// Expire commits any record whose deadline has passed without
// evidence: the client closes with the retirement cause rather than
// risk misclassifying late correlated traffic. With nothing expired it
// returns empty Effects.
func (m *Machine[T]) Expire(now int64) Effects[T] {
	if m.dead {
		return Effects[T]{}
	}
	m.clock(now)
	var expired *record
	for _, r := range m.records {
		if r.deadline > now {
			continue
		}
		if expired == nil || r.deadline < expired.deadline ||
			(r.deadline == expired.deadline && r.actionID < expired.actionID) {
			expired = r
		}
	}
	if expired == nil {
		return Effects[T]{}
	}
	return m.fatal(ReasonRetirementExpired, expired.kind)
}
