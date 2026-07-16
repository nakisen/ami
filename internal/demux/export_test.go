package demux

// Test-only accessors for the accounting the conformance properties
// assert against.

// Aggregates exposes the live charge counters.
func (m *Machine[T]) Aggregates() (subBytes, listBytes int) {
	return m.subBytes, m.listBytes
}

// RetirementLoad exposes the retirement pool occupancy: admission-held
// reservations and live public records.
func (m *Machine[T]) RetirementLoad() (held, records int) {
	return m.retHeld, m.publicRecords()
}

// Branches exposes the number of retained branch entries of each
// family, adopted or not, terminal or not.
func (m *Machine[T]) Branches() (subs, follows, lists int) {
	return len(m.subByID), len(m.folByID), len(m.listByID)
}

// Tickets exposes the number of unresolved ticket entries.
func (m *Machine[T]) Tickets() int {
	return len(m.pendByTicket)
}

// debugTickets lists unresolved ticket states for test diagnostics.
func (m *Machine[T]) debugTickets() []string {
	var out []string
	for _, p := range m.pendByTicket {
		out = append(out, p.actionID+
			" state="+itoa(int(p.state))+
			" committed="+boolStr(p.committed)+
			" dead="+boolStr(p.deadDone)+
			" folOpen="+boolStr(p.folOpen)+
			" lstOpen="+boolStr(p.lstOpen))
	}
	return out
}

func boolStr(b bool) string {
	if b {
		return "t"
	}
	return "f"
}
