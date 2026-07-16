package demux

import "testing"

// abandonRequest admits, commits, and abandons a request at the given
// logical time, returning its ActionID's record deadline via
// NextDeadline for single-record machines.
func abandonRequest(t *testing.T, m *Machine[int], id string, now int64) {
	t.Helper()
	tk := admit(t, m, id, KindRequest, AdmitOptions[int]{})
	m.CommitWrite(tk)
	m.Abandon(tk, now)
}

func TestRecordDeadlineFromLogicalClock(t *testing.T) {
	m := newMachine(t) // lifetime 1000
	abandonRequest(t, m, "a1", 100)
	dl, ok := m.NextDeadline()
	if !ok || dl != 1100 {
		t.Fatalf("NextDeadline = (%d, %t), want (1100, true)", dl, ok)
	}
}

func TestNextDeadlineReportsEarliest(t *testing.T) {
	m := newMachine(t)
	abandonRequest(t, m, "a1", 500) // deadline 1500
	abandonRequest(t, m, "a2", 600) // deadline 1600
	dl, ok := m.NextDeadline()
	if !ok || dl != 1500 {
		t.Fatalf("NextDeadline = (%d, %t), want (1500, true)", dl, ok)
	}
	// Releasing the earliest record advances the deadline.
	route(t, m, resp("a1", KindRequest, true), 0)
	dl, ok = m.NextDeadline()
	if !ok || dl != 1600 {
		t.Fatalf("NextDeadline = (%d, %t), want (1600, true)", dl, ok)
	}
}

func TestExpireBeforeDeadlineIsEmpty(t *testing.T) {
	m := newMachine(t)
	abandonRequest(t, m, "a1", 100)
	fx := m.Expire(1099)
	if fx.Fatal != nil {
		t.Fatalf("expiry fired before the deadline")
	}
	if _, dead := m.Dead(); dead {
		t.Fatalf("machine died on an empty expiry")
	}
}

func TestExpiryIsFatalWithTheKind(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		m := newMachine(t)
		abandonRequest(t, m, "a1", 100)
		fx := m.Expire(1100)
		if fx.Fatal == nil || fx.Fatal.Reason != ReasonRetirementExpired || fx.Fatal.Kind != KindRequest {
			t.Fatalf("fatality %+v, want retirement expiry of a request record", fx.Fatal)
		}
	})
	t.Run("list", func(t *testing.T) {
		m := newMachine(t)
		tk := startList(t, m, "l1", listOpts("done"))
		m.Abandon(tk, 100)
		fx := m.Expire(1100)
		if fx.Fatal == nil || fx.Fatal.Reason != ReasonRetirementExpired || fx.Fatal.Kind != KindList {
			t.Fatalf("fatality %+v, want retirement expiry of a list record", fx.Fatal)
		}
	})
}

func TestEvidenceReleasesBeforeExpiry(t *testing.T) {
	m := newMachine(t)
	abandonRequest(t, m, "a1", 100)
	route(t, m, resp("a1", KindRequest, true), 0) // absorbed: evidence
	if _, ok := m.NextDeadline(); ok {
		t.Fatalf("deadline armed with no live record")
	}
	fx := m.Expire(5000)
	if fx.Fatal != nil {
		t.Fatalf("released record still expired")
	}
}

func TestRecordsOccupyReservedSlots(t *testing.T) {
	m := newMachine(t, func(l *Limits) { l.MaxRetirement = 2 })
	abandonRequest(t, m, "a1", 10)
	abandonRequest(t, m, "a2", 20)
	wantRetirement(t, m, 0, 2)
	// Live records are never evicted: admission is rejected
	// definitely-not-sent.
	if _, err := m.Admit("a3", KindRequest, AdmitOptions[int]{}); err == nil {
		t.Fatalf("admission accepted with every slot occupied by live records")
	} else {
		wantErr(t, err, ErrRetirementLimit)
	}
	// Evidence releases a slot and admission recovers.
	route(t, m, resp("a1", KindRequest, true), 0)
	admit(t, m, "a3", KindRequest, AdmitOptions[int]{})
}

func TestInternalRecordOutsideThePublicPool(t *testing.T) {
	m := newMachine(t, func(l *Limits) { l.MaxRetirement = 1 })
	tk := admit(t, m, "ping-1", KindRequest, AdmitOptions[int]{Internal: true})
	m.CommitWrite(tk)
	m.Abandon(tk, 10)
	wantRetirement(t, m, 0, 0) // internal records do not count publicly

	// The public pool is untouched; the internal slot is occupied.
	admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	if _, err := m.Admit("ping-2", KindRequest, AdmitOptions[int]{Internal: true}); err == nil {
		t.Fatalf("internal admission accepted while its record is live")
	} else {
		wantErr(t, err, ErrInternalBusy)
	}

	// Evidence releases the internal slot for the next Ping.
	route(t, m, resp("ping-1", KindRequest, true), 0)
	admit(t, m, "ping-2", KindRequest, AdmitOptions[int]{Internal: true})
}

func TestQuarantineAccounting(t *testing.T) {
	m := newMachine(t)
	abandonRequest(t, m, "a1", 10)
	before := m.Counters().Quarantined
	route(t, m, evOwn("x", "a1", KindRequest), 1)
	route(t, m, evOwn("x", "a1", KindRequest), 2)
	route(t, m, resp("a1", KindRequest, true), 3)
	if got := m.Counters().Quarantined; got != before+3 {
		t.Fatalf("quarantined counter %d, want %d", got, before+3)
	}
}

func TestExpiryCascadesLikeAnyDeath(t *testing.T) {
	m := newMachine(t)
	sub := subscribe(t, m)
	abandonRequest(t, m, "a1", 100)
	fx := m.Expire(2000)
	if fx.Fatal == nil {
		t.Fatalf("no fatality at expiry")
	}
	if !woken(fx, sub) {
		t.Fatalf("subscription not woken by the death cascade")
	}
	takeState(t, m, sub, TakeTerminal, ReasonClientDead)
	if _, err := m.Admit("a2", KindRequest, AdmitOptions[int]{}); err == nil {
		t.Fatalf("admission accepted after expiry death")
	}
}
