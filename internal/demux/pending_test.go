package demux

import "testing"

func TestAdmitValidation(t *testing.T) {
	fol := &FollowOptions{Caps: Caps{Items: 4, Bytes: 512}}
	for _, tt := range []struct {
		name string
		id   string
		kind Kind
		o    AdmitOptions[int]
		want error
	}{
		{"empty ActionID", "", KindRequest, AdmitOptions[int]{}, ErrInvalidOptions},
		{"zero kind", "a1", 0, AdmitOptions[int]{}, ErrInvalidOptions},
		{"list without options", "a1", KindList, AdmitOptions[int]{}, ErrInvalidOptions},
		{"request with list options", "a1", KindRequest, AdmitOptions[int]{List: listOpts()}, ErrInvalidOptions},
		{"list with follow", "a1", KindList, AdmitOptions[int]{List: listOpts(), Follow: fol}, ErrInvalidOptions},
		{"internal with follow", "a1", KindRequest, AdmitOptions[int]{Internal: true, Follow: fol}, ErrInvalidOptions},
		{"zero follow caps", "a1", KindRequest, AdmitOptions[int]{Follow: &FollowOptions{}}, ErrInvalidOptions},
		{"zero list caps", "a1", KindList, AdmitOptions[int]{List: &ListOptions[int]{ObservedBytes: 1}}, ErrInvalidOptions},
		{"zero observed budget", "a1", KindList, AdmitOptions[int]{List: &ListOptions[int]{Caps: Caps{Items: 1, Bytes: 1}}}, ErrInvalidOptions},
		{"empty follow event name", "a1", KindRequest, AdmitOptions[int]{Follow: &FollowOptions{Events: []string{""}, Caps: Caps{Items: 1, Bytes: 1}}}, ErrInvalidOptions},
		{"too many completion names", "a1", KindList, AdmitOptions[int]{List: &ListOptions[int]{
			Completions:   []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"},
			Caps:          Caps{Items: 1, Bytes: 1},
			ObservedBytes: 1,
		}}, ErrMatcherLimit},
	} {
		m := newMachine(t)
		if _, err := m.Admit(tt.id, tt.kind, tt.o); err == nil {
			t.Errorf("%s: admission accepted", tt.name)
		} else {
			wantErr(t, err, tt.want)
		}
	}

	m := newMachine(t)
	m.Kill(ReasonKilled)
	if _, err := m.Admit("a1", KindRequest, AdmitOptions[int]{}); err == nil {
		t.Fatalf("admission accepted on a dead machine")
	} else {
		wantErr(t, err, ErrDead)
	}
}

func TestRequestLifecycle(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	wantRetirement(t, m, 1, 0)
	m.CommitWrite(tk)

	fx := route(t, m, resp("a1", KindRequest, true), 42)
	c := completed(t, fx, tk)
	if !c.Delivered || c.Response != 42 {
		t.Fatalf("completion %+v, want delivered payload 42", c)
	}
	wantRetirement(t, m, 0, 0)
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after full resolution")
	}

	// Exactly-once accounting: the duplicate response is fatal.
	routeFatal(t, m, resp("a1", KindRequest, true), 43, ReasonResponseUnmatched)
}

func TestErrorResponseStillDelivered(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	m.CommitWrite(tk)
	fx := route(t, m, resp("a1", KindRequest, false), 7)
	c := completed(t, fx, tk)
	if !c.Delivered || c.Response != 7 {
		t.Fatalf("error response not delivered to its waiter: %+v", c)
	}
	wantRetirement(t, m, 0, 0)
}

func TestResponseOutrunsCommitWrite(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	fx := route(t, m, resp("a1", KindRequest, true), 1)
	completed(t, fx, tk)
	if m.Tickets() != 1 {
		t.Fatalf("ticket released before the write resolution")
	}
	m.CommitWrite(tk) // becomes the write resolution only
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after the trailing CommitWrite")
	}
}

func TestResponseStrictness(t *testing.T) {
	for _, tt := range []struct {
		name string
		env  Envelope
		want Reason
	}{
		{"no ActionID", Envelope{Class: ClassResponse, Size: 10}, ReasonResponseNoID},
		{"foreign", respForeign("other-1"), ReasonResponseForeign},
		{"unknown", resp("ghost", KindRequest, true), ReasonResponseUnmatched},
		{"invalid envelope", Envelope{Size: 10}, ReasonEnvelopeInvalid},
	} {
		m := newMachine(t)
		routeFatal(t, m, tt.env, 0, tt.want)
	}
}

func TestAbortNotSent(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	m.AbortNotSent(tk)
	wantRetirement(t, m, 0, 0)
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after abort")
	}
	// The action never existed remotely: its late response is unknown.
	routeFatal(t, m, resp("a1", KindRequest, true), 0, ReasonResponseUnmatched)
}

func TestWriteResolutionDiscipline(t *testing.T) {
	t.Run("commit twice", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
		m.CommitWrite(tk)
		wantPanic(t, "write resolved twice", func() { m.CommitWrite(tk) })
	})
	t.Run("abort after commit", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
		m.CommitWrite(tk)
		wantPanic(t, "write resolved twice", func() { m.AbortNotSent(tk) })
	})
	t.Run("abort after response", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
		route(t, m, resp("a1", KindRequest, true), 1)
		wantPanic(t, "abort after a correlated response", func() { m.AbortNotSent(tk) })
	})
	t.Run("abandon before commit", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
		wantPanic(t, "abandon without a committed write", func() { m.Abandon(tk, 1) })
	})
	t.Run("abandon after resolved completion", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
		m.CommitWrite(tk)
		route(t, m, resp("a1", KindRequest, true), 1)
		// Fully resolved: the ticket entry itself is gone.
		wantPanic(t, "unknown ticket", func() { m.Abandon(tk, 1) })
	})
	t.Run("abandon after completion with an open follow", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{
			Follow: &FollowOptions{Caps: Caps{Items: 4, Bytes: 512}},
		})
		m.CommitWrite(tk)
		route(t, m, resp("a1", KindRequest, true), 1)
		// The follow obligation keeps the ticket alive, but the
		// outcome is already committed.
		wantPanic(t, "abandon without a committed write", func() { m.Abandon(tk, 1) })
	})
	t.Run("unknown ticket", func(t *testing.T) {
		m := newMachine(t)
		wantPanic(t, "unknown ticket", func() { m.CommitWrite(Ticket{n: 99}) })
	})
}

func TestPendingLimit(t *testing.T) {
	m := newMachine(t, func(l *Limits) { l.MaxPending = 1 })
	admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	if _, err := m.Admit("a2", KindRequest, AdmitOptions[int]{}); err == nil {
		t.Fatalf("admission beyond MaxPending accepted")
	} else {
		wantErr(t, err, ErrPendingLimit)
	}
	// The internal keepalive slot is reserved separately.
	if _, err := m.Admit("ping-1", KindRequest, AdmitOptions[int]{Internal: true}); err != nil {
		t.Fatalf("internal admission blocked by the public limit: %v", err)
	}
	if _, err := m.Admit("ping-2", KindRequest, AdmitOptions[int]{Internal: true}); err == nil {
		t.Fatalf("overlapping internal admission accepted")
	} else {
		wantErr(t, err, ErrInternalBusy)
	}
}

func TestInternalLifecycle(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "ping-1", KindRequest, AdmitOptions[int]{Internal: true})
	m.CommitWrite(tk)
	fx := route(t, m, resp("ping-1", KindRequest, true), 1)
	completed(t, fx, tk)
	wantRetirement(t, m, 0, 0)
	// The reserved slot is free for the next Ping.
	admit(t, m, "ping-2", KindRequest, AdmitOptions[int]{Internal: true})
}

func TestRetirementLimitGatesAdmission(t *testing.T) {
	m := newMachine(t, func(l *Limits) { l.MaxRetirement = 2 })
	tk1 := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	admit(t, m, "a2", KindRequest, AdmitOptions[int]{})
	if _, err := m.Admit("a3", KindRequest, AdmitOptions[int]{}); err == nil {
		t.Fatalf("admission accepted with an exhausted retirement pool")
	} else {
		wantErr(t, err, ErrRetirementLimit)
	}
	// Completing a1 releases its held slot.
	m.CommitWrite(tk1)
	route(t, m, resp("a1", KindRequest, true), 1)
	admit(t, m, "a3", KindRequest, AdmitOptions[int]{})
}

func TestAbortNotSentReleasesBranches(t *testing.T) {
	t.Run("follow", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 4, Bytes: 512}, nil, nil))
		route(t, m, evOwn("x", "a1", KindRequest), 1) // buffered while provisional
		m.AbortNotSent(tk)
		wantAggregates(t, m, 0, 0)
		wantRetirement(t, m, 0, 0)
		_, follows, _ := m.Branches()
		if follows != 0 || m.Tickets() != 0 {
			t.Fatalf("state retained after abort: follows=%d tickets=%d", follows, m.Tickets())
		}
	})
	t.Run("list", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "l1", KindList, AdmitOptions[int]{List: listOpts("done")})
		route(t, m, evOwn("item", "l1", KindList), 1)
		m.AbortNotSent(tk)
		wantAggregates(t, m, 0, 0)
		wantRetirement(t, m, 0, 0) // released outright: no record for an unsent action
		_, _, lists := m.Branches()
		if lists != 0 || m.Tickets() != 0 {
			t.Fatalf("state retained after abort: lists=%d tickets=%d", lists, m.Tickets())
		}
		// Late correlated traffic is late-list discard, not fatal.
		route(t, m, evOwn("item", "l1", KindList), 2)
	})
}

func TestCloseListDiscipline(t *testing.T) {
	t.Run("without a list", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
		wantPanic(t, "closing a list the ticket does not hold", func() { m.CloseList(tk) })
	})
	t.Run("live branch", func(t *testing.T) {
		m := newMachine(t)
		tk := startList(t, m, "l1", listOpts("done"))
		wantPanic(t, "closing a live unadopted list", func() { m.CloseList(tk) })
	})
}

func TestReasonStrings(t *testing.T) {
	for r := ReasonNone; r <= ReasonClientDead; r++ {
		if r.String() == "unknown" {
			t.Errorf("reason %d has no diagnostic name", r)
		}
	}
	if Reason(255).String() != "unknown" {
		t.Errorf("out-of-range reason not reported as unknown")
	}
}

func TestKindDivergencePanics(t *testing.T) {
	m := newMachine(t)
	admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	wantPanic(t, "kind diverged", func() {
		m.Route(resp("a1", KindList, true), 0)
	})
}
