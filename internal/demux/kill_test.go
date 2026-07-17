package demux

import "testing"

func TestKillCascade(t *testing.T) {
	m := newMachine(t)

	sub := subscribe(t, m)
	route(t, m, ev("x"), 1) // queued on the subscription

	folTk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 8, Bytes: 4096}, nil, nil))
	m.CommitWrite(folTk)
	route(t, m, resp("a1", KindRequest, true), 0)
	fol := m.AdoptFollow(folTk)
	route(t, m, evOwn("x", "a1", KindRequest), 2) // queued on the follow (and the subscription)

	lstTk := startList(t, m, "l1", listOpts("done"))
	route(t, m, resp("l1", KindList, true), 0)
	lst := m.AdoptList(lstTk)
	route(t, m, evOwn("item", "l1", KindList), 3) // queued on the list

	pendTk := admit(t, m, "a2", KindRequest, AdmitOptions[int]{})
	m.CommitWrite(pendTk)

	pingTk := admit(t, m, "ping-1", KindRequest, AdmitOptions[int]{Internal: true})
	m.CommitWrite(pingTk)

	abandonRequest(t, m, "a3", 10) // a live record

	fx := m.Kill(ReasonKilled)

	// Every uncompleted pending completes exactly once, undelivered.
	for _, tk := range []Ticket{pendTk, pingTk} {
		c := completed(t, fx, tk)
		if c.Delivered {
			t.Fatalf("death completion claims a delivered response")
		}
	}
	if len(fx.Complete) != 2 {
		t.Fatalf("%d completions, want 2", len(fx.Complete))
	}

	// Every active branch was woken with its terminal.
	for _, id := range []BranchID{sub, fol, lst} {
		if !woken(fx, id) {
			t.Fatalf("branch %v not woken by the cascade", id)
		}
		takeState(t, m, id, TakeTerminal, ReasonClientDead)
	}

	// Queues discarded, records released, aggregates zeroed.
	wantAggregates(t, m, 0, 0)
	wantRetirement(t, m, 0, 0)
	if _, ok := m.NextDeadline(); ok {
		t.Fatalf("deadline armed on a dead machine")
	}

	// Kill is idempotent.
	again := m.Kill(ReasonKilled)
	if again.Fatal != nil || len(again.Wake) != 0 || len(again.Complete) != 0 {
		t.Fatalf("second Kill produced effects: %+v", again)
	}

	// The dead machine rejects new work and routing.
	if _, err := m.Admit("a9", KindRequest, AdmitOptions[int]{}); err == nil {
		t.Fatalf("admission accepted after death")
	} else {
		wantErr(t, err, ErrDead)
	}
	if _, err := m.Subscribe(Matcher{}, Caps{Items: 1, Bytes: 1}); err == nil {
		t.Fatalf("registration accepted after death")
	} else {
		wantErr(t, err, ErrDead)
	}
	wantPanic(t, "route on a dead machine", func() { m.Route(ev("x"), 0) })

	if reason, dead := m.Dead(); !dead || reason != ReasonKilled {
		t.Fatalf("Dead() = (%v, %t), want (killed, true)", reason, dead)
	}
}

func TestCleanDrainSurvivesKill(t *testing.T) {
	m := newMachine(t)

	// A follow and a list, both cleanly complete with queued items.
	folTk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 8, Bytes: 4096}, nil, []string{"done"}))
	m.CommitWrite(folTk)
	route(t, m, resp("a1", KindRequest, true), 0)
	fol := m.AdoptFollow(folTk)
	route(t, m, evOwn("x", "a1", KindRequest), 1)
	route(t, m, evOwn("done", "a1", KindRequest), 2)

	lstTk := startList(t, m, "l1", listOpts("done"))
	route(t, m, resp("l1", KindList, true), 0)
	lst := m.AdoptList(lstTk)
	route(t, m, evOwn("item", "l1", KindList), 3)
	route(t, m, evOwn("done", "l1", KindList), 1)

	m.Kill(ReasonKilled)

	// An already-terminal branch keeps draining after client death.
	takeItem(t, m, fol, 1)
	takeItem(t, m, fol, 2)
	takeState(t, m, fol, TakeEOF, 0)
	takeItem(t, m, lst, 3)
	takeState(t, m, lst, TakeEOF, 0)
	if _, ok := m.ListCompletion(lst); !ok {
		t.Fatalf("stored completion lost at death")
	}

	m.Close(fol, 0)
	m.Close(lst, 0)
	wantAggregates(t, m, 0, 0)
}

func TestDeathCompletionThenResolution(t *testing.T) {
	t.Run("follow", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 8, Bytes: 4096}, nil, nil))
		m.CommitWrite(tk)
		fx := m.Kill(ReasonKilled)
		completed(t, fx, tk)
		// The session's Do path errors out and resolves the branch.
		m.CloseFollow(tk)
		if m.Tickets() != 0 {
			t.Fatalf("ticket retained after post-death CloseFollow")
		}
	})
	t.Run("follow adopted dead", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 8, Bytes: 4096}, nil, nil))
		m.CommitWrite(tk)
		route(t, m, resp("a1", KindRequest, true), 0)
		m.Kill(ReasonKilled)
		// The response won first: Do succeeds and adopts a branch that
		// is already dead.
		id := m.AdoptFollow(tk)
		takeState(t, m, id, TakeTerminal, ReasonClientDead)
		m.Close(id, 0)
	})
	t.Run("write resolutions after death", func(t *testing.T) {
		m := newMachine(t)
		tk1 := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
		tk2 := admit(t, m, "a2", KindRequest, AdmitOptions[int]{})
		m.Kill(ReasonKilled)
		m.CommitWrite(tk1)  // trailing write resolution: a no-op
		m.AbortNotSent(tk2) // zero-byte failure caused by the death: tolerated
		if m.Tickets() != 0 {
			t.Fatalf("tickets retained after post-death write resolutions")
		}
	})
}

func TestRouteFatalityCascadesInOneCall(t *testing.T) {
	m := newMachine(t)
	sub := subscribe(t, m)
	tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
	m.CommitWrite(tk)

	fx := routeFatal(t, m, Envelope{Size: 10}, 0, ReasonEnvelopeInvalid)
	c := completed(t, fx, tk)
	if c.Delivered {
		t.Fatalf("fatality delivered a response")
	}
	if !woken(fx, sub) {
		t.Fatalf("subscription not woken by the fatality cascade")
	}
	takeState(t, m, sub, TakeTerminal, ReasonClientDead)
}

func TestExpireAndNextDeadlineOnDeadMachine(t *testing.T) {
	m := newMachine(t)
	abandonRequest(t, m, "a1", 10)
	m.Kill(ReasonKilled)
	if _, ok := m.NextDeadline(); ok {
		t.Fatalf("deadline armed on a dead machine")
	}
	fx := m.Expire(99999)
	if fx.Fatal != nil || len(fx.Wake) != 0 || len(fx.Complete) != 0 {
		t.Fatalf("Expire on a dead machine produced effects")
	}
}
