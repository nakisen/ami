package demux

import "testing"

func followOpts(caps Caps, events, completions []string) AdmitOptions[int] {
	return AdmitOptions[int]{Follow: &FollowOptions{
		Events:      events,
		Completions: completions,
		Caps:        caps,
	}}
}

func TestFollowLifecycle(t *testing.T) {
	m := newMachine(t)
	sub := subscribe(t, m) // ordinary delivery continues alongside the follow
	tk := admit(t, m, "a1", KindRequest, followOpts(
		Caps{Items: 8, Bytes: 4096},
		[]string{"OriginateResponse"}, // folded at registration
		[]string{"followdone"},
	))
	m.CommitWrite(tk)

	// Correlated events arrive before the response: routing-active
	// from admission.
	route(t, m, evOwn("originateresponse", "a1", KindRequest), 1)
	// A correlated event outside the selection fans out ordinarily
	// only.
	route(t, m, evOwn("otherevent", "a1", KindRequest), 2)

	fx := route(t, m, resp("a1", KindRequest, true), 100)
	completed(t, fx, tk)
	id := m.AdoptFollow(tk)
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after adoption")
	}

	// Post-response correlated traffic keeps flowing to the follow.
	route(t, m, evOwn("originateresponse", "a1", KindRequest), 3)
	// The completion event is charged and enqueued, then the clean
	// terminal commits.
	fx = route(t, m, evOwn("followdone", "a1", KindRequest), 4)
	if !woken(fx, id) {
		t.Fatalf("follow not woken on completion")
	}

	takeItem(t, m, id, 1)
	takeItem(t, m, id, 3)
	takeItem(t, m, id, 4) // the terminal event itself
	takeState(t, m, id, TakeEOF, 0)

	// Ordinary delivery saw every correlated event, completion
	// included.
	for _, want := range []int{1, 2, 3, 4} {
		takeItem(t, m, sub, want)
	}

	// After the clean terminal the follow is out of routing: further
	// correlated events fan out ordinarily only.
	route(t, m, evOwn("originateresponse", "a1", KindRequest), 5)
	takeState(t, m, id, TakeEOF, 0)
	takeItem(t, m, sub, 5)

	m.Close(id, 0)
	m.Close(sub, 0)
	wantAggregates(t, m, 0, 0)
}

func TestFollowNilSelectionTakesAll(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 8, Bytes: 4096}, nil, nil))
	m.CommitWrite(tk)
	route(t, m, evOwn("anything", "a1", KindRequest), 1)
	route(t, m, evOwn("elsewise", "a1", KindRequest), 2)
	route(t, m, resp("a1", KindRequest, true), 0)
	id := m.AdoptFollow(tk)
	takeItem(t, m, id, 1)
	takeItem(t, m, id, 2)
	// No declared completion: the follow ends only by explicit close.
	takeState(t, m, id, TakeEmpty, 0)
	m.Close(id, 0)
	wantPanic(t, "unknown branch", func() { m.Take(id) })
}

func TestFollowCompletionWhileProvisional(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 8, Bytes: 4096}, nil, []string{"done"}))
	m.CommitWrite(tk)
	route(t, m, evOwn("progress", "a1", KindRequest), 1)
	route(t, m, evOwn("done", "a1", KindRequest), 2)
	// Adoption hands over the already-terminal branch.
	route(t, m, resp("a1", KindRequest, true), 0)
	id := m.AdoptFollow(tk)
	takeItem(t, m, id, 1)
	takeItem(t, m, id, 2)
	takeState(t, m, id, TakeEOF, 0)
}

func TestLaggedWinsOverCompletion(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 1, Bytes: 4096}, nil, []string{"done"}))
	m.CommitWrite(tk)
	route(t, m, resp("a1", KindRequest, true), 0)
	id := m.AdoptFollow(tk)

	route(t, m, evOwn("progress", "a1", KindRequest), 1)
	// The terminal event cannot reserve: ErrLagged wins and nothing is
	// silently dropped behind a clean end-of-stream.
	fx := route(t, m, evOwn("done", "a1", KindRequest), 2)
	if !woken(fx, id) {
		t.Fatalf("victim not woken")
	}
	takeState(t, m, id, TakeTerminal, ReasonLagged)
	wantAggregates(t, m, 0, 0)
}

func TestFollowLagMidStream(t *testing.T) {
	m := newMachine(t)
	sub := subscribe(t, m)
	tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 1, Bytes: 4096}, nil, nil))
	m.CommitWrite(tk)
	route(t, m, resp("a1", KindRequest, true), 0)
	id := m.AdoptFollow(tk)

	route(t, m, evOwn("x", "a1", KindRequest), 1)
	route(t, m, evOwn("x", "a1", KindRequest), 2) // follow lags at its boundary
	takeState(t, m, id, TakeTerminal, ReasonLagged)

	// The lagging follow never delayed ordinary delivery.
	takeItem(t, m, sub, 1)
	takeItem(t, m, sub, 2)
}

func TestCloseFollowReleasesEverything(t *testing.T) {
	m := newMachine(t)
	tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 8, Bytes: 4096}, nil, nil))
	m.CommitWrite(tk)
	route(t, m, evOwn("x", "a1", KindRequest), 1)
	route(t, m, resp("a1", KindRequest, false), 0) // AMI rejected: Do returns non-nil
	m.CloseFollow(tk)
	wantAggregates(t, m, 0, 0)
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after CloseFollow")
	}
	_, follows, _ := m.Branches()
	if follows != 0 {
		t.Fatalf("%d follow entries retained after CloseFollow", follows)
	}
}

func TestAbandonQuarantinesCorrelatedEvents(t *testing.T) {
	m := newMachine(t)
	sub := subscribe(t, m)
	tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 8, Bytes: 4096}, nil, nil))
	m.CommitWrite(tk)
	route(t, m, evOwn("x", "a1", KindRequest), 1)
	takeItem(t, m, sub, 1) // delivered ordinarily while active

	m.Abandon(tk, 50)
	wantRetirement(t, m, 0, 1)
	if m.Tickets() != 0 {
		t.Fatalf("abandon left the ticket unresolved")
	}
	wantAggregates(t, m, 0, 0) // the provisional follow queue was released

	// Quarantine: absorbed and counted, never ordinary delivery.
	before := m.Counters().Quarantined
	route(t, m, evOwn("x", "a1", KindRequest), 2)
	takeState(t, m, sub, TakeEmpty, 0)
	if got := m.Counters().Quarantined; got != before+1 {
		t.Fatalf("quarantined counter %d, want %d", got, before+1)
	}

	// The late response is the record's evidence: absorbed, never
	// delivered, and the record releases.
	fx := route(t, m, resp("a1", KindRequest, true), 99)
	if len(fx.Complete) != 0 {
		t.Fatalf("absorbed response delivered to a waiter: %+v", fx.Complete)
	}
	wantRetirement(t, m, 0, 0)

	// After release the ActionID's events degrade to ordinary fan-out.
	route(t, m, evOwn("x", "a1", KindRequest), 3)
	takeItem(t, m, sub, 3)

	// And its duplicate response is unknown: fatal.
	routeFatal(t, m, resp("a1", KindRequest, true), 0, ReasonResponseUnmatched)
}

func TestAdoptFollowDiscipline(t *testing.T) {
	t.Run("without a follow", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, AdmitOptions[int]{})
		wantPanic(t, "adopting a follow the ticket does not hold", func() { m.AdoptFollow(tk) })
	})
	t.Run("twice", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 1, Bytes: 1}, nil, nil))
		m.CommitWrite(tk)
		route(t, m, resp("a1", KindRequest, true), 0)
		m.AdoptFollow(tk)
		wantPanic(t, "unknown ticket", func() { m.AdoptFollow(tk) })
	})
	t.Run("take before adoption", func(t *testing.T) {
		m := newMachine(t)
		tk := admit(t, m, "a1", KindRequest, followOpts(Caps{Items: 4, Bytes: 512}, nil, nil))
		_ = tk
		var provisional BranchID
		// The provisional branch ID is internal; reconstruct it the
		// hard way through adoption on a second machine is overkill —
		// instead assert via the first allocated branch ID.
		provisional = BranchID{n: 1}
		wantPanic(t, "unadopted follow", func() { m.Take(provisional) })
	})
}
