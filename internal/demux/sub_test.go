package demux

import "testing"

func TestSubscribeValidation(t *testing.T) {
	m := newMachine(t, func(l *Limits) { l.MaxSubscriptions = 1 })

	if _, err := m.Subscribe(Matcher{}, Caps{}); err == nil {
		t.Fatalf("zero caps accepted")
	} else {
		wantErr(t, err, ErrInvalidOptions)
	}
	if _, err := m.Subscribe(Matcher{Events: []string{""}}, Caps{Items: 1, Bytes: 1}); err == nil {
		t.Fatalf("empty event name accepted")
	} else {
		wantErr(t, err, ErrInvalidOptions)
	}
	tight := newMachine(t, func(l *Limits) { l.MaxMatcherBytes = 8 })
	if _, err := tight.Subscribe(Matcher{Events: []string{"ninebytes"}}, Caps{Items: 1, Bytes: 1}); err == nil {
		t.Fatalf("oversized matcher accepted")
	} else {
		wantErr(t, err, ErrMatcherLimit)
	}

	subscribe(t, m)
	if _, err := m.Subscribe(Matcher{}, Caps{Items: 1, Bytes: 1}); err == nil {
		t.Fatalf("registration beyond MaxSubscriptions accepted")
	} else {
		wantErr(t, err, ErrSubscriptionLimit)
	}

	m.Kill(ReasonKilled)
	if _, err := m.Subscribe(Matcher{}, Caps{Items: 1, Bytes: 1}); err == nil {
		t.Fatalf("registration accepted on a dead machine")
	} else {
		wantErr(t, err, ErrDead)
	}
}

func TestFanoutMatching(t *testing.T) {
	m := newMachine(t)
	all := subscribe(t, m)                  // empty matcher: everything
	named := subscribe(t, m, "AgentCalled") // folded at registration
	other := subscribe(t, m, "peerstatus")  // must stay empty
	fx := route(t, m, ev("agentcalled"), 1) // names arrive pre-folded
	if !woken(fx, all) || !woken(fx, named) || woken(fx, other) {
		t.Fatalf("wake set %v: want all and named, not other", fx.Wake)
	}
	takeItem(t, m, all, 1)
	takeItem(t, m, named, 1)
	takeState(t, m, other, TakeEmpty, 0)

	// An event matching nothing is dropped at zero queue cost, counted.
	before := m.Counters().Unmatched
	fx = route(t, m, ev("zzz"), 2)
	if len(fx.Wake) != 1 { // only the match-all subscription
		t.Fatalf("wake set %v for a nearly-unmatched event", fx.Wake)
	}
	m.Close(all, 0)
	fx = route(t, m, ev("zzz"), 3)
	if len(fx.Wake) != 0 {
		t.Fatalf("wake set %v for an unmatched event", fx.Wake)
	}
	if got := m.Counters().Unmatched; got != before+1 {
		t.Fatalf("unmatched counter %d, want %d", got, before+1)
	}
}

func TestTakeAndCharges(t *testing.T) {
	m := newMachine(t)
	id := subscribe(t, m)
	takeState(t, m, id, TakeEmpty, 0)
	route(t, m, sized(ev("a"), 30), 1)
	route(t, m, sized(ev("b"), 20), 2)
	wantAggregates(t, m, 50, 0)
	takeItem(t, m, id, 1) // wire order
	wantAggregates(t, m, 20, 0)
	takeItem(t, m, id, 2)
	wantAggregates(t, m, 0, 0)
	takeState(t, m, id, TakeEmpty, 0)
}

func TestSubscriptionItemBoundary(t *testing.T) {
	m := newMachine(t)
	id, err := m.Subscribe(Matcher{}, Caps{Items: 2, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	route(t, m, ev("a"), 1)
	route(t, m, ev("a"), 2) // exactly at the limit
	fx := route(t, m, ev("a"), 3)
	if !woken(fx, id) {
		t.Fatalf("victim not woken on its terminal")
	}
	takeState(t, m, id, TakeTerminal, ReasonLagged)
	wantAggregates(t, m, 0, 0) // queued events discarded, charges released
}

func TestSubscriptionByteBoundary(t *testing.T) {
	m := newMachine(t)
	id, err := m.Subscribe(Matcher{}, Caps{Items: 16, Bytes: 20})
	if err != nil {
		t.Fatal(err)
	}
	route(t, m, sized(ev("a"), 10), 1)
	route(t, m, sized(ev("a"), 10), 2) // exactly at the byte limit
	takeItem(t, m, id, 1)
	route(t, m, sized(ev("a"), 10), 3) // freed capacity is usable again
	route(t, m, sized(ev("a"), 11), 4) // 10+11 > 20: lagged
	takeState(t, m, id, TakeTerminal, ReasonLagged)
}

func TestAggregateBoundaryFreesForLaterRecipients(t *testing.T) {
	m := newMachine(t, func(l *Limits) { l.MaxSubscriptionBytes = 25 })
	first := subscribe(t, m)
	second := subscribe(t, m)
	route(t, m, sized(ev("a"), 10), 1) // both take it: aggregate 20
	wantAggregates(t, m, 20, 0)

	// The next 10-byte event overflows the aggregate at the first
	// recipient; its terminal releases 10 bytes, so the second
	// recipient reserves within the freed capacity and stays healthy.
	fx := route(t, m, sized(ev("a"), 10), 2)
	takeState(t, m, first, TakeTerminal, ReasonLagged)
	if !woken(fx, second) {
		t.Fatalf("healthy recipient missed the event after the victim's release")
	}
	takeItem(t, m, second, 1)
	takeItem(t, m, second, 2)
	wantAggregates(t, m, 0, 0)
}

func TestLaggardIsolation(t *testing.T) {
	m := newMachine(t)
	a := subscribe(t, m)
	b, err := m.Subscribe(Matcher{}, Caps{Items: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	c := subscribe(t, m)

	route(t, m, ev("x"), 1)
	route(t, m, ev("x"), 2) // b lags at its exact boundary
	route(t, m, ev("x"), 3) // a and c continue in registration order

	takeState(t, m, b, TakeTerminal, ReasonLagged)
	for _, id := range []BranchID{a, c} {
		takeItem(t, m, id, 1)
		takeItem(t, m, id, 2)
		takeItem(t, m, id, 3)
		takeState(t, m, id, TakeEmpty, 0)
	}
}

func TestCloseSubscription(t *testing.T) {
	m := newMachine(t)
	id := subscribe(t, m)
	route(t, m, ev("a"), 1)
	m.Close(id, 0)
	wantAggregates(t, m, 0, 0)
	m.Close(id, 0) // idempotent: already released
	subs, _, _ := m.Branches()
	if subs != 0 {
		t.Fatalf("%d subscription entries retained after Close", subs)
	}
	// Events after close no longer match.
	fx := route(t, m, ev("a"), 2)
	if len(fx.Wake) != 0 {
		t.Fatalf("closed subscription still woken")
	}
}

func TestCloseAfterTerminalReleasesEntry(t *testing.T) {
	m := newMachine(t)
	id, err := m.Subscribe(Matcher{}, Caps{Items: 1, Bytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	route(t, m, ev("a"), 1)
	route(t, m, ev("a"), 2) // lagged
	takeState(t, m, id, TakeTerminal, ReasonLagged)
	m.Close(id, 0) // acknowledgment: removes the terminal bookkeeping
	subs, _, _ := m.Branches()
	if subs != 0 {
		t.Fatalf("%d subscription entries retained after terminal Close", subs)
	}
}

func TestTerminalProbe(t *testing.T) {
	m := New[int](testLimits())
	if reason, terminal := m.Terminal(BranchID{}); !terminal || reason != ReasonClosed {
		t.Fatalf("Terminal(unknown) = (%v, %v), want committed ReasonClosed", reason, terminal)
	}
	id, err := m.Subscribe(Matcher{}, Caps{Items: 1, Bytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, terminal := m.Terminal(id); terminal {
		t.Fatal("active subscription probed terminal")
	}
	// Overflow commits the lag terminal without consuming the queue.
	m.Route(ev("a"), 1)
	m.Route(ev("b"), 2)
	if reason, terminal := m.Terminal(id); !terminal || reason != ReasonLagged {
		t.Fatalf("Terminal(lagged) = (%v, %v), want ReasonLagged", reason, terminal)
	}
	// The probe consumed nothing: Take still reports the terminal.
	if _, res := m.Take(id); res.State != TakeTerminal || res.Reason != ReasonLagged {
		t.Fatalf("Take after probe = %+v", res)
	}
	m.Close(id, 0)
	if reason, terminal := m.Terminal(id); !terminal || reason != ReasonClosed {
		t.Fatalf("Terminal(closed) = (%v, %v), want committed ReasonClosed", reason, terminal)
	}
}
