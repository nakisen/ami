package demux

import "testing"

// startList admits a list, commits its write, and returns the ticket.
func startList(t *testing.T, m *Machine[int], id string, o *ListOptions[int]) Ticket {
	t.Helper()
	tk := admit(t, m, id, KindList, AdmitOptions[int]{List: o})
	m.CommitWrite(tk)
	return tk
}

func TestListHappyPath(t *testing.T) {
	m := newMachine(t)
	// A subscription matching the item names must never see
	// list-correlated traffic.
	sub := subscribe(t, m, "peerentry", "peerlistcomplete")

	tk := startList(t, m, "l1", listOpts("PeerlistComplete"))
	wantRetirement(t, m, 1, 0)

	// Items tolerated before the initial response.
	route(t, m, evOwn("peerentry", "l1", KindList), 1)

	fx := route(t, m, resp("l1", KindList, true), 100)
	c := completed(t, fx, tk)
	if !c.Delivered || c.Response != 100 {
		t.Fatalf("list response not delivered: %+v", c)
	}
	id := m.AdoptList(tk)
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after adoption")
	}

	route(t, m, evOwn("peerentry", "l1", KindList), 2)
	route(t, m, evOwn("peerentry", "l1", KindList), 3)

	// Completion by declared name; the payload doubles as the declared
	// count and matches the three observed items.
	fx = route(t, m, evOwn("peerlistcomplete", "l1", KindList), 3)
	if !woken(fx, id) {
		t.Fatalf("list not woken on completion")
	}
	wantRetirement(t, m, 0, 0) // response and mark both seen: slot released

	takeItem(t, m, id, 1)
	takeItem(t, m, id, 2)
	takeItem(t, m, id, 3)
	takeState(t, m, id, TakeEOF, 0)
	if got, ok := m.ListCompletion(id); !ok || got != 3 {
		t.Fatalf("ListCompletion = (%d, %t), want (3, true)", got, ok)
	}

	// Exclusivity: the subscription saw nothing.
	takeState(t, m, sub, TakeEmpty, 0)

	// Late correlated traffic discards silently, forever.
	before := m.Counters().LateListDiscards
	route(t, m, evOwn("peerentry", "l1", KindList), 9)
	if got := m.Counters().LateListDiscards; got != before+1 {
		t.Fatalf("late-list counter %d, want %d", got, before+1)
	}
	takeState(t, m, sub, TakeEmpty, 0)

	m.Close(id, 0)
	wantAggregates(t, m, 0, 0)
}

func TestListEventListHeaderCompletion(t *testing.T) {
	m := newMachine(t)
	// Empty Completions: the pure header convention.
	o := listOpts()
	o.Count = nil
	tk := startList(t, m, "l1", o)
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evMark("itemscomplete", "l1", MarkComplete), 2)
	takeItem(t, m, id, 1)
	takeState(t, m, id, TakeEOF, 0)
	if got, ok := m.ListCompletion(id); !ok || got != 2 {
		t.Fatalf("ListCompletion = (%d, %t), want (2, true)", got, ok)
	}
}

// TestListOverflowOnTerminalMarkKeepsEvidence pins the routing order:
// when the event carrying the terminal mark itself overflows the
// observed budget, the mark still counts as slot evidence, so a fully
// evidenced list settles outright instead of leaving a drain record
// waiting for a second mark — one that could only expire fatally and
// kill a healthy client over a single list's overflow.
func TestListOverflowOnTerminalMarkKeepsEvidence(t *testing.T) {
	m := newMachine(t)
	o := listOpts()
	o.Count = nil
	o.ObservedBytes = 25 // two 10-byte items fit; the third event overflows
	tk := startList(t, m, "l1", o)
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evOwn("item", "l1", KindList), 2)

	// The completion mark itself tips the observed budget: the list
	// fails with overflow, but the mark has been seen.
	fx := route(t, m, evMark("done", "l1", MarkComplete), 3)
	if !woken(fx, id) {
		t.Fatal("failed list not woken")
	}
	takeState(t, m, id, TakeTerminal, ReasonOverflow)

	// Response and mark are both in evidence: no drain record remains
	// and nothing is armed to expire.
	wantRetirement(t, m, 0, 0)
	if _, ok := m.NextDeadline(); ok {
		t.Fatal("a drain record survived a fully evidenced overflow")
	}
	m.Close(id, 0)
}

// TestListOverflowOnEarlyTerminalMarkAwaitsOnlyResponse is the
// pre-response variant: the overflowing mark is evidence, so the drain
// record waits only for the response and releases with it.
func TestListOverflowOnEarlyTerminalMarkAwaitsOnlyResponse(t *testing.T) {
	m := newMachine(t)
	o := listOpts()
	o.Count = nil
	o.ObservedBytes = 15 // the second event overflows
	tk := startList(t, m, "l1", o)
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evMark("done", "l1", MarkComplete), 2) // overflows; mark is evidence
	wantRetirement(t, m, 0, 1)                         // the record awaits the response only

	route(t, m, resp("l1", KindList, true), 0)
	wantRetirement(t, m, 0, 0) // released; nothing can expire
	id := m.AdoptList(tk)
	takeState(t, m, id, TakeTerminal, ReasonOverflow)
	m.Close(id, 0)
}

// TestListCloseDatesDrainRecordAtCloseTime pins the drain deadline to
// the Close call, not to the last routed message: after a long idle
// stretch the stale clock would otherwise produce an already-expired
// record whose only outcome is a fatal expiry.
func TestListCloseDatesDrainRecordAtCloseTime(t *testing.T) {
	m := newMachine(t)
	life := testLimits().RetirementLifetime
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, at(resp("l1", KindList, true), 100), 0)
	id := m.AdoptList(tk)

	// The clock last advanced at 100; the local Close happens much
	// later, on an otherwise idle session.
	m.Close(id, 5000)
	wantRetirement(t, m, 0, 1)
	if dl, ok := m.NextDeadline(); !ok || dl != 5000+life {
		t.Fatalf("NextDeadline = (%d, %t), want (%d, true): the record must be dated at Close", dl, ok, 5000+life)
	}
	// What would have been the stale deadline passes without incident.
	if fx := m.Expire(100 + life); fx.Fatal != nil {
		t.Fatal("drain record expired on the stale pre-Close clock")
	}
}

func TestListEarlyCompletionBeforeResponse(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evOwn("item", "l1", KindList), 2)
	route(t, m, evOwn("done", "l1", KindList), 2) // count matches
	wantRetirement(t, m, 1, 0)                    // mark seen, response outstanding

	// Post-mark stragglers before the response: absorbed and counted.
	before := m.Counters().Quarantined
	route(t, m, evOwn("item", "l1", KindList), 9)
	if got := m.Counters().Quarantined; got != before+1 {
		t.Fatalf("quarantined counter %d, want %d", got, before+1)
	}

	// The success response returns an already-complete handle.
	route(t, m, resp("l1", KindList, true), 0)
	wantRetirement(t, m, 0, 0)
	id := m.AdoptList(tk)
	takeItem(t, m, id, 1)
	takeItem(t, m, id, 2)
	takeState(t, m, id, TakeEOF, 0)
}

func TestListErrorResponseDiscardsBufferedState(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evOwn("item", "l1", KindList), 2)

	fx := route(t, m, resp("l1", KindList, false), 0)
	completed(t, fx, tk)
	wantAggregates(t, m, 0, 0)
	wantRetirement(t, m, 0, 0) // an Error response is evidence for both
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after an error response released the branch")
	}
	_, _, lists := m.Branches()
	if lists != 0 {
		t.Fatalf("%d list entries retained after the error response", lists)
	}

	// Stray later events fall to late-list discard.
	before := m.Counters().LateListDiscards
	route(t, m, evOwn("item", "l1", KindList), 3)
	if got := m.Counters().LateListDiscards; got != before+1 {
		t.Fatalf("late-list counter %d, want %d", got, before+1)
	}
}

func TestListBufferedCancelled(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evMark("cancelmark", "l1", MarkCancelled), 0)
	// The success response commits the buffered cancellation.
	route(t, m, resp("l1", KindList, true), 0)
	wantRetirement(t, m, 0, 0)
	id := m.AdoptList(tk)
	takeState(t, m, id, TakeTerminal, ReasonCancelled)
	wantAggregates(t, m, 0, 0)
}

// TestListMalformedCountFailsList pins the malformed-count verdict: a
// completion whose declared count cannot be used fails the list with
// its own reason instead of silently skipping the declared check and
// committing a clean snapshot.
func TestListMalformedCountFailsList(t *testing.T) {
	m := newMachine(t)
	o := listOpts("done")
	o.Count = func(v int) (int64, CountVerdict) {
		if v == 99 {
			return 0, CountMalformed
		}
		return 0, CountAbsent
	}
	tk := startList(t, m, "l1", o)
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)
	route(t, m, evOwn("item", "l1", KindList), 1)
	fx := route(t, m, evOwn("done", "l1", KindList), 99)
	if !woken(fx, id) {
		t.Fatal("failed list not woken")
	}
	takeState(t, m, id, TakeTerminal, ReasonCountMalformed)
	wantRetirement(t, m, 0, 0) // response and mark both in evidence
	m.Close(id, 0)
	wantAggregates(t, m, 0, 0)
}

func TestListCancelledMidStream(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)
	route(t, m, evOwn("item", "l1", KindList), 1)
	fx := route(t, m, evMark("cancelmark", "l1", MarkCancelled), 0)
	if !woken(fx, id) {
		t.Fatalf("list not woken on cancellation")
	}
	takeState(t, m, id, TakeTerminal, ReasonCancelled)
	wantAggregates(t, m, 0, 0) // queued items discarded at commit
	wantRetirement(t, m, 0, 0)
}

func TestListCountMismatch(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evOwn("item", "l1", KindList), 2)
	route(t, m, evOwn("done", "l1", KindList), 5) // claims five, observed two
	takeState(t, m, id, TakeTerminal, ReasonCountMismatch)
	wantAggregates(t, m, 0, 0)
	wantRetirement(t, m, 0, 0)
	if _, ok := m.ListCompletion(id); ok {
		t.Fatalf("failed list retained a completion event")
	}
}

func TestListCountAbsentOrUndeclared(t *testing.T) {
	t.Run("field absent", func(t *testing.T) {
		m := newMachine(t)
		tk := startList(t, m, "l1", listOpts("done"))
		route(t, m, resp("l1", KindList, true), 0)
		id := m.AdoptList(tk)
		route(t, m, evOwn("item", "l1", KindList), 1)
		route(t, m, evOwn("done", "l1", KindList), -1) // negative payload: no count present
		takeItem(t, m, id, 1)
		takeState(t, m, id, TakeEOF, 0)
	})
	t.Run("no count configured", func(t *testing.T) {
		m := newMachine(t)
		o := listOpts("done")
		o.Count = nil
		tk := startList(t, m, "l1", o)
		route(t, m, resp("l1", KindList, true), 0)
		id := m.AdoptList(tk)
		route(t, m, evOwn("done", "l1", KindList), 5)
		takeState(t, m, id, TakeEOF, 0)
	})
}

func TestListOverflowMidStream(t *testing.T) {
	m := newMachine(t)
	o := listOpts("done")
	o.Caps = Caps{Items: 2, Bytes: 4096}
	tk := startList(t, m, "l1", o)
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)

	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evOwn("item", "l1", KindList), 2)
	fx := route(t, m, evOwn("item", "l1", KindList), 3) // beyond Items
	if !woken(fx, id) {
		t.Fatalf("list not woken on overflow")
	}
	takeState(t, m, id, TakeTerminal, ReasonOverflow)
	wantAggregates(t, m, 0, 0)
	// The mark is outstanding: the reserved slot became a drain record.
	wantRetirement(t, m, 0, 1)

	// The remainder is absorbed until terminal evidence.
	before := m.Counters().Quarantined
	route(t, m, evOwn("item", "l1", KindList), 4)
	if got := m.Counters().Quarantined; got != before+1 {
		t.Fatalf("quarantined counter %d, want %d", got, before+1)
	}
	route(t, m, evMark("done", "l1", MarkComplete), 0)
	wantRetirement(t, m, 0, 0)

	// After release: permanent late-list discard.
	before = m.Counters().LateListDiscards
	route(t, m, evOwn("item", "l1", KindList), 5)
	if got := m.Counters().LateListDiscards; got != before+1 {
		t.Fatalf("late-list counter %d, want %d", got, before+1)
	}
}

func TestListOverflowBeforeResponse(t *testing.T) {
	m := newMachine(t)
	o := listOpts("done")
	o.Caps = Caps{Items: 1, Bytes: 4096}
	tk := startList(t, m, "l1", o)

	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evOwn("item", "l1", KindList), 2) // overflow while buffering
	wantRetirement(t, m, 0, 1)                    // record needs the response and the mark

	// The response still completes the pending, and is evidence for
	// the record.
	fx := route(t, m, resp("l1", KindList, true), 0)
	completed(t, fx, tk)
	wantRetirement(t, m, 0, 1) // mark still outstanding
	id := m.AdoptList(tk)
	takeState(t, m, id, TakeTerminal, ReasonOverflow)

	route(t, m, evMark("done", "l1", MarkComplete), 0)
	wantRetirement(t, m, 0, 0)
}

func TestListErrorResponseReleasesEarlierDrainRecord(t *testing.T) {
	m := newMachine(t)
	o := listOpts("done")
	o.Caps = Caps{Items: 1, Bytes: 4096}
	tk := startList(t, m, "l1", o)
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evOwn("item", "l1", KindList), 2) // pre-response overflow: drain record
	wantRetirement(t, m, 0, 1)

	// A rejected list never streams: the Error response is evidence
	// for both outstanding facts and the record releases immediately.
	// The branch never escapes on an error response, overflow or not.
	fx := route(t, m, resp("l1", KindList, false), 0)
	completed(t, fx, tk)
	wantRetirement(t, m, 0, 0)
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after the error response")
	}
	_, _, lists := m.Branches()
	if lists != 0 {
		t.Fatalf("%d list entries retained after the error response", lists)
	}
}

func TestListObservedBudget(t *testing.T) {
	m := newMachine(t)
	o := listOpts("done")
	o.ObservedBytes = 25
	tk := startList(t, m, "l1", o)
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)

	route(t, m, evOwn("item", "l1", KindList), 1) // observed 10
	takeItem(t, m, id, 1)                         // draining does not refund the budget
	route(t, m, evOwn("item", "l1", KindList), 2) // observed 20
	route(t, m, evOwn("item", "l1", KindList), 3) // observed 30 > 25
	takeState(t, m, id, TakeTerminal, ReasonOverflow)
	wantRetirement(t, m, 0, 1)
}

func TestListCompletionReserveFailure(t *testing.T) {
	m := newMachine(t)
	o := listOpts("done")
	o.Caps = Caps{Items: 16, Bytes: 25}
	tk := startList(t, m, "l1", o)
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)

	route(t, m, sized(evOwn("item", "l1", KindList), 10), 1)
	route(t, m, sized(evOwn("item", "l1", KindList), 10), 2)
	// The completion event cannot reserve its storage charge: the list
	// fails as overflow. Both evidence facts are in hand, so the slot
	// releases without a record.
	route(t, m, sized(evOwn("done", "l1", KindList), 10), 2)
	takeState(t, m, id, TakeTerminal, ReasonOverflow)
	wantRetirement(t, m, 0, 0)
	wantAggregates(t, m, 0, 0)
}

func TestListCloseWhileStreaming(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, resp("l1", KindList, true), 0)
	id := m.AdoptList(tk)
	// The routed envelope's timestamp feeds the logical clock that
	// dates the drain record Close creates.
	route(t, m, at(evOwn("item", "l1", KindList), 200), 1)

	m.Close(id, 0)
	wantAggregates(t, m, 0, 0)
	wantRetirement(t, m, 0, 1) // the remote may still stream: drain record
	if dl, ok := m.NextDeadline(); !ok || dl != 1200 {
		t.Fatalf("NextDeadline = (%d, %t), want (1200, true)", dl, ok)
	}

	// Remainder absorbed; the mark is the record's evidence.
	route(t, m, evOwn("item", "l1", KindList), 2)
	route(t, m, evOwn("done", "l1", KindList), 3)
	wantRetirement(t, m, 0, 0)
}

func TestListAbandon(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, evOwn("item", "l1", KindList), 1)

	m.Abandon(tk, 10)
	wantAggregates(t, m, 0, 0)
	wantRetirement(t, m, 0, 1)
	if m.Tickets() != 0 {
		t.Fatalf("abandon left the ticket unresolved")
	}

	// The response and the mark both resolve before release.
	route(t, m, resp("l1", KindList, true), 0)
	wantRetirement(t, m, 0, 1)
	route(t, m, evMark("done", "l1", MarkComplete), 0)
	wantRetirement(t, m, 0, 0)
}

func TestListAbandonAfterBufferedMark(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, evOwn("item", "l1", KindList), 1)
	route(t, m, evOwn("done", "l1", KindList), 1) // mark buffered pre-response

	m.Abandon(tk, 10)
	wantRetirement(t, m, 0, 1) // the response still needs a home

	// The late response is absorbed by the record, not fatal, and
	// releases it.
	fx := m.Route(resp("l1", KindList, true), 0)
	if fx.Fatal != nil {
		t.Fatalf("late response for an abandoned list was fatal: %v", fx.Fatal.Reason)
	}
	if len(fx.Complete) != 0 {
		t.Fatalf("absorbed response delivered to a waiter")
	}
	wantRetirement(t, m, 0, 0)
}

func TestListLimit(t *testing.T) {
	m := newMachine(t, func(l *Limits) { l.MaxLists = 1 })
	tk := startList(t, m, "l1", listOpts("done"))
	if _, err := m.Admit("l2", KindList, AdmitOptions[int]{List: listOpts("done")}); err == nil {
		t.Fatalf("admission beyond MaxLists accepted")
	} else {
		wantErr(t, err, ErrListLimit)
	}
	// Completion frees the routing-active slot even before Close.
	route(t, m, resp("l1", KindList, true), 0)
	route(t, m, evOwn("done", "l1", KindList), 0)
	_ = m.AdoptList(tk)
	admit(t, m, "l2", KindList, AdmitOptions[int]{List: listOpts("done")})
}

// TestQueueStatusInterleave is the wallboard scenario: a subscription
// matching queue events and a list whose correlated items carry the
// same event names, interleaved on the wire.
func TestQueueStatusInterleave(t *testing.T) {
	m := newMachine(t)
	sub := subscribe(t, m, "queuemember", "queueentry", "queuestatuscomplete")

	tk := startList(t, m, "q1", listOpts("QueueStatusComplete"))
	route(t, m, resp("q1", KindList, true), 0)
	id := m.AdoptList(tk)

	// Interleave: correlated items, uncorrelated live events, foreign
	// traffic.
	route(t, m, evOwn("queuemember", "q1", KindList), 1) // list only
	route(t, m, ev("queuemember"), 101)                  // live: subscription only
	route(t, m, evOwn("queueentry", "q1", KindList), 2)  // list only
	fx := m.Route(Envelope{Class: ClassEvent, Name: "queueentry", ActionID: "other-7", Size: 10}, 102)
	if fx.Fatal != nil {
		t.Fatalf("foreign-correlated event was fatal")
	}
	route(t, m, ev("queuemember"), 103)
	route(t, m, evOwn("queuemember", "q1", KindList), 3)

	// Completion with a verified declared count of 3.
	route(t, m, evOwn("queuestatuscomplete", "q1", KindList), 3)

	// The list saw exactly its correlated items, in wire order.
	takeItem(t, m, id, 1)
	takeItem(t, m, id, 2)
	takeItem(t, m, id, 3)
	takeState(t, m, id, TakeEOF, 0)
	if _, ok := m.ListCompletion(id); !ok {
		t.Fatalf("completion event not stored")
	}

	// The subscription saw exactly the live events, in wire order.
	takeItem(t, m, sub, 101)
	takeItem(t, m, sub, 102)
	takeItem(t, m, sub, 103)
	takeState(t, m, sub, TakeEmpty, 0)

	m.Close(id, 0)
	m.Close(sub, 0)
	wantAggregates(t, m, 0, 0)
	wantRetirement(t, m, 0, 0)
}

func TestCloseListAfterDeathCompletion(t *testing.T) {
	m := newMachine(t)
	tk := startList(t, m, "l1", listOpts("done"))
	route(t, m, evOwn("item", "l1", KindList), 1)
	fx := m.Kill(ReasonKilled)
	c := completed(t, fx, tk)
	if c.Delivered {
		t.Fatalf("death completion claims a delivered response")
	}
	m.CloseList(tk)
	if m.Tickets() != 0 {
		t.Fatalf("ticket retained after CloseList")
	}
	_, _, lists := m.Branches()
	if lists != 0 {
		t.Fatalf("%d list entries retained after CloseList", lists)
	}
}
