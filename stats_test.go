package ami

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
)

func TestStatsCountersAndGauges(t *testing.T) {
	c, s := dialTest(t, nil)
	if got := c.Stats(); got != (Stats{}) {
		t.Fatalf("Stats() on a fresh client = %+v, want the zero value", got)
	}

	sub, err := c.Subscribe(SubSpec{Events: []string{"QueueMemberStatus"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	s.event("QueueMemberStatus", "Queue", "support")
	s.event("Newexten", "Context", "nobody-subscribed") // matches nothing
	s.event("VarSet", "Variable", "also-unmatched")
	s.sync(c)

	got := c.Stats()
	if got.Unmatched != 2 {
		t.Fatalf("Unmatched = %d, want the two events no subscription matched", got.Unmatched)
	}
	if got.Subscriptions != 1 {
		t.Fatalf("Subscriptions = %d, want 1", got.Subscriptions)
	}
	if got.Lists != 0 || got.Pending != 0 || got.Retirements != 0 {
		t.Fatalf("Stats() = %+v, want no lists, no in-flight actions, no retirements", got)
	}
	// The matched event is queued and charged; the unmatched ones cost
	// nothing, which is the accounting claim behind the unfiltered-flood
	// design.
	if got.QueuedSubscriptionBytes <= 0 {
		t.Fatalf("QueuedSubscriptionBytes = %d, want the queued event's charge", got.QueuedSubscriptionBytes)
	}
	if got.QueuedListBytes != 0 {
		t.Fatalf("QueuedListBytes = %d, want 0", got.QueuedListBytes)
	}

	// Draining releases the charge; the monotonic counter does not move
	// back.
	if _, err := sub.Next(t.Context()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	drained := c.Stats()
	if drained.QueuedSubscriptionBytes != 0 {
		t.Fatalf("QueuedSubscriptionBytes after draining = %d, want 0", drained.QueuedSubscriptionBytes)
	}
	if drained.Unmatched != got.Unmatched {
		t.Fatalf("Unmatched moved from %d to %d; it must be monotonic", got.Unmatched, drained.Unmatched)
	}
}

func TestStatsTracksListsAndLateDiscards(t *testing.T) {
	c, s := dialTest(t, nil)
	act, _ := NewAction("QueueStatus")
	spec, _ := ListSpecFor("QueueStatus")
	done := make(chan struct{})
	var list *List
	var listErr error
	go func() {
		defer close(done)
		list, listErr = c.StartList(context.Background(), act, spec)
	}()
	got := s.readAction()
	s.respond(got.id, "Success", "EventList", "start")
	<-done
	if listErr != nil {
		t.Fatalf("StartList() = %v", listErr)
	}

	s.event("QueueMember", "ActionID", got.id, "Queue", "support")
	s.sync(c)
	if st := c.Stats(); st.Lists != 1 || st.QueuedListBytes <= 0 {
		t.Fatalf("Stats() = %+v, want one list holding its queued item's charge", st)
	}

	s.event("QueueStatusComplete", "ActionID", got.id, "EventList", "Complete", "ListItems", "1")
	s.sync(c)
	list.Close()

	// Correlated traffic arriving for a list that no longer exists is
	// discarded permanently, and counted so the loss is never silent.
	s.event("QueueMember", "ActionID", got.id, "Queue", "too-late")
	s.sync(c)
	st := c.Stats()
	if st.LateListDiscards != 1 {
		t.Fatalf("LateListDiscards = %d, want the one late correlated event", st.LateListDiscards)
	}
	if st.Lists != 0 || st.QueuedListBytes != 0 {
		t.Fatalf("Stats() after Close = %+v, want the list released", st)
	}
}

// TestStatsAfterLagAndTermination pins the reason Stats exists: after
// ErrLagged an operator asks how close to the bound the consumer was, and
// after the client dies the accounting must still be readable to explain
// what the session did.
func TestStatsAfterLagAndTermination(t *testing.T) {
	c, s := dialTest(t, func(cfg *Config) {
		cfg.Limits.SubscriptionQueueItems = 2
	})
	sub, err := c.Subscribe(SubSpec{Events: []string{"Newchannel"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for range 3 {
		s.event("Newchannel", "Channel", "PJSIP/synthetic-0001")
	}
	s.sync(c)
	waitDone(t, sub.Done(), "lagged subscription")
	if err := sub.Err(); !errors.Is(err, ErrLagged) {
		t.Fatalf("Err() = %v, want ErrLagged", err)
	}
	// Overflow discarded the queue and released its charge, while the
	// terminal branch is still held until its handle closes.
	lagged := c.Stats()
	if lagged.QueuedSubscriptionBytes != 0 {
		t.Fatalf("QueuedSubscriptionBytes after ErrLagged = %d, want the queue released", lagged.QueuedSubscriptionBytes)
	}
	if lagged.Subscriptions != 1 {
		t.Fatalf("Subscriptions after ErrLagged = %d, want the unclosed handle still counted", lagged.Subscriptions)
	}

	c.Close()
	<-c.Done()
	// A terminated client still answers, which is the point: the gauges
	// report what survived termination. A branch's bookkeeping survives
	// until its handle is closed — the machine retains a terminal branch
	// so its committed result stays observable — so the unclosed handle is
	// still counted here.
	after := c.Stats()
	if after.Subscriptions != 1 || after.Lists != 0 || after.Pending != 0 {
		t.Fatalf("Stats() after termination = %+v, want the unclosed handle still counted", after)
	}
	if after.QueuedSubscriptionBytes != 0 || after.QueuedListBytes != 0 {
		t.Fatalf("Stats() after termination = %+v, want every queue charge released", after)
	}
	sub.Close()
	if released := c.Stats(); released.Subscriptions != 0 {
		t.Fatalf("Subscriptions after closing the handle = %d, want 0", released.Subscriptions)
	}
}

func TestStatsDiagDrops(t *testing.T) {
	// The silent default queues nothing, so nothing can be dropped and the
	// nil diagnostics instance must not be dereferenced.
	c, _ := dialTest(t, nil)
	if got := c.Stats().DiagDrops; got != 0 {
		t.Fatalf("DiagDrops without a Logger = %d, want 0", got)
	}

	var buf bytes.Buffer
	logged, _ := dialTest(t, func(cfg *Config) {
		cfg.Logger = slog.New(slog.NewTextHandler(&buf, nil))
	})
	if got := logged.Stats().DiagDrops; got != 0 {
		t.Fatalf("DiagDrops on a quiet session = %d, want 0", got)
	}
}
