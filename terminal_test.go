package ami

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// TestTerminalCausesAreStableWithoutTheLock pins the contract the
// lock-free reads rest on: a cause observed through Err is the committed
// first winner, it is stable from the moment Done closes, and concurrent
// polling never observes an intermediate state. Under -race this also
// covers the publication itself, since the pollers run while the read loop
// commits.
func TestTerminalCausesAreStableWithoutTheLock(t *testing.T) {
	c, _ := dialTest(t, nil)
	sub, err := c.Subscribe(SubSpec{Events: []string{"Newchannel"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// While everything is healthy every accessor reports nil.
	if err := c.Err(); err != nil {
		t.Fatalf("Err() on a running client = %v, want nil", err)
	}
	if err := sub.Err(); err != nil {
		t.Fatalf("subscription Err() while active = %v, want nil", err)
	}

	// Poll all three accessors across the terminal transition. Any value
	// a poller sees must be either nil or the final cause; nothing else
	// may ever be observable.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	bad := make(chan error, 8)
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := c.Err(); err != nil && !errors.Is(err, ErrClosed) {
					bad <- err
					return
				}
				if err := sub.Err(); err != nil && !errors.Is(err, ErrClosed) {
					bad <- err
					return
				}
			}
		})
	}

	c.Close()
	<-c.Done()
	close(stop)
	wg.Wait()
	select {
	case err := <-bad:
		t.Fatalf("a poller observed %v, want only nil or the committed ErrClosed", err)
	default:
	}

	// Stable after Done, and repeated reads agree.
	for range 3 {
		if err := c.Err(); !errors.Is(err, ErrClosed) {
			t.Fatalf("Err() after Done = %v, want ErrClosed", err)
		}
		if err := sub.Err(); !errors.Is(err, ErrClosed) {
			t.Fatalf("subscription Err() after client death = %v, want ErrClosed", err)
		}
	}
}

// TestTerminalCauseFirstWinnerSurvivesLaterDeaths pins that the lock-free
// value is the first winner: a later terminal attempt cannot replace a
// committed cause, which is what makes an unlocked read safe to trust.
func TestTerminalCauseFirstWinnerSurvivesLaterDeaths(t *testing.T) {
	c, _ := dialTest(t, nil)
	first := errors.New("synthetic first cause")
	c.die(first)
	<-c.Done()
	if err := c.Err(); !errors.Is(err, first) {
		t.Fatalf("Err() = %v, want the first committed cause", err)
	}
	c.die(errors.New("synthetic later cause"))
	c.Close()
	if err := c.Err(); !errors.Is(err, first) {
		t.Fatalf("Err() after later deaths = %v, want the first cause preserved", err)
	}
}

// TestCleanTerminalReadsAsNilWithoutTheLock pins the one case where the
// published pointer stays nil on purpose: a clean terminal has no error,
// so Err must keep reporting nil while Done is closed and the queue
// drains to io.EOF.
func TestCleanTerminalReadsAsNilWithoutTheLock(t *testing.T) {
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
	defer list.Close()

	s.event("QueueMember", "ActionID", got.id, "Queue", "support")
	s.event("QueueStatusComplete", "ActionID", got.id, "EventList", "Complete", "ListItems", "1")
	waitDone(t, list.Done(), "completed list")
	if err := list.Err(); err != nil {
		t.Fatalf("Err() after clean completion = %v, want nil", err)
	}
	// The queued item still drains after the clean terminal, and Err stays
	// nil throughout.
	if _, err := list.Next(t.Context()); err != nil {
		t.Fatalf("Next() after clean completion = %v", err)
	}
	if err := list.Err(); err != nil {
		t.Fatalf("Err() after draining = %v, want nil", err)
	}
}
