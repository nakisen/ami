package ami

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nakisen/ami/internal/demux"
)

func TestDoSuccess(t *testing.T) {
	c, s := dialTest(t, nil)
	res := mustDo(t, c, s, "CoreStatus", "CoreStartupTime", "synthetic")
	if res.Response.Get("CoreStartupTime") != "synthetic" {
		t.Fatalf("response fields lost: %v", res.Response)
	}
	if res.ActionID == "" || res.Follow != nil {
		t.Fatalf("DoResult = %+v, want an ActionID and no follow", res)
	}
}

func TestDoDistinctActionIDs(t *testing.T) {
	c, s := dialTest(t, nil)
	a := mustDo(t, c, s, "Ping")
	b := mustDo(t, c, s, "Ping")
	if a.ActionID == b.ActionID {
		t.Fatalf("two dispatches shared ActionID %q", a.ActionID)
	}
}

func TestDoErrorResponse(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan error, 1)
	go func() {
		act, _ := NewAction("Originate")
		_, err := c.Do(context.Background(), act)
		done <- err
	}()
	act := s.readAction()
	s.respond(act.id, "Error", "Message", "Permission denied")
	err := <-done
	var re *ResponseError
	if !errors.As(err, &re) {
		t.Fatalf("Do() = %v, want *ResponseError", err)
	}
	if re.Response().Get("Message") != "Permission denied" {
		t.Fatalf("raw response lost: %v", re.Response())
	}
	if strings.Contains(err.Error(), "Permission") {
		t.Fatalf("error text embeds the remote Message: %q", err.Error())
	}
	// The client survives an AMI-level rejection.
	mustDo(t, c, s, "Ping")
}

func TestDoContextCancelAwaitingResponse(t *testing.T) {
	c, s := dialTest(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		act, _ := NewAction("Originate")
		_, err := c.Do(ctx, act)
		done <- err
	}()
	act := s.readAction() // fully written
	cancel()
	err := <-done
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseResponse || !re.MayHaveExecuted() {
		t.Fatalf("Do() = %v, want outcome-unknown RequestError", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() = %v, want the context cause wrapped", err)
	}

	// The late response is absorbed by the retirement record: the
	// client stays alive and later dispatches work.
	s.respond(act.id, "Success")
	mustDo(t, c, s, "Ping")
}

func TestDoPendingLimit(t *testing.T) {
	c, s := dialTest(t, func(cfg *Config) {
		cfg.Limits.MaxPending = 1
	})
	first := make(chan error, 1)
	go func() {
		act, _ := NewAction("SlowThing")
		_, err := c.Do(context.Background(), act)
		first <- err
	}()
	slow := s.readAction() // action 1 in flight, occupying the only slot

	act, _ := NewAction("Ping")
	_, err := c.Do(context.Background(), act)
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseAdmission || re.MayHaveExecuted() {
		t.Fatalf("Do() over the pending limit = %v, want not-sent admission RequestError", err)
	}

	s.respond(slow.id, "Success")
	if err := <-first; err != nil {
		t.Fatalf("first Do() = %v", err)
	}
	mustDo(t, c, s, "Ping") // the slot is free again
}

// gatedConn blocks writes behind a gate once armed, signaling entry, so
// a test can hold a dispatch provably inside its socket write — and
// therefore provably owning the writer.
type gatedConn struct {
	net.Conn
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	gate    chan struct{}
}

func (g *gatedConn) Write(p []byte) (int, error) {
	if g.armed.Load() {
		g.once.Do(func() { close(g.entered) })
		<-g.gate
	}
	return g.Conn.Write(p)
}

func TestDoWriteAdmissionTimeout(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	g := &gatedConn{Conn: clientEnd, entered: make(chan struct{}), gate: make(chan struct{})}
	cfg := testConfig(g)
	cfg.Limits.WriteAdmission = 50 * time.Millisecond
	s := newScript(t, serverEnd)
	handshake := make(chan struct{})
	go func() {
		defer close(handshake)
		s.serveLogin()
	}()
	c, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	<-handshake
	t.Cleanup(func() {
		c.Close()
		<-c.Done()
		serverEnd.Close()
	})

	// Hold one dispatch inside its write: it provably owns the writer.
	g.armed.Store(true)
	blocked := make(chan error, 1)
	go func() {
		act, _ := NewAction("Stuck")
		_, err := c.Do(context.Background(), act)
		blocked <- err
	}()
	<-g.entered

	act, _ := NewAction("Ping")
	_, err = c.Do(context.Background(), act)
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseAdmission || re.MayHaveExecuted() {
		t.Fatalf("Do() = %v, want not-sent admission RequestError", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() = %v: the WriteAdmission bound must not masquerade as the caller's context", err)
	}

	// Release the held writer and complete it normally.
	g.armed.Store(false)
	close(g.gate)
	stuck := s.readAction()
	s.respond(stuck.id, "Success")
	if err := <-blocked; err != nil {
		t.Fatalf("released Do() = %v", err)
	}
}

func TestSubscribeDeliveryAndFiltering(t *testing.T) {
	c, s := dialTest(t, nil)
	sub, err := c.Subscribe(MatchEvents("QueueMemberStatus"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	s.event("QueueMemberStatus", "Queue", "synthetic-1")
	s.event("Newexten", "Context", "irrelevant")
	s.event("queuememberstatus", "Queue", "synthetic-2") // case-insensitive match

	ev, err := sub.Next(context.Background())
	if err != nil || ev.Name() != "QueueMemberStatus" || ev.Get("Queue") != "synthetic-1" {
		t.Fatalf("Next() = (%v, %v)", ev, err)
	}
	ev, err = sub.Next(context.Background())
	if err != nil || ev.Get("Queue") != "synthetic-2" {
		t.Fatalf("Next() skipped the unmatched event wrongly: (%v, %v)", ev, err)
	}
}

func TestSubscribeNextContextCancelKeepsSubscription(t *testing.T) {
	c, s := dialTest(t, nil)
	sub, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(canceled) = %v", err)
	}
	// Still registered and delivering.
	s.event("FullyBooted")
	ev, err := sub.Next(context.Background())
	if err != nil || ev.Name() != "FullyBooted" {
		t.Fatalf("Next() after canceled wait = (%v, %v)", ev, err)
	}
}

func TestSubscriptionLag(t *testing.T) {
	c, s := dialTest(t, nil)
	sub, err := c.Subscribe(Buffer(2))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	healthy, err := c.Subscribe(MatchEvents("Newchannel"))
	if err != nil {
		t.Fatal(err)
	}
	defer healthy.Close()

	for range 3 {
		s.event("Newchannel", "Channel", "PJSIP/synthetic-0001")
	}
	s.sync(c)

	waitDone(t, sub.Done(), "lagged subscription")
	if err := sub.Err(); !errors.Is(err, ErrLagged) {
		t.Fatalf("Err() = %v, want ErrLagged", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, ErrLagged) {
		t.Fatalf("Next() = %v, want ErrLagged", err)
	}
	// The laggard was isolated: the healthy subscription got all three
	// events and the client is alive.
	for i := range 3 {
		if _, err := healthy.Next(context.Background()); err != nil {
			t.Fatalf("healthy Next() %d = %v", i, err)
		}
	}
}

func TestSubscriptionCloseDiscards(t *testing.T) {
	c, s := dialTest(t, nil)
	sub, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	s.event("Newchannel")
	s.sync(c)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	waitDone(t, sub.Done(), "closed subscription")
	if err := sub.Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Err() = %v, want ErrClosed", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next() after Close = %v, want ErrClosed", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
}

func TestConsume(t *testing.T) {
	c, s := dialTest(t, nil)
	sub, err := c.Subscribe(MatchEvents("Newstate"))
	if err != nil {
		t.Fatal(err)
	}
	s.event("Newstate", "N", "1")
	s.event("Newstate", "N", "2")
	stop := errors.New("done consuming")
	var got []string
	err = sub.Consume(context.Background(), func(ev Event) error {
		got = append(got, ev.Get("N"))
		if len(got) == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Consume() = %v, want the handler error", err)
	}
	if len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Fatalf("consumed %v, want ordered delivery", got)
	}
	// Consume closed the subscription on exit and is single-use.
	if err := sub.Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Err() after Consume = %v, want ErrClosed", err)
	}
	if err := sub.Consume(context.Background(), func(Event) error { return nil }); err != errAdapterUsed {
		t.Fatalf("second Consume() = %v, want the single-use rejection", err)
	}
}

func TestAllClosesOnBreak(t *testing.T) {
	c, s := dialTest(t, nil)
	sub, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	s.event("Newchannel")
	for ev, err := range sub.All(context.Background()) {
		if err != nil || ev.Name() != "Newchannel" {
			t.Fatalf("All yielded (%v, %v)", ev, err)
		}
		break
	}
	if err := sub.Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Err() after breaking All = %v, want ErrClosed", err)
	}
	for _, err := range sub.All(context.Background()) {
		if err != errAdapterUsed {
			t.Fatalf("second All yielded %v, want the single-use rejection", err)
		}
	}
}

// TestAdaptersHoldConsumerLease pins the ownership contract: inside an
// All yield or a Consume handler, the adapter still owns the consumer
// slot, so a concurrent bare Next is rejected rather than stealing the
// next queued event.
func TestAdaptersHoldConsumerLease(t *testing.T) {
	t.Run("subscription all", func(t *testing.T) {
		c, s := dialTest(t, nil)
		sub, err := c.Subscribe(MatchEvents("Newchannel"))
		if err != nil {
			t.Fatal(err)
		}
		s.event("Newchannel", "N", "1")
		s.event("Newchannel", "N", "2")
		s.sync(c)
		var got []string
		for ev, err := range sub.All(context.Background()) {
			if err != nil {
				t.Fatalf("All yielded error %v", err)
			}
			if _, nerr := sub.Next(context.Background()); nerr != errConcurrentConsumer {
				t.Fatalf("Next inside All = %v, want the consumer-discipline rejection", nerr)
			}
			got = append(got, ev.Get("N"))
			if len(got) == 2 {
				break
			}
		}
		if len(got) != 2 || got[0] != "1" || got[1] != "2" {
			t.Fatalf("All delivered %v, want both events in order", got)
		}
	})

	t.Run("subscription consume", func(t *testing.T) {
		c, s := dialTest(t, nil)
		sub, err := c.Subscribe(MatchEvents("Newchannel"))
		if err != nil {
			t.Fatal(err)
		}
		s.event("Newchannel")
		stop := errors.New("stop")
		err = sub.Consume(context.Background(), func(Event) error {
			if _, nerr := sub.Next(context.Background()); nerr != errConcurrentConsumer {
				t.Fatalf("Next inside Consume = %v, want the consumer-discipline rejection", nerr)
			}
			return stop
		})
		if !errors.Is(err, stop) {
			t.Fatalf("Consume() = %v, want the handler error", err)
		}
	})

	t.Run("list all", func(t *testing.T) {
		c, s := dialTest(t, nil)
		started := make(chan struct{})
		var list *List
		go func() {
			defer close(started)
			act, _ := NewAction("QueueStatus")
			list, _ = c.StartList(context.Background(), act, ListSpec{})
		}()
		act := s.readAction()
		s.respond(act.id, "Success")
		<-started
		s.event("QueueMember", "ActionID", act.id, "Queue", "q1")
		s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete")
		var items int
		for _, err := range list.All(context.Background()) {
			if err != nil {
				t.Fatalf("All yielded error %v", err)
			}
			if _, nerr := list.Next(context.Background()); nerr != errConcurrentConsumer {
				t.Fatalf("Next inside list All = %v, want the consumer-discipline rejection", nerr)
			}
			items++
		}
		if items != 1 {
			t.Fatalf("All delivered %d items, want 1", items)
		}
	})
}

func TestDoWithFollow(t *testing.T) {
	c, s := dialTest(t, nil)
	watcher, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan struct{})
	var res DoResult
	var doErr error
	go func() {
		defer close(done)
		act, _ := NewAction("Originate")
		res, doErr = c.Do(context.Background(), act,
			WithFollow(FollowSpec{CompletionEvents: []string{"OriginateResponse"}}))
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done
	if doErr != nil {
		t.Fatalf("Do() = %v", doErr)
	}
	if res.Follow == nil {
		t.Fatal("successful Do with follow returned no subscription")
	}

	s.event("DialBegin", "ActionID", act.id)
	s.event("OriginateResponse", "ActionID", act.id, "Reason", "4")

	ev, err := res.Follow.Next(context.Background())
	if err != nil || ev.Name() != "DialBegin" {
		t.Fatalf("follow Next() = (%v, %v)", ev, err)
	}
	ev, err = res.Follow.Next(context.Background())
	if err != nil || ev.Name() != "OriginateResponse" {
		t.Fatalf("follow completion Next() = (%v, %v)", ev, err)
	}
	if _, err := res.Follow.Next(context.Background()); err != io.EOF {
		t.Fatalf("follow after completion = %v, want io.EOF", err)
	}
	if err := res.Follow.Err(); err != nil {
		t.Fatalf("clean follow Err() = %v, want nil", err)
	}
	res.Follow.Close()

	// Correlated events also fanned out to the ordinary subscription.
	for _, want := range []string{"DialBegin", "OriginateResponse"} {
		ev, err := watcher.Next(context.Background())
		if err != nil || ev.Name() != want {
			t.Fatalf("watcher Next() = (%v, %v), want %s", ev, err, want)
		}
	}
}

func TestDoWithFollowErrorResponseReleasesBranch(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan error, 1)
	go func() {
		act, _ := NewAction("Originate")
		_, err := c.Do(context.Background(), act,
			WithFollow(FollowSpec{CompletionEvents: []string{"OriginateResponse"}}))
		done <- err
	}()
	act := s.readAction()
	s.respond(act.id, "Error", "Message", "nope")
	var re *ResponseError
	if err := <-done; !errors.As(err, &re) {
		t.Fatalf("Do() = %v, want *ResponseError", err)
	}
	// The provisional follow is gone; its late correlated event is an
	// ordinary event for a completed request and the client is alive.
	s.event("OriginateResponse", "ActionID", act.id)
	mustDo(t, c, s, "Ping")
}

func TestStartListHappyPath(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan struct{})
	var list *List
	var listErr error
	go func() {
		defer close(done)
		act, _ := NewAction("QueueStatus")
		list, listErr = c.StartList(context.Background(), act, ListSpec{
			CountFields: []string{"ListItems"},
		})
	}()
	act := s.readAction()
	if !strings.Contains(act.id, "-l") {
		t.Errorf("list ActionID %q lacks the list discriminator", act.id)
	}
	s.respond(act.id, "Success", "EventList", "start", "Message", "Queue status will follow")
	<-done
	if listErr != nil {
		t.Fatalf("StartList() = %v", listErr)
	}
	if list.Response().Get("Message") != "Queue status will follow" {
		t.Fatalf("list response lost: %v", list.Response())
	}

	s.event("QueueMember", "ActionID", act.id, "Queue", "synthetic-a")
	s.event("QueueMember", "ActionID", act.id, "Queue", "synthetic-b")
	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete", "ListItems", "2")

	for _, want := range []string{"synthetic-a", "synthetic-b"} {
		ev, err := list.Next(context.Background())
		if err != nil || ev.Get("Queue") != want {
			t.Fatalf("list Next() = (%v, %v), want %s", ev, err, want)
		}
	}
	if _, err := list.Next(context.Background()); err != io.EOF {
		t.Fatalf("list end = %v, want io.EOF", err)
	}
	if err := list.Err(); err != nil {
		t.Fatalf("clean list Err() = %v", err)
	}
	completion, ok := list.Completion()
	if !ok || completion.Name() != "QueueStatusComplete" {
		t.Fatalf("Completion() = (%v, %v)", completion, ok)
	}
	list.Close()
	mustDo(t, c, s, "Ping")
}

func TestStartListItemsNotDeliveredToSubscriptions(t *testing.T) {
	c, s := dialTest(t, nil)
	watcher, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan struct{})
	var list *List
	go func() {
		defer close(done)
		act, _ := NewAction("QueueStatus")
		list, _ = c.StartList(context.Background(), act, ListSpec{})
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done
	defer list.Close()

	s.event("QueueMember", "ActionID", act.id)
	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete")
	s.event("FullyBooted") // sentinel: the only thing the watcher may see

	ev, err := watcher.Next(context.Background())
	if err != nil || ev.Name() != "FullyBooted" {
		t.Fatalf("watcher saw (%v, %v); list traffic leaked into subscriptions", ev, err)
	}
}

func TestStartListErrorResponse(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan error, 1)
	go func() {
		act, _ := NewAction("QueueStatus")
		_, err := c.StartList(context.Background(), act, ListSpec{})
		done <- err
	}()
	act := s.readAction()
	s.respond(act.id, "Error", "Message", "no such queue")
	var re *ResponseError
	if err := <-done; !errors.As(err, &re) {
		t.Fatalf("StartList() = %v, want *ResponseError", err)
	}
	// Late list-kind traffic for the rejected list is discarded forever.
	s.event("QueueMember", "ActionID", act.id)
	mustDo(t, c, s, "Ping")
}

func TestStartListCountMismatch(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan struct{})
	var list *List
	go func() {
		defer close(done)
		act, _ := NewAction("QueueStatus")
		list, _ = c.StartList(context.Background(), act, ListSpec{CountFields: []string{"ListItems"}})
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done
	defer list.Close()

	s.event("QueueMember", "ActionID", act.id)
	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete", "ListItems", "5")

	// The queued item is discarded with the failure; the terminal is a
	// count-mismatch ListError.
	waitDone(t, list.Done(), "count-mismatch list")
	var le *ListError
	if err := list.Err(); !errors.As(err, &le) || le.Failure != ListCountMismatch {
		t.Fatalf("Err() = %v, want ListError{count mismatch}", err)
	}
	if _, err := list.Next(context.Background()); !errors.As(err, &le) {
		t.Fatalf("Next() = %v, want the ListError", err)
	}
	mustDo(t, c, s, "Ping") // the client survives a failed list
}

func TestStartListMalformedCount(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan struct{})
	var list *List
	go func() {
		defer close(done)
		act, _ := NewAction("QueueStatus")
		list, _ = c.StartList(context.Background(), act, ListSpec{CountFields: []string{"ListItems"}})
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done
	defer list.Close()

	s.event("QueueMember", "ActionID", act.id)
	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete", "ListItems", "bogus")

	// The declared integrity check could not run: that is a typed list
	// failure, not a clean end-of-stream over a possibly short snapshot.
	waitDone(t, list.Done(), "malformed-count list")
	var le *ListError
	if err := list.Err(); !errors.As(err, &le) || le.Failure != ListCountMalformed {
		t.Fatalf("Err() = %v, want ListError{count malformed}", err)
	}
	if _, err := list.Next(context.Background()); !errors.As(err, &le) {
		t.Fatalf("Next() = %v, want the ListError", err)
	}
	mustDo(t, c, s, "Ping") // the failure stays scoped to the list
}

func TestStartListCountFieldsBounds(t *testing.T) {
	c, _ := dialTest(t, nil)
	act, _ := NewAction("QueueStatus")
	many := make([]string, maxCountFields+1)
	for i := range many {
		many[i] = "F"
	}
	if _, err := c.StartList(context.Background(), act, ListSpec{CountFields: many}); err == nil {
		t.Fatal("StartList accepted a CountFields declaration over the name bound")
	}
	long := []string{strings.Repeat("x", maxCountFieldSize+1)}
	if _, err := c.StartList(context.Background(), act, ListSpec{CountFields: long}); err == nil {
		t.Fatal("StartList accepted a CountFields declaration over the byte bound")
	}
}

func TestStartListCancelled(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan struct{})
	var list *List
	go func() {
		defer close(done)
		act, _ := NewAction("QueueStatus")
		list, _ = c.StartList(context.Background(), act, ListSpec{})
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done
	defer list.Close()

	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Cancelled")
	waitDone(t, list.Done(), "cancelled list")
	var le *ListError
	if err := list.Err(); !errors.As(err, &le) || le.Failure != ListCancelled {
		t.Fatalf("Err() = %v, want ListError{cancelled}", err)
	}
}

func TestListCloseWhileStreamingDrains(t *testing.T) {
	c, s := dialTest(t, nil)
	watcher, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan struct{})
	var list *List
	go func() {
		defer close(done)
		act, _ := NewAction("QueueStatus")
		list, _ = c.StartList(context.Background(), act, ListSpec{})
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done

	s.event("QueueMember", "ActionID", act.id, "Queue", "before-close")
	list.Close()
	if err := list.Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Err() after Close = %v, want ErrClosed", err)
	}

	// The remote keeps streaming: drained traffic reaches nothing, and
	// the terminal mark releases the drain without killing the client.
	s.event("QueueMember", "ActionID", act.id, "Queue", "after-close")
	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete")
	s.event("FullyBooted") // sentinel

	ev, err := watcher.Next(context.Background())
	if err != nil || ev.Name() != "FullyBooted" {
		t.Fatalf("watcher saw (%v, %v); drained list traffic leaked", ev, err)
	}
	mustDo(t, c, s, "Ping")
}

func TestStartListContextAbandon(t *testing.T) {
	c, s := dialTest(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		act, _ := NewAction("QueueStatus")
		_, err := c.StartList(ctx, act, ListSpec{})
		done <- err
	}()
	act := s.readAction()
	cancel()
	err := <-done
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseResponse || !re.MayHaveExecuted() {
		t.Fatalf("StartList() = %v, want outcome-unknown RequestError", err)
	}

	// The whole late list — response, items, completion — is absorbed
	// by the drain and the client survives.
	s.respond(act.id, "Success")
	s.event("QueueMember", "ActionID", act.id)
	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete")
	mustDo(t, c, s, "Ping")
}

// TestDoneWaitsForInflightDispatch pins the quiescence barrier: Done
// must not close while a dispatch still holds its machine bookkeeping.
func TestDoneWaitsForInflightDispatch(t *testing.T) {
	c, _ := dialTest(t, nil)
	c.mu.Lock()
	c.inflight.Add(1) // a dispatch mid-bookkeeping
	c.mu.Unlock()
	c.Close()
	select {
	case <-c.Done():
		t.Fatal("Done closed while an in-flight dispatch held its bookkeeping")
	case <-time.After(50 * time.Millisecond):
	}
	c.inflight.Done()
	waitDone(t, c.Done(), "client after the in-flight hold released")
}

// TestClientDeathReleasesInflightDispatch drives the barrier end to
// end: a dispatch parked awaiting its response is completed by client
// death, finishes its bookkeeping, and Done still closes.
func TestClientDeathReleasesInflightDispatch(t *testing.T) {
	c, s := dialTest(t, nil)
	res := make(chan error, 1)
	go func() {
		act, _ := NewAction("Slow")
		_, err := c.Do(context.Background(), act)
		res <- err
	}()
	s.readAction() // fully written, awaiting its response
	c.Close()
	err := <-res
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseResponse || !re.MayHaveExecuted() {
		t.Fatalf("Do() at death = %v, want outcome-unknown RequestError", err)
	}
	waitDone(t, c.Done(), "client that died with a dispatch in flight")
}

// waitConsumerParked waits until the handle's consumer slot is taken
// and gives the consumer a beat to reach its park point, so a
// subsequent Close provably races a parked Next rather than one that
// has not started.
func waitConsumerParked(t *testing.T, busy *atomic.Bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !busy.Load() {
		if time.Now().After(deadline) {
			t.Fatal("consumer never entered Next")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
}

func TestCloseWakesParkedNext(t *testing.T) {
	c, s := dialTest(t, nil)

	t.Run("subscription", func(t *testing.T) {
		sub, err := c.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		res := make(chan error, 1)
		go func() {
			_, err := sub.Next(context.Background())
			res <- err
		}()
		waitConsumerParked(t, &sub.busy)
		sub.Close()
		select {
		case err := <-res:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("Next() after Close = %v, want ErrClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close left the parked Next blocked forever")
		}
	})

	t.Run("list", func(t *testing.T) {
		started := make(chan struct{})
		var list *List
		go func() {
			defer close(started)
			act, _ := NewAction("QueueStatus")
			list, _ = c.StartList(context.Background(), act, ListSpec{})
		}()
		act := s.readAction()
		s.respond(act.id, "Success")
		<-started

		res := make(chan error, 1)
		go func() {
			_, err := list.Next(context.Background())
			res <- err
		}()
		waitConsumerParked(t, &list.busy)
		list.Close()
		select {
		case err := <-res:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("Next() after Close = %v, want ErrClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close left the parked Next blocked forever")
		}
	})
}

func TestDoInvalidActionKeepsClientAlive(t *testing.T) {
	c, s := dialTest(t, nil)

	// A zero-value Action fails outbound validation before any byte is
	// written; the failure must stay scoped to its dispatch instead of
	// killing the session.
	_, err := c.Do(context.Background(), Action{})
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseWrite || re.MayHaveExecuted() {
		t.Fatalf("Do(zero Action) = %v, want not-sent write RequestError", err)
	}
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("Do(zero Action) = %v, want a wrapped *ProtocolError", err)
	}

	// An action exceeding the outbound wire limits is rejected the same
	// way: validated before the first byte, definitely not sent.
	big, err := NewAction("Command", Field{Key: "Command", Value: strings.Repeat("x", 64<<10)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Do(context.Background(), big)
	if !errors.As(err, &re) || re.Phase != PhaseWrite || re.MayHaveExecuted() || !errors.As(err, &pe) {
		t.Fatalf("Do(oversized) = %v, want not-sent write RequestError wrapping *ProtocolError", err)
	}

	// The session survived both rejections.
	mustDo(t, c, s, "Ping")
}

func TestWriteDeathRaceStaysNotSent(t *testing.T) {
	c, _ := dialTest(t, nil)

	// Admit a request by hand and let client death complete it, exactly
	// the state a death completion racing a failed zero-byte write
	// leaves behind.
	c.mu.Lock()
	tkt, err := c.machine.Admit("synthetic-race-r1", demux.KindRequest, demux.AdmitOptions[Message]{})
	if err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	w := make(chan demux.Completion[Message], 1)
	c.waiters[tkt] = w
	c.mu.Unlock()

	c.die(errors.New("synthetic transport failure"))

	// Zero bytes reached the wire, so definitely-not-sent must win over
	// the death completion's unknown-outcome default.
	if !c.resolveNotSentLocked(tkt, w, demux.AdmitOptions[Message]{}) {
		t.Fatal("resolveNotSentLocked adopted a raced death completion as the outcome")
	}
}

func TestWriteRaceDeliveredResponseWins(t *testing.T) {
	c, _ := dialTest(t, nil)

	c.mu.Lock()
	tkt, err := c.machine.Admit("synthetic-race-r2", demux.KindRequest, demux.AdmitOptions[Message]{})
	if err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	c.mu.Unlock()

	// A delivered completion — a response that outran the write failure —
	// is the committed outcome and stays in the waiter channel.
	w := make(chan demux.Completion[Message], 1)
	w <- demux.Completion[Message]{Ticket: tkt, Delivered: true}
	if c.resolveNotSentLocked(tkt, w, demux.AdmitOptions[Message]{}) {
		t.Fatal("resolveNotSentLocked discarded a delivered response")
	}
	if len(w) != 1 {
		t.Fatal("the delivered completion was not left for the awaiter")
	}
}

func TestSubscribeValidationAndLimits(t *testing.T) {
	c, _ := dialTest(t, func(cfg *Config) {
		cfg.Limits.MaxSubscriptions = 1
	})
	if _, err := c.Subscribe(Buffer(-1)); err == nil {
		t.Fatal("Subscribe(Buffer(-1)) succeeded")
	}
	sub, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe(); err == nil || !strings.Contains(err.Error(), "subscription limit") {
		t.Fatalf("Subscribe() over the limit = %v", err)
	}
	sub.Close()
	if _, err := c.Subscribe(); err != nil {
		t.Fatalf("Subscribe() after Close = %v, want the slot released", err)
	}
}
