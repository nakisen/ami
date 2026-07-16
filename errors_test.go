package ami

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestErrorSurfaceText pins the stable, sanitized Error texts and the
// wrapping contracts of the typed errors.
func TestErrorSurfaceText(t *testing.T) {
	cause := errors.New("synthetic cause with sensitive host:5038 text")

	re := &RequestError{Phase: PhaseWrite, ActionID: "x", mayHaveExecuted: true, cause: cause}
	if re.Error() != "ami: request failed: write (outcome unknown)" {
		t.Errorf("RequestError text = %q", re.Error())
	}
	if !errors.Is(re, ErrOutcomeUnknown) || !errors.Is(re, cause) || !re.MayHaveExecuted() {
		t.Error("RequestError wrapping misbehaves")
	}
	notSent := &RequestError{Phase: PhaseAdmission, cause: cause}
	if notSent.Error() != "ami: request failed: admission" || errors.Is(notSent, ErrOutcomeUnknown) {
		t.Errorf("not-sent RequestError = %q", notSent.Error())
	}
	if PhaseResponse.String() != "awaiting response" || RequestPhase(0).String() != "unknown" {
		t.Error("RequestPhase.String misbehaves")
	}

	respErr := &ResponseError{resp: Response{newMessage([]Field{{Key: "Message", Value: "secret"}})}}
	if strings.Contains(respErr.Error(), "secret") {
		t.Errorf("ResponseError text embeds the response: %q", respErr.Error())
	}
	if respErr.Response().Get("Message") != "secret" {
		t.Error("ResponseError lost the raw response")
	}

	de := &DialError{Phase: "tls", cause: cause}
	if de.Error() != "ami: dial failed: tls" || !errors.Is(de, cause) {
		t.Errorf("DialError = %q", de.Error())
	}
	if strings.Contains(de.Error(), "host") {
		t.Error("DialError text embeds the cause")
	}

	ke := &KeepaliveError{Phase: "response", cause: ErrPingTimeout}
	if ke.Error() != "ami: keepalive failed: response" || !errors.Is(ke, ErrPingTimeout) {
		t.Errorf("KeepaliveError = %q", ke.Error())
	}

	for failure, want := range map[ListFailure]string{
		ListCancelled:     "cancelled",
		ListOverflowed:    "overflowed",
		ListCountMismatch: "count mismatch",
		ListFailure(0):    "unknown",
	} {
		if failure.String() != want {
			t.Errorf("ListFailure(%d).String() = %q, want %q", failure, failure.String(), want)
		}
	}
	le := &ListError{Failure: ListOverflowed}
	if le.Error() != "ami: list failed: overflowed" {
		t.Errorf("ListError text = %q", le.Error())
	}

	rte := &RetirementError{Kind: "list"}
	if rte.Error() != "ami: retirement expired: list record" || !errors.Is(rte, ErrRetirementExpired) {
		t.Errorf("RetirementError = %q", rte.Error())
	}
}

func TestPanicError(t *testing.T) {
	wrapped := errors.New("boom")
	if got := panicError(wrapped); got != wrapped {
		t.Errorf("panicError(error) = %v", got)
	}
	if got := panicError("ami/demux: invariant violated: x"); got.Error() != "ami/demux: invariant violated: x" {
		t.Errorf("panicError(string) = %v", got)
	}
	if got := panicError(42); !strings.Contains(got.Error(), "42") {
		t.Errorf("panicError(other) = %v", got)
	}
}

func TestDispatchRejectionMappings(t *testing.T) {
	c, s := dialTest(t, func(cfg *Config) {
		cfg.Limits.MaxLists = 1
		cfg.Limits.MaxMatcherNames = 2
	})

	// List limit: one list active, the second rejected not-sent.
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
	defer list.Close()

	q, _ := NewAction("QueueStatus")
	_, err := c.StartList(context.Background(), q, ListSpec{})
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseAdmission || !strings.Contains(err.Error(), "admission") {
		t.Fatalf("StartList over the list limit = %v, want admission RequestError", err)
	}

	// Matcher limit through a follow spec: rejected before any byte.
	o, _ := NewAction("Originate")
	_, err = c.Do(context.Background(), o, WithFollow(FollowSpec{
		EventNames: []string{"a", "b", "c"},
	}))
	if err == nil || !strings.Contains(err.Error(), "matcher") {
		t.Fatalf("Do with an oversized matcher = %v", err)
	}

	// Structurally invalid follow options: an empty event name.
	_, err = c.Do(context.Background(), o, WithFollow(FollowSpec{
		EventNames: []string{""},
	}))
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("Do with an empty follow name = %v", err)
	}

	// Subscription matcher limit.
	if _, err := c.Subscribe(MatchEvents("a", "b", "c")); err == nil || !strings.Contains(err.Error(), "matcher") {
		t.Fatalf("Subscribe with an oversized matcher = %v", err)
	}
}

func TestListAll(t *testing.T) {
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

	s.event("QueueMember", "ActionID", act.id, "Queue", "synthetic-a")
	s.event("QueueMember", "ActionID", act.id, "Queue", "synthetic-b")
	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete")

	var got []string
	for ev, err := range list.All(context.Background()) {
		if err != nil {
			t.Fatalf("All yielded error %v", err)
		}
		got = append(got, ev.Get("Queue"))
	}
	if len(got) != 2 || got[0] != "synthetic-a" || got[1] != "synthetic-b" {
		t.Fatalf("All items = %v", got)
	}
	// The adapter closed the handle; clean completion kept a nil Err
	// until the local close committed... which preserves the clean
	// result: Err stays nil after a clean terminal followed by Close.
	if err := list.Err(); err != nil {
		t.Fatalf("Err() after clean All = %v, want nil", err)
	}
	mustDo(t, c, s, "Ping")
}
