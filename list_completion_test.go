package ami

import (
	"context"
	"errors"
	"testing"
)

func startCompletionTestList(t *testing.T, c *Client, s *script, spec ListSpec) (*List, string) {
	t.Helper()
	done := make(chan struct{})
	var list *List
	var startErr error
	go func() {
		defer close(done)
		act, err := NewAction("QueueStatus")
		if err != nil {
			startErr = err
			return
		}
		list, startErr = c.StartList(context.Background(), act, spec)
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done
	if startErr != nil {
		t.Fatalf("StartList() = %v", startErr)
	}
	return list, act.id
}

func TestListAllPreservesCompletionAfterOwningClose(t *testing.T) {
	c, s := dialTest(t, nil)
	list, actionID := startCompletionTestList(t, c, s, ListSpec{})

	s.event("QueueMember", "ActionID", actionID, "Queue", "synthetic")
	s.event("QueueStatusComplete", "ActionID", actionID, "EventList", "Complete", "ListItems", "1")

	var items int
	for ev, err := range list.All(context.Background()) {
		if err != nil {
			t.Fatalf("All yielded error %v", err)
		}
		if ev.Get("Queue") != "synthetic" {
			t.Fatalf("All yielded event %v", ev)
		}
		items++
	}
	if items != 1 {
		t.Fatalf("All yielded %d items, want 1", items)
	}

	completion, ok := list.Completion()
	if !ok {
		t.Fatal("Completion() unavailable after clean All")
	}
	if completion.Name() != "QueueStatusComplete" || completion.Get("ListItems") != "1" {
		t.Fatalf("Completion() = %v, want QueueStatusComplete with ListItems=1", completion)
	}
	if err := list.Err(); err != nil {
		t.Fatalf("Err() after clean All = %v, want nil", err)
	}
}

func TestListAllDoesNotPreserveCompletionAfterFailure(t *testing.T) {
	tests := []struct {
		name string
		spec ListSpec
		send func(*script, string)
		want ListFailure
	}{
		{
			name: "cancelled",
			send: func(s *script, actionID string) {
				s.event("QueueStatusComplete", "ActionID", actionID, "EventList", "Cancelled")
			},
			want: ListCancelled,
		},
		{
			name: "count mismatch",
			spec: ListSpec{CountFields: []string{"ListItems"}},
			send: func(s *script, actionID string) {
				s.event("QueueMember", "ActionID", actionID)
				s.event("QueueStatusComplete", "ActionID", actionID, "EventList", "Complete", "ListItems", "2")
			},
			want: ListCountMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, s := dialTest(t, nil)
			list, actionID := startCompletionTestList(t, c, s, tt.spec)
			tt.send(s, actionID)

			var gotErr error
			for _, err := range list.All(context.Background()) {
				gotErr = err
			}
			var listErr *ListError
			if !errors.As(gotErr, &listErr) || listErr.Failure != tt.want {
				t.Fatalf("All error = %v, want ListError{%v}", gotErr, tt.want)
			}
			listErr = nil
			if err := list.Err(); !errors.As(err, &listErr) || listErr.Failure != tt.want {
				t.Fatalf("Err() after failed All = %v, want ListError{%v}", err, tt.want)
			}
			if completion, ok := list.Completion(); ok {
				t.Fatalf("Completion() = %v after failed All, want unavailable", completion)
			}
		})
	}
}
