package ami

import (
	"context"
	"slices"
	"testing"
)

func TestListSpecFor(t *testing.T) {
	spec, ok := ListSpecFor("QueueStatus")
	if !ok {
		t.Fatal("ListSpecFor(QueueStatus) reported the action as unknown")
	}
	if !slices.Equal(spec.CompletionEvents, []string{"QueueStatusComplete"}) ||
		!slices.Equal(spec.CountFields, []string{"ListItems"}) {
		t.Fatalf("ListSpecFor(QueueStatus) = %+v", spec)
	}
	// Matched under the protocol's ASCII case folding, like every other
	// identifier.
	for _, name := range []string{"queuestatus", "QUEUESTATUS", "QueueStatus"} {
		if _, ok := ListSpecFor(name); !ok {
			t.Errorf("ListSpecFor(%q) reported the action as unknown", name)
		}
	}
	// The count alternatives exist because StatusComplete carries the same
	// count under two names.
	if spec, ok := ListSpecFor("Status"); !ok ||
		!slices.Equal(spec.CountFields, []string{"ListItems", "Items"}) {
		t.Fatalf("ListSpecFor(Status) = (%+v, %v)", spec, ok)
	}
	// An unknown action is reported, not guessed: the caller declares its
	// own contract.
	for _, name := range []string{"", "Ping", "QueueStatusComplete", "PJSIPShowEndpoints"} {
		if spec, ok := ListSpecFor(name); ok {
			t.Errorf("ListSpecFor(%q) = (%+v, true), want unknown", name, spec)
		}
	}
}

// TestListSpecForReturnsACopy pins the isolation the table needs: the
// entries are package-level values, so a caller mutating a returned spec
// must not corrupt them for every later call.
func TestListSpecForReturnsACopy(t *testing.T) {
	first, ok := ListSpecFor("QueueStatus")
	if !ok {
		t.Fatal("ListSpecFor(QueueStatus) reported the action as unknown")
	}
	first.CompletionEvents[0] = "Corrupted"
	first.CountFields[0] = "Corrupted"

	second, _ := ListSpecFor("QueueStatus")
	if second.CompletionEvents[0] != "QueueStatusComplete" || second.CountFields[0] != "ListItems" {
		t.Fatalf("mutating a returned spec changed the table: %+v", second)
	}
}

// TestListContractsAreWellFormed pins the table's own invariants: every
// entry is usable as a ListSpec without further validation, and no action
// appears twice under a different spelling.
func TestListContractsAreWellFormed(t *testing.T) {
	d := DefaultLimits()
	seen := map[string]bool{}
	for _, c := range listContracts {
		if c.action == "" {
			t.Fatal("a contract has no action name")
		}
		if seen[foldASCII(c.action)] {
			t.Fatalf("action %q appears twice in the table", c.action)
		}
		seen[foldASCII(c.action)] = true

		if len(c.spec.CompletionEvents) == 0 {
			t.Errorf("%s: no completion event declared; an entry that adds nothing over the header convention should not be in the table", c.action)
		}
		for _, name := range c.spec.CompletionEvents {
			if name == "" {
				t.Errorf("%s: empty completion event name", c.action)
			}
		}
		// The declaration must fit the default bounds, or the table would
		// ship an entry StartList rejects out of the box.
		if len(c.spec.CountFields) > d.MaxMatcherNames || len(c.spec.CompletionEvents) > d.MaxMatcherNames {
			t.Errorf("%s: declaration exceeds the default matcher name bound", c.action)
		}
		total := 0
		for _, f := range c.spec.CountFields {
			if f == "" {
				t.Errorf("%s: empty count field name", c.action)
			}
			total += len(f)
		}
		if total > d.MaxMatcherBytes {
			t.Errorf("%s: count declaration exceeds the default matcher byte bound", c.action)
		}
	}
}

// TestListSpecForDrivesARealList runs two table entries end to end,
// including the one whose count arrives under the second alternative, so
// the shipped contracts are exercised by the same machinery a consumer
// uses rather than only compared as data.
func TestListSpecForDrivesARealList(t *testing.T) {
	tests := []struct {
		action     string
		completion string
		countField string // the field the server actually sends
	}{
		{"QueueStatus", "QueueStatusComplete", "ListItems"},
		{"Status", "StatusComplete", "Items"},
	}
	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			c, s := dialTest(t, nil)
			spec, ok := ListSpecFor(tt.action)
			if !ok {
				t.Fatalf("ListSpecFor(%q) reported the action as unknown", tt.action)
			}
			act, err := NewAction(tt.action)
			if err != nil {
				t.Fatal(err)
			}
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

			s.event("SyntheticItem", "ActionID", got.id, "Detail", "one")
			s.event(tt.completion, "ActionID", got.id, tt.countField, "1")

			items := 0
			for _, err := range list.All(t.Context()) {
				if err != nil {
					t.Fatalf("All() error = %v", err)
				}
				items++
			}
			if items != 1 {
				t.Fatalf("All() yielded %d items, want 1", items)
			}
			if err := list.Err(); err != nil {
				t.Fatalf("Err() after clean completion = %v", err)
			}
			cpl, ok := list.Completion()
			if !ok || !cpl.Is(tt.completion) {
				t.Fatalf("Completion() = (%v, %v), want the declared completion event", cpl.Name(), ok)
			}
		})
	}
}
