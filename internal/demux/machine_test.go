package demux

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// The model tests drive the machine with int payloads. For lists, the
// completion event's payload doubles as its declared count: countFromPayload
// reports the payload as the count when it is non-negative.

func testLimits() Limits {
	return Limits{
		MaxPending:           4,
		MaxSubscriptions:     4,
		MaxSubscriptionBytes: 1 << 20,
		MaxLists:             4,
		MaxListBytes:         1 << 20,
		MaxMatcherNames:      8,
		MaxMatcherBytes:      256,
		MaxRetirement:        8,
		RetirementLifetime:   1000,
	}
}

func newMachine(t *testing.T, mut ...func(*Limits)) *Machine[int] {
	t.Helper()
	lim := testLimits()
	for _, f := range mut {
		f(&lim)
	}
	if err := lim.Validate(); err != nil {
		t.Fatalf("test limits invalid: %v", err)
	}
	return New[int](lim)
}

func countFromPayload(v int) (int64, bool) {
	if v < 0 {
		return 0, false
	}
	return int64(v), true
}

// Envelope builders. Sizes default to 10 bytes.

func resp(id string, kind Kind, success bool) Envelope {
	return Envelope{Class: ClassResponse, ActionID: id, Own: true, Kind: kind, Success: success, Size: 10}
}

func respForeign(id string) Envelope {
	return Envelope{Class: ClassResponse, ActionID: id, Size: 10}
}

func ev(name string) Envelope {
	return Envelope{Class: ClassEvent, Name: name, Size: 10}
}

func evOwn(name, id string, kind Kind) Envelope {
	return Envelope{Class: ClassEvent, Name: name, ActionID: id, Own: true, Kind: kind, Size: 10}
}

func evMark(name, id string, mark Mark) Envelope {
	return Envelope{Class: ClassEvent, Name: name, ActionID: id, Own: true, Kind: KindList, Mark: mark, Size: 10}
}

func sized(env Envelope, size int) Envelope {
	env.Size = size
	return env
}

func at(env Envelope, now int64) Envelope {
	env.Now = now
	return env
}

// Call helpers.

func admit(t *testing.T, m *Machine[int], id string, kind Kind, o AdmitOptions[int]) Ticket {
	t.Helper()
	tk, err := m.Admit(id, kind, o)
	if err != nil {
		t.Fatalf("Admit(%s): unexpected error %v", id, err)
	}
	return tk
}

func listOpts(completions ...string) *ListOptions[int] {
	return &ListOptions[int]{
		Completions:   completions,
		Caps:          Caps{Items: 16, Bytes: 4096},
		ObservedBytes: 1 << 16,
		Count:         countFromPayload,
	}
}

func subscribe(t *testing.T, m *Machine[int], names ...string) BranchID {
	t.Helper()
	id, err := m.Subscribe(Matcher{Events: names}, Caps{Items: 16, Bytes: 4096})
	if err != nil {
		t.Fatalf("Subscribe(%v): unexpected error %v", names, err)
	}
	return id
}

// route asserts the envelope routes without a fatality.
func route(t *testing.T, m *Machine[int], env Envelope, msg int) Effects[int] {
	t.Helper()
	fx := m.Route(env, msg)
	if fx.Fatal != nil {
		t.Fatalf("Route(%+v): unexpected fatality %v", env, fx.Fatal.Reason)
	}
	return fx
}

// routeFatal asserts the envelope kills the machine with the reason.
func routeFatal(t *testing.T, m *Machine[int], env Envelope, msg int, want Reason) Effects[int] {
	t.Helper()
	fx := m.Route(env, msg)
	if fx.Fatal == nil {
		t.Fatalf("Route(%+v): no fatality, want %v", env, want)
	}
	if fx.Fatal.Reason != want {
		t.Fatalf("Route(%+v): fatality %v, want %v", env, fx.Fatal.Reason, want)
	}
	if _, dead := m.Dead(); !dead {
		t.Fatalf("Route(%+v): machine alive after fatality", env)
	}
	return fx
}

func woken(fx Effects[int], id BranchID) bool {
	return slices.Contains(fx.Wake, id)
}

// completed returns the single Completion for the ticket, failing if it
// is absent.
func completed(t *testing.T, fx Effects[int], tk Ticket) Completion[int] {
	t.Helper()
	for _, c := range fx.Complete {
		if c.Ticket == tk {
			return c
		}
	}
	t.Fatalf("effects carry no completion for ticket %v", tk)
	return Completion[int]{}
}

// takeItem asserts the next Take yields an item with the payload.
func takeItem(t *testing.T, m *Machine[int], id BranchID, want int) {
	t.Helper()
	got, res := m.Take(id)
	if res.State != TakeItem {
		t.Fatalf("Take: state %v, want TakeItem(%d)", res.State, want)
	}
	if got != want {
		t.Fatalf("Take: payload %d, want %d", got, want)
	}
}

// takeState asserts the next Take reports the state (and reason, for
// TakeTerminal).
func takeState(t *testing.T, m *Machine[int], id BranchID, want TakeState, reason Reason) {
	t.Helper()
	_, res := m.Take(id)
	if res.State != want {
		t.Fatalf("Take: state %v, want %v", res.State, want)
	}
	if want == TakeTerminal && res.Reason != reason {
		t.Fatalf("Take: terminal reason %v, want %v", res.Reason, reason)
	}
}

func wantAggregates(t *testing.T, m *Machine[int], sub, list int) {
	t.Helper()
	gotSub, gotList := m.Aggregates()
	if gotSub != sub || gotList != list {
		t.Fatalf("aggregates (sub=%d, list=%d), want (sub=%d, list=%d)", gotSub, gotList, sub, list)
	}
}

func wantRetirement(t *testing.T, m *Machine[int], held, records int) {
	t.Helper()
	gotHeld, gotRecords := m.RetirementLoad()
	if gotHeld != held || gotRecords != records {
		t.Fatalf("retirement (held=%d, records=%d), want (held=%d, records=%d)", gotHeld, gotRecords, held, records)
	}
}

func wantErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error %v, want %v", err, want)
	}
}

// wantPanic asserts f panics with a message containing want.
func wantPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("no panic, want one containing %q", want)
		}
		s, ok := r.(string)
		if !ok || !strings.Contains(s, want) {
			t.Fatalf("panic %v, want one containing %q", r, want)
		}
	}()
	f()
}

func TestLimitsValidate(t *testing.T) {
	if err := testLimits().Validate(); err != nil {
		t.Fatalf("valid limits rejected: %v", err)
	}
	zeroed := []func(*Limits){
		func(l *Limits) { l.MaxPending = 0 },
		func(l *Limits) { l.MaxSubscriptions = 0 },
		func(l *Limits) { l.MaxSubscriptionBytes = -1 },
		func(l *Limits) { l.MaxLists = 0 },
		func(l *Limits) { l.MaxListBytes = 0 },
		func(l *Limits) { l.MaxMatcherNames = 0 },
		func(l *Limits) { l.MaxMatcherBytes = 0 },
		func(l *Limits) { l.MaxRetirement = 0 },
		func(l *Limits) { l.RetirementLifetime = 0 },
	}
	for i, mut := range zeroed {
		lim := testLimits()
		mut(&lim)
		if err := lim.Validate(); err == nil {
			t.Errorf("case %d: non-positive limit accepted", i)
		}
	}
}

func TestFold(t *testing.T) {
	for _, tt := range []struct{ in, want string }{
		{"", ""},
		{"agentcalled", "agentcalled"},
		{"AgentCalled", "agentcalled"},
		{"PEERENTRY", "peerentry"},
		{"Üst-Case", "Üst-case"}, // only ASCII letters fold
	} {
		if got := fold(tt.in); got != tt.want {
			t.Errorf("fold(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
	if s := "already-lower"; fold(s) != s {
		t.Errorf("fold of a lowercase string changed it")
	}
}
