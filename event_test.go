package ami

import (
	"slices"
	"strings"
	"testing"
)

func TestNewEvent(t *testing.T) {
	e, err := NewEvent("QueueMemberStatus",
		Field{Key: "Queue", Value: "support"},
		Field{Key: "ActionID", Value: "synthetic-r1"},
		Field{Key: "Variable", Value: "a=1"},
		Field{Key: "Variable", Value: "b=2"},
		Field{Key: "Empty", Value: ""},
	)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if e.Name() != "QueueMemberStatus" {
		t.Fatalf("Name() = %q, want QueueMemberStatus", e.Name())
	}
	// The synthesized envelope field leads, and the declared fields keep
	// their order and their duplicates.
	want := []Field{
		{Key: "Event", Value: "QueueMemberStatus"},
		{Key: "Queue", Value: "support"},
		{Key: "ActionID", Value: "synthetic-r1"},
		{Key: "Variable", Value: "a=1"},
		{Key: "Variable", Value: "b=2"},
		{Key: "Empty", Value: ""},
	}
	var got []Field
	for k, v := range e.Fields() {
		got = append(got, Field{Key: k, Value: v})
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Fields() = %v, want %v", got, want)
	}
	if vals := e.Values("VARIABLE"); !slices.Equal(vals, []string{"a=1", "b=2"}) {
		t.Fatalf("Values(VARIABLE) = %q, want both values in order", vals)
	}
	if v, ok := e.Lookup("Empty"); !ok || v != "" {
		t.Fatalf("Lookup(Empty) = (%q, %v), want a present empty value", v, ok)
	}
	if _, ok := e.Lookup("Absent"); ok {
		t.Fatal("Lookup(Absent) reported a field that was never declared")
	}
}

func TestNewEventRejects(t *testing.T) {
	tests := []struct {
		name    string
		event   string
		fields  []Field
		wantHas string
	}{
		{"empty name", "", nil, "empty name"},
		{"name with cr", "Newchannel\r", nil, "NUL, CR, or LF"},
		{"name with lf", "Newchannel\nEvent: Injected", nil, "NUL, CR, or LF"},
		{"name with nul", "Newchannel\x00", nil, "NUL, CR, or LF"},
		{"empty key", "Newchannel", []Field{{Key: "", Value: "v"}}, "empty key"},
		{"colon in key", "Newchannel", []Field{{Key: "A:B", Value: "v"}}, "colon"},
		{"cr in key", "Newchannel", []Field{{Key: "A\rB", Value: "v"}}, "colon, NUL, CR, or LF"},
		{"reserved event key", "Newchannel", []Field{{Key: "event", Value: "Other"}}, "reserved"},
		{"lf injection in value", "Newchannel", []Field{{Key: "K", Value: "v\r\nEvent: Injected"}}, "NUL, CR, or LF"},
		{"nul in value", "Newchannel", []Field{{Key: "K", Value: "a\x00b"}}, "NUL, CR, or LF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewEvent(tt.event, tt.fields...)
			if err == nil || !strings.Contains(err.Error(), tt.wantHas) {
				t.Fatalf("NewEvent() error = %v, want containing %q", err, tt.wantHas)
			}
		})
	}
}

func TestNewEventCopiesFields(t *testing.T) {
	in := []Field{{Key: "Queue", Value: "original"}}
	e, err := NewEvent("QueueMemberStatus", in...)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	in[0] = Field{Key: "Queue", Value: "mutated"}
	if got := e.Get("Queue"); got != "original" {
		t.Fatalf("mutating the input slice changed the event: %q", got)
	}
}

// TestNewEventMatchesDeliveredShape pins the constructor against the
// delivery path: a synthesized event must classify exactly as the same
// frame parsed off the wire, or a handler test proves nothing about the
// handler's real input.
func TestNewEventMatchesDeliveredShape(t *testing.T) {
	c, s := dialTest(t, nil)
	sub, err := c.Subscribe(SubSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	s.event("QueueMemberStatus", "Queue", "support", "Variable", "a=1", "Variable", "b=2")

	delivered, err := sub.Next(t.Context())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	built, err := NewEvent("QueueMemberStatus",
		Field{Key: "Queue", Value: "support"},
		Field{Key: "Variable", Value: "a=1"},
		Field{Key: "Variable", Value: "b=2"},
	)
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	var deliveredFields, builtFields []Field
	for k, v := range delivered.Fields() {
		deliveredFields = append(deliveredFields, Field{Key: k, Value: v})
	}
	for k, v := range built.Fields() {
		builtFields = append(builtFields, Field{Key: k, Value: v})
	}
	if !slices.Equal(deliveredFields, builtFields) {
		t.Fatalf("constructed event = %v, delivered event = %v", builtFields, deliveredFields)
	}
}

func TestNewResponse(t *testing.T) {
	r, err := NewResponse(
		Field{Key: "Response", Value: "Follows"},
		Field{Key: "ActionID", Value: "synthetic-r1"},
		Field{Key: "Output", Value: "line one"},
		Field{Key: "Output", Value: "line two"},
	)
	if err != nil {
		t.Fatalf("NewResponse() error = %v", err)
	}
	if !responseSuccess(r.Message) {
		t.Fatal("a Follows response did not read as an acknowledgement")
	}
	if got := r.Values("Output"); !slices.Equal(got, []string{"line one", "line two"}) {
		t.Fatalf("Values(Output) = %q, want both lines in order", got)
	}
	if got := r.Get("ActionID"); got != "synthetic-r1" {
		t.Fatalf("Get(ActionID) = %q", got)
	}
}

func TestNewResponseRejectsAndAcceptsEmpty(t *testing.T) {
	// An Event key would classify the message as an event, so it could
	// never be delivered as a response.
	if _, err := NewResponse(Field{Key: "Event", Value: "Newchannel"}); err == nil ||
		!strings.Contains(err.Error(), "reserved") {
		t.Fatalf("NewResponse with an Event field = %v, want a reserved-key error", err)
	}
	if _, err := NewResponse(Field{Key: "A:B", Value: "v"}); err == nil ||
		!strings.Contains(err.Error(), "colon") {
		t.Fatalf("NewResponse with a colon in a key = %v", err)
	}
	// No envelope field is synthesized: a response with no fields at all
	// is a legal, empty declaration rather than an error.
	r, err := NewResponse()
	if err != nil {
		t.Fatalf("NewResponse() error = %v", err)
	}
	if r.Get("Response") != "" || responseSuccess(r.Message) {
		t.Fatalf("empty NewResponse() synthesized a disposition: %v", r)
	}
}
