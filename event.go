package ami

// An Event is one AMI event delivered through a subscription, follow,
// or list handle. It embeds the underlying immutable Message, so every
// field accessor is available; Name returns the event name directly.
type Event struct {
	Message
}

// Name returns the event's name: the value of its Event field, exactly
// as it appeared on the wire. It is never empty on an event delivered
// by this package, because a message without a usable Event field is
// not classified as an event.
func (e Event) Name() string {
	return e.Get("Event")
}

// Is reports whether the event's name equals name under ASCII case
// folding: the same equivalence the library itself uses for SubSpec.Events,
// FollowSpec and ListSpec completion names, and every other protocol
// identifier comparison.
//
// Prefer it over strings.EqualFold, whose Unicode simple folding is wider
// than the protocol's. The two disagree on inputs AMI treats as distinct —
// Kelvin sign versus "k", dotless and dotted i — so an application
// matching with strings.EqualFold can route an event the library would
// never have matched to that name.
func (e Event) Is(name string) bool {
	return equalFoldASCII(e.Name(), name)
}

// NewEvent constructs an Event: for tests, and for application code that
// synthesizes events. name becomes the event's Event field, placed first,
// and must be non-empty and free of NUL, CR, and LF; the remaining fields
// follow in order, with duplicate keys preserved, and are validated as
// NewAction validates an action's fields.
//
// The inbound envelope keys ActionID, EventList, and Response are legal
// here, because delivered events carry them — an OriginateResponse
// carries all three. A caller-supplied Event field is rejected: the
// constructor owns that field, so the result satisfies the invariant
// every delivered event satisfies, that Name is never empty.
//
// The fields are copied, so later mutation of the caller's slice cannot
// change the event.
func NewEvent(name string, fields ...Field) (Event, error) {
	if err := validateName("event", name); err != nil {
		return Event{}, err
	}
	if err := validateFields("event", []string{"Event"}, fields); err != nil {
		return Event{}, err
	}
	fs := make([]Field, 0, len(fields)+1)
	fs = append(fs, Field{Key: "Event", Value: name})
	fs = append(fs, fields...)
	return Event{Message{fields: fs}}, nil
}

// NewResponse constructs a Response for tests, from the fields a real
// response carries: its Response disposition, the echoed ActionID, and
// any payload, in the given order. No envelope field is synthesized — a
// response's disposition is the caller's declaration, including its
// absence.
//
// Fields are validated as in NewEvent, except that an Event key is
// rejected: a message carrying one classifies as an event, so it could
// never arrive as a response. The fields are copied.
func NewResponse(fields ...Field) (Response, error) {
	if err := validateFields("response", []string{"Event"}, fields); err != nil {
		return Response{}, err
	}
	return Response{newMessage(fields)}, nil
}

// A Response is the immediate AMI response to one action. It embeds the
// underlying immutable Message; the raw fields are explicit, untrusted
// remote data that the application must classify before acting on or
// logging.
type Response struct {
	Message
}

// responseSuccess reports whether a response message acknowledges the
// action: Asterisk reports "Success" and, for command output frames,
// "Follows". Anything else — "Error", "Goodbye", or arbitrary text — is
// a rejection.
func responseSuccess(m Message) bool {
	switch v := m.Get("Response"); {
	case equalFoldASCII(v, "Success"), equalFoldASCII(v, "Follows"):
		return true
	}
	return false
}
