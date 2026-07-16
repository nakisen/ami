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
