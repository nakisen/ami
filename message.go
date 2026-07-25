package ami

import (
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/nakisen/ami/internal/wire"
)

// A Field is one key/value pair of an AMI message. AMI messages are
// ordered field sequences in which the same key may legally repeat
// (Variable:, ChanVariable:, Output:), so a Field is meaningful only at
// its position within that order.
type Field struct {
	Key   string
	Value string
}

// A Message is one complete AMI message: an immutable, ordered sequence
// of fields as they appeared on the wire. Repeated keys are preserved in
// wire order, values keep meaningful emptiness and whitespace, and key
// matching is case-insensitive throughout.
//
// The zero value is an empty message. Message values are immutable and
// safe for concurrent use.
type Message struct {
	fields []Field
}

// newMessage constructs a Message from fields, copying the slice so
// later caller mutation cannot reach the stored sequence.
func newMessage(fields []Field) Message {
	if len(fields) == 0 {
		return Message{}
	}
	return Message{fields: slices.Clone(fields)}
}

// validateName applies the naming rules every constructed message kind
// shares: a non-empty name free of NUL, CR, and LF.
func validateName(kind, name string) error {
	if name == "" {
		return fmt.Errorf("ami: invalid %s: empty name", kind)
	}
	if strings.ContainsAny(name, "\x00\r\n") {
		return fmt.Errorf("ami: invalid %s: name contains NUL, CR, or LF", kind)
	}
	return nil
}

// validateFields applies the field rules every constructed message kind
// shares: non-empty keys free of colons, NUL, CR, and LF, values free of
// NUL, CR, and LF, and no key from reserved, which names the envelope
// fields the constructor itself owns. NUL is rejected alongside the line
// terminators because C-based managers truncate at it, so a NUL-bearing
// value could mean something other than what was validated.
func validateFields(kind string, reserved []string, fields []Field) error {
	for i, f := range fields {
		switch {
		case f.Key == "":
			return fmt.Errorf("ami: invalid %s: field %d: empty key", kind, i)
		case strings.ContainsAny(f.Key, ":\x00\r\n"):
			return fmt.Errorf("ami: invalid %s: field %d: key contains a colon, NUL, CR, or LF", kind, i)
		}
		for _, r := range reserved {
			if strings.EqualFold(f.Key, r) {
				return fmt.Errorf("ami: invalid %s: field %d: reserved key %q", kind, i, f.Key)
			}
		}
		if strings.ContainsAny(f.Value, "\x00\r\n") {
			return fmt.Errorf("ami: invalid %s: field %d: value contains NUL, CR, or LF", kind, i)
		}
	}
	return nil
}

// messageFromWire adopts fields parsed by internal/wire, converting them
// into the package's own Field type with the single copy the immutable
// Message requires. internal/wire never imports this package; the type
// conversion happens here, at the boundary.
func messageFromWire(fields []wire.Field) Message {
	if len(fields) == 0 {
		return Message{}
	}
	fs := make([]Field, len(fields))
	for i, f := range fields {
		fs[i] = Field(f)
	}
	return Message{fields: fs}
}

// Get returns the value of the first field whose key equals key under
// case-insensitive matching, or the empty string when no such field
// exists. Use Lookup to distinguish an absent field from a present field
// with an empty value, and Values to observe every occurrence of a
// repeating key.
func (m Message) Get(key string) string {
	v, _ := m.Lookup(key)
	return v
}

// Lookup returns the value of the first field whose key equals key under
// case-insensitive matching. The second result reports whether such a
// field exists, distinguishing an absent field from a present field with
// an empty value.
func (m Message) Lookup(key string) (string, bool) {
	for _, f := range m.fields {
		if strings.EqualFold(f.Key, key) {
			return f.Value, true
		}
	}
	return "", false
}

// Values returns the values of every field whose key equals key under
// case-insensitive matching, in wire order, or nil when no such field
// exists. The returned slice is the caller's own copy; mutating it does
// not affect the message.
func (m Message) Values(key string) []string {
	var vals []string
	for _, f := range m.fields {
		if strings.EqualFold(f.Key, key) {
			vals = append(vals, f.Value)
		}
	}
	return vals
}

// Fields returns an iterator over every field of the message as
// (key, value) pairs in wire order, including repeated keys.
func (m Message) Fields() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for _, f := range m.fields {
			if !yield(f.Key, f.Value) {
				return
			}
		}
	}
}
