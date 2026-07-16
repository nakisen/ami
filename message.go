package ami

import (
	"iter"
	"slices"
	"strings"
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
