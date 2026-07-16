// Package wire implements the AMI wire protocol: banner reading, inbound
// message parsing, and outbound message encoding, all under explicit
// caller-supplied limits.
//
// The package is mechanical. It preserves field order and duplicate keys,
// keeps keys and values verbatim — consuming exactly one optional space
// after the key's colon — and attaches no meaning to field names beyond
// the two wire-level cases that change framing or accounting:
//
//   - A message whose first field is "Response: Follows" uses the legacy
//     Command output framing (Asterisk 12–14.1): after optional Privilege
//     and ActionID trailer headers, every line up to "--END COMMAND--" is
//     raw command output. The parser normalizes each output line into a
//     synthesized "Output" field so both Command framings present the
//     same message shape.
//   - Fields whose key is "Output" — synthesized from legacy payload or
//     received as modern repeated headers — are charged against the
//     command-output line and byte limits instead of the per-message
//     field and byte limits, so command output has its own budget and
//     cannot consume the bounds meant for ordinary messages.
//
// Envelope classification (Action, Response, Event), case-insensitive
// lookup, and every session concern live in the root package. This
// package must never import the root package; the root package converts
// wire fields into its immutable Message with a single copy.
package wire

import (
	"errors"
	"fmt"
)

// A Field is one key/value pair of an AMI message, meaningful only at its
// position within the message's ordered field sequence. It mirrors the
// root package's public Field structure without importing it.
type Field struct {
	Key   string
	Value string
}

// Limits bounds every wire dimension. Each limit must be positive (see
// Validate): zero never means unbounded anywhere in this package, and an
// unvalidated zero limit fails closed at the first read or write it
// bounds. Line limits bound a line's content excluding its terminator;
// byte budgets bound raw consumed or produced bytes including
// terminators.
type Limits struct {
	// MaxBannerBytes bounds the banner line content, excluding the line
	// terminator. The banner is read before authentication, so this is
	// deliberately the tightest inbound limit.
	MaxBannerBytes int

	// MaxLineBytes bounds one inbound line's content, excluding the line
	// terminator. It applies to every message line, command output
	// included.
	MaxLineBytes int

	// MaxFields bounds the fields of one inbound message, excluding
	// command output fields, which MaxCommandOutputLines bounds instead.
	MaxFields int

	// MaxMessageBytes bounds the raw inbound bytes of one message outside
	// command output: field lines, framing lines, and the terminating
	// blank line, terminators included.
	MaxMessageBytes int

	// MaxCommandOutputLines bounds the command output lines of one
	// inbound message under either framing: synthesized legacy payload
	// lines and modern repeated Output headers alike.
	MaxCommandOutputLines int

	// MaxCommandOutputBytes bounds the raw inbound bytes of one message's
	// command output lines, terminators included.
	MaxCommandOutputBytes int

	// MaxActionFields bounds the fields of one outbound message.
	MaxActionFields int

	// MaxActionLineBytes bounds one encoded outbound line, excluding its
	// terminator.
	MaxActionLineBytes int

	// MaxActionBytes bounds one outbound message's total encoded bytes,
	// terminators and the final blank line included.
	MaxActionBytes int
}

// Validate reports the first non-positive limit. The connection layer
// validates limits once at construction; this package then trusts them.
func (l Limits) Validate() error {
	for _, d := range []struct {
		name  string
		value int
	}{
		{"MaxBannerBytes", l.MaxBannerBytes},
		{"MaxLineBytes", l.MaxLineBytes},
		{"MaxFields", l.MaxFields},
		{"MaxMessageBytes", l.MaxMessageBytes},
		{"MaxCommandOutputLines", l.MaxCommandOutputLines},
		{"MaxCommandOutputBytes", l.MaxCommandOutputBytes},
		{"MaxActionFields", l.MaxActionFields},
		{"MaxActionLineBytes", l.MaxActionLineBytes},
		{"MaxActionBytes", l.MaxActionBytes},
	} {
		if d.value <= 0 {
			return fmt.Errorf("ami/wire: limit %s must be positive", d.name)
		}
	}
	return nil
}

// Wire errors. Reader methods and AppendMessage return these sentinels
// with stable text that never embeds remote or caller data; the root
// package maps them onto its public error taxonomy.
var (
	// ErrBannerTooLong reports a banner line exceeding MaxBannerBytes.
	ErrBannerTooLong = errors.New("ami/wire: banner line too long")

	// ErrLineTooLong reports an inbound line exceeding MaxLineBytes.
	ErrLineTooLong = errors.New("ami/wire: line too long")

	// ErrTooManyFields reports an inbound message exceeding MaxFields.
	ErrTooManyFields = errors.New("ami/wire: too many fields")

	// ErrMessageTooLarge reports an inbound message exceeding
	// MaxMessageBytes outside command output.
	ErrMessageTooLarge = errors.New("ami/wire: message too large")

	// ErrTooManyOutputLines reports command output exceeding
	// MaxCommandOutputLines.
	ErrTooManyOutputLines = errors.New("ami/wire: too many command output lines")

	// ErrOutputTooLarge reports command output exceeding
	// MaxCommandOutputBytes.
	ErrOutputTooLarge = errors.New("ami/wire: command output too large")

	// ErrMalformedLine reports an inbound line with no colon or an empty
	// key where a field is required.
	ErrMalformedLine = errors.New("ami/wire: malformed line")

	// ErrEmptyMessage reports a blank line where an inbound message must
	// start, or an outbound message with no fields.
	ErrEmptyMessage = errors.New("ami/wire: empty message")

	// ErrCommandFraming reports a legacy command frame whose
	// "--END COMMAND--" terminator is not followed by the message-ending
	// blank line.
	ErrCommandFraming = errors.New("ami/wire: malformed command framing")

	// ErrTooManyActionFields reports an outbound message exceeding
	// MaxActionFields.
	ErrTooManyActionFields = errors.New("ami/wire: too many action fields")

	// ErrActionLineTooLong reports an outbound line exceeding
	// MaxActionLineBytes.
	ErrActionLineTooLong = errors.New("ami/wire: action line too long")

	// ErrActionTooLarge reports an outbound message exceeding
	// MaxActionBytes.
	ErrActionTooLarge = errors.New("ami/wire: action too large")

	// ErrInvalidKey reports an outbound field key that is empty or
	// contains a colon, carriage return, or line feed.
	ErrInvalidKey = errors.New("ami/wire: invalid field key")

	// ErrInvalidValue reports an outbound field value containing a
	// carriage return or line feed.
	ErrInvalidValue = errors.New("ami/wire: invalid field value")
)
