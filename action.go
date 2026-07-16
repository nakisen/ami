package ami

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
)

// An Action describes one AMI action: the action name plus the ordered
// extra fields sent with it. The Action and ActionID envelope fields are
// deliberately absent from the field list — the transport composes them
// at write time, so a caller can never smuggle a conflicting envelope.
//
// Actions are immutable after construction and safe for concurrent use.
// They are reusable descriptions: the same Action value may be
// dispatched any number of times. The zero value is invalid and is
// rejected at write time.
type Action struct {
	name   string
	fields []Field
}

// NewAction validates and constructs an Action. The name must be
// non-empty and free of CR and LF. Field keys must be non-empty, free of
// colons, CR, and LF, and must not be the reserved Action or ActionID
// envelope keys under case-insensitive matching; field values must be
// free of CR and LF. Duplicate keys are legal and their order is
// preserved. The fields are copied, so later mutation of the caller's
// slice cannot change the action.
//
// NewAction validates shape and injection only. Connection-level size
// limits are enforced by WriteAction against the connection's
// WireLimits.
func NewAction(name string, fields ...Field) (Action, error) {
	if name == "" {
		return Action{}, errors.New("ami: invalid action: empty name")
	}
	if strings.ContainsAny(name, "\r\n") {
		return Action{}, errors.New("ami: invalid action: name contains CR or LF")
	}
	for i, f := range fields {
		switch {
		case f.Key == "":
			return Action{}, fmt.Errorf("ami: invalid action: field %d: empty key", i)
		case strings.ContainsAny(f.Key, ":\r\n"):
			return Action{}, fmt.Errorf("ami: invalid action: field %d: key contains a colon, CR, or LF", i)
		case strings.EqualFold(f.Key, "Action"), strings.EqualFold(f.Key, "ActionID"):
			return Action{}, fmt.Errorf("ami: invalid action: field %d: reserved key %q", i, f.Key)
		case strings.ContainsAny(f.Value, "\r\n"):
			return Action{}, fmt.Errorf("ami: invalid action: field %d: value contains CR or LF", i)
		}
	}
	return Action{name: name, fields: slices.Clone(fields)}, nil
}

// Name returns the action name.
func (a Action) Name() string {
	return a.name
}

// Fields returns an iterator over the action's extra fields as
// (key, value) pairs in order, duplicates included. The Action and
// ActionID envelope fields are not part of the sequence.
func (a Action) Fields() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for _, f := range a.fields {
			if !yield(f.Key, f.Value) {
				return
			}
		}
	}
}
