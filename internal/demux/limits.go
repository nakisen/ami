package demux

import (
	"errors"
	"fmt"
)

// Limits bounds every machine dimension. Each limit must be positive
// (see Validate): zero never means unbounded, and an unvalidated
// non-positive limit fails closed at the first admission, registration,
// or delivery it bounds. The numeric anchors are a session concern;
// the machine only enforces them.
type Limits struct {
	// MaxPending bounds concurrently admitted public actions. The
	// internal keepalive Ping uses a permanently reserved separate
	// slot.
	MaxPending int

	// MaxSubscriptions bounds concurrently registered ordinary
	// subscriptions.
	MaxSubscriptions int

	// MaxSubscriptionBytes bounds the client-wide queued bytes of the
	// subscription family: ordinary subscriptions and follows together.
	MaxSubscriptionBytes int

	// MaxLists bounds concurrently routing-active lists.
	MaxLists int

	// MaxListBytes bounds the client-wide retained list bytes: queued
	// items plus stored completion events.
	MaxListBytes int

	// MaxMatcherNames bounds one registration's declarative name set —
	// subscription matchers, follow selections and completions, and
	// list completions alike.
	MaxMatcherNames int

	// MaxMatcherBytes bounds the cumulative byte length of one
	// registration's declarative name set.
	MaxMatcherBytes int

	// MaxRetirement bounds the reserved retirement/drain slots. Every
	// public admission holds one slot until its action's outcome is
	// proven; live records are never evicted, so exhaustion rejects
	// admission as definitely-not-sent. The internal keepalive slot is
	// separate.
	MaxRetirement int

	// RetirementLifetime is the logical-clock duration from record
	// creation to expiry, in the same units as the timestamps supplied
	// by the session.
	RetirementLifetime int64
}

// Validate reports the first non-positive limit. The session validates
// limits once at construction; the machine then trusts them.
func (l Limits) Validate() error {
	for _, d := range []struct {
		name  string
		value int64
	}{
		{"MaxPending", int64(l.MaxPending)},
		{"MaxSubscriptions", int64(l.MaxSubscriptions)},
		{"MaxSubscriptionBytes", int64(l.MaxSubscriptionBytes)},
		{"MaxLists", int64(l.MaxLists)},
		{"MaxListBytes", int64(l.MaxListBytes)},
		{"MaxMatcherNames", int64(l.MaxMatcherNames)},
		{"MaxMatcherBytes", int64(l.MaxMatcherBytes)},
		{"MaxRetirement", int64(l.MaxRetirement)},
		{"RetirementLifetime", l.RetirementLifetime},
	} {
		if d.value <= 0 {
			return fmt.Errorf("ami/demux: limit %s must be positive", d.name)
		}
	}
	return nil
}

// Caps bounds one branch's queue by item count and retained bytes. Both
// must be positive; a zero cap fails closed by rejecting the
// registration.
type Caps struct {
	Items int
	Bytes int
}

func (c Caps) valid() bool {
	return c.Items > 0 && c.Bytes > 0
}

// Matcher is a subscription's declarative selection: a bounded set of
// event names, folded once at registration. An empty set matches every
// event.
type Matcher struct {
	Events []string
}

// AdmitOptions carries the branch registrations an admission makes
// atomically with its pending reservation.
type AdmitOptions[T any] struct {
	// Internal admits the reserved keepalive slot instead of a public
	// one. Internal admissions carry no follow or list branch.
	Internal bool

	// Follow registers a request-kind follow branch, routing-active
	// from admission.
	Follow *FollowOptions

	// List configures a list-kind admission; required exactly when the
	// kind is KindList.
	List *ListOptions[T]
}

// FollowOptions is the machine-facing shape of a FollowSpec.
type FollowOptions struct {
	// Events selects nonterminal follow events by folded name; empty
	// selects every correlated nonterminal event.
	Events []string

	// Completions declares the terminal event names. Every declared
	// completion is implicitly eligible for delivery regardless of
	// Events. Empty means the follow only ends by explicit close.
	Completions []string

	// Caps bounds the follow queue; its charges count against the
	// subscription-family aggregate.
	Caps Caps
}

// ListOptions is the machine-facing shape of a ListSpec.
type ListOptions[T any] struct {
	// Completions declares terminal event names for actions predating
	// the EventList header convention; the generic EventList marks
	// always terminate. Empty selects the pure header convention.
	Completions []string

	// Caps bounds the queued items and retained bytes, stored
	// completion event included.
	Caps Caps

	// ObservedBytes bounds the cumulative wire bytes the remote may
	// stream through this list while it is routing-active, regardless
	// of drain rate.
	ObservedBytes int

	// Count extracts the declared item count from the completion
	// event, reporting its verdict: absent, declared, or malformed. It
	// must be a pure bounded function authored by the session — it
	// runs during Route. Nil means no count is declared.
	Count func(T) (int64, CountVerdict)
}

// Machine errors. Admit and Subscribe return these sentinels with
// stable text; the session maps them onto the public error taxonomy.
// Every rejection happens before any byte could have been written:
// definitely-not-sent.
var (
	// ErrDead reports a call on a killed machine.
	ErrDead = errors.New("ami/demux: machine dead")

	// ErrPendingLimit reports admission beyond MaxPending.
	ErrPendingLimit = errors.New("ami/demux: pending limit reached")

	// ErrRetirementLimit reports that no retirement/drain slot could be
	// reserved for the admission.
	ErrRetirementLimit = errors.New("ami/demux: retirement slots exhausted")

	// ErrSubscriptionLimit reports registration beyond
	// MaxSubscriptions.
	ErrSubscriptionLimit = errors.New("ami/demux: subscription limit reached")

	// ErrListLimit reports admission beyond MaxLists.
	ErrListLimit = errors.New("ami/demux: list limit reached")

	// ErrMatcherLimit reports a declarative name set exceeding
	// MaxMatcherNames or MaxMatcherBytes.
	ErrMatcherLimit = errors.New("ami/demux: name set too large")

	// ErrInternalBusy reports an internal admission while the reserved
	// keepalive slot — pending or retirement — is still occupied.
	ErrInternalBusy = errors.New("ami/demux: internal slot occupied")

	// ErrInvalidOptions reports structurally invalid admission or
	// registration options: a kind/branch mismatch, empty names, or
	// non-positive caps.
	ErrInvalidOptions = errors.New("ami/demux: invalid options")
)
