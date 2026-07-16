package demux

// Kind discriminates the two ActionID families the session issues:
// ordinary requests and list actions. The session parses it from its
// opaque ActionID discriminator; on an Envelope it is meaningful only
// when Own is true.
type Kind uint8

const (
	KindRequest Kind = 1 + iota
	KindList
)

// Class is the envelope classification of one parsed message. The zero
// value is ClassInvalid so an unclassified envelope fails closed: the
// machine treats it as a fatal envelope violation.
type Class uint8

const (
	ClassInvalid Class = iota
	ClassEvent
	ClassResponse
)

// Mark is the parsed EventList header disposition. MarkStart is
// confirmatory only and never changes routing; MarkComplete and
// MarkCancelled are terminal marks for list-kind traffic.
type Mark uint8

const (
	MarkNone Mark = iota
	MarkStart
	MarkComplete
	MarkCancelled
)

// Envelope is the classification the session extracts from one parsed
// message; it is total input for Route. The session owns extraction:
// Event presence beats an event-specific Response field, conflicting
// duplicate envelope fields classify as ClassInvalid, and Name arrives
// already ASCII-folded.
type Envelope struct {
	// Class is the message classification; ClassInvalid is fatal.
	Class Class

	// Name is the folded event name; meaningful when Class is
	// ClassEvent and never empty on a classified event.
	Name string

	// ActionID is the verbatim correlation key; empty when the message
	// carries none.
	ActionID string

	// Own reports whether ActionID carries this session's prefix.
	Own bool

	// Kind is the parsed ActionID discriminator; meaningful only when
	// Own is true.
	Kind Kind

	// Mark is the parsed EventList disposition.
	Mark Mark

	// Success is the response disposition — whether the Response field
	// reported success — and is meaningful only when Class is
	// ClassResponse. It decides list-branch arming; request-kind
	// responses are delivered to their waiters either way.
	Success bool

	// Size is the retained-byte charge for every queue this message
	// enters: the frame's wire size.
	Size int

	// Now is the caller's monotonic timestamp; it advances the logical
	// clock that dates drain records created during routing.
	Now int64
}

// Ticket identifies one admitted action through its resolution
// protocol. The zero value is not a valid ticket.
type Ticket struct{ n uint64 }

// BranchID identifies one consumer-facing branch — subscription,
// adopted follow, or adopted list — for Take and Close. The zero value
// is not a valid branch.
type BranchID struct{ n uint64 }

// Reason is a machine-internal cause code. The session maps reasons to
// the public error surface; reason strings are stable diagnostics and
// never embed remote data.
type Reason uint8

const (
	ReasonNone Reason = iota

	// Fatalities.
	ReasonEnvelopeInvalid   // structurally invalid envelope
	ReasonResponseNoID      // response without an ActionID
	ReasonResponseForeign   // response with a foreign ActionID
	ReasonResponseUnmatched // response matching no pending and no record: unknown or duplicate
	ReasonRetirementExpired // a retirement/drain record expired without evidence
	ReasonKilled            // session-initiated Kill

	// Branch terminals.
	ReasonLagged        // reserve-or-terminate failed for a subscription or follow
	ReasonOverflow      // list capacity or observed-bytes budget exhausted
	ReasonCancelled     // EventList: cancelled
	ReasonCountMismatch // declared list count did not match observed items
	ReasonClosed        // local close
	ReasonClientDead    // terminated by the client-wide death cascade
)

// String returns the stable diagnostic name of the reason.
func (r Reason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonEnvelopeInvalid:
		return "invalid envelope"
	case ReasonResponseNoID:
		return "response without ActionID"
	case ReasonResponseForeign:
		return "foreign response"
	case ReasonResponseUnmatched:
		return "unmatched response"
	case ReasonRetirementExpired:
		return "retirement expired"
	case ReasonKilled:
		return "killed"
	case ReasonLagged:
		return "lagged"
	case ReasonOverflow:
		return "list overflow"
	case ReasonCancelled:
		return "list cancelled"
	case ReasonCountMismatch:
		return "list count mismatch"
	case ReasonClosed:
		return "closed"
	case ReasonClientDead:
		return "client dead"
	}
	return "unknown"
}

// Effects is what a machine call asks the session to do after releasing
// the lock. Wake identifies branches whose consumers must be signaled;
// Complete releases response waiters; Fatal, when non-nil, requires the
// session to terminate the client with the mapped cause.
type Effects[T any] struct {
	Wake     []BranchID
	Complete []Completion[T]
	Fatal    *Fatality
}

func (fx *Effects[T]) wake(id uint64) {
	fx.Wake = append(fx.Wake, BranchID{id})
}

// Completion releases one admitted action's waiter. Delivered reports
// whether Response holds the correlated response; false means the
// client died first and the session supplies its root cause instead.
type Completion[T any] struct {
	Ticket    Ticket
	Response  T
	Delivered bool
}

// Fatality reports an internally detected terminal violation. Kind is
// meaningful for ReasonRetirementExpired, identifying which record
// family expired.
type Fatality struct {
	Reason Reason
	Kind   Kind
}

// TakeState reports what Take found.
type TakeState uint8

const (
	// TakeItem: one message was dequeued and its charges released.
	TakeItem TakeState = 1 + iota
	// TakeEmpty: the branch is active with nothing queued; the session
	// parks the consumer until the branch appears in a wake set.
	TakeEmpty
	// TakeEOF: a clean terminal fully drained; nothing further will be
	// delivered.
	TakeEOF
	// TakeTerminal: the branch is terminal with TakeResult.Reason; its
	// queue was discarded when the terminal committed.
	TakeTerminal
)

// TakeResult is the branch state Take observed.
type TakeResult struct {
	State  TakeState
	Reason Reason
}

// Counters is the machine's monotonic discard accounting: traffic that
// was absorbed or dropped by design, counted so it is never silent.
type Counters struct {
	// LateListDiscards counts own list-kind events with no remaining
	// state: late traffic for completed or retired lists, discarded
	// forever.
	LateListDiscards uint64

	// Quarantined counts messages absorbed without delivery: traffic
	// held by live retirement/drain records and list traffic after a
	// buffered terminal mark.
	Quarantined uint64

	// Unmatched counts events that matched no ordinary subscription —
	// on a busy unfiltered connection, the common case and the cheapest
	// path through the machine.
	Unmatched uint64
}
