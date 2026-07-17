package ami

import "errors"

// Sentinel errors. Errors returned by this package support errors.Is
// against the sentinel that describes them; richer typed errors wrap
// the sentinels rather than replace them. Sentinel text is stable and
// never embeds remote or connection-specific data.
var (
	// ErrClosed reports that the connection, client, subscription, or
	// list was closed locally before or during the operation.
	ErrClosed = errors.New("ami: closed")

	// ErrLagged reports that a subscription overflowed its bounded
	// queue and was closed: the consumer lost synchronization with the
	// event stream, queued events were discarded, and nothing further
	// is delivered on that subscription. Recovery is a replacement
	// subscription followed by a fresh snapshot; events are never
	// silently skipped on a subscription that remains open.
	ErrLagged = errors.New("ami: subscription lagged")

	// ErrLoginFailed reports that the AMI server rejected
	// authentication during Dial.
	ErrLoginFailed = errors.New("ami: login failed")

	// ErrPingTimeout reports that a keepalive Ping was completely
	// written and its valid matching response did not arrive within the
	// configured response timeout. The client is terminated with this
	// as its root cause.
	ErrPingTimeout = errors.New("ami: ping response timeout")

	// ErrPingWriteTimeout reports that a due keepalive Ping could not
	// acquire write ownership and be fully written within the
	// configured write-attempt deadline. The client is terminated with
	// this as its root cause.
	ErrPingWriteTimeout = errors.New("ami: ping write timeout")

	// ErrOutcomeUnknown reports that at least one byte of an action may
	// have reached the server without proof of the complete exchange:
	// the server may have executed the action. Callers must reconcile
	// externally instead of retrying blindly; the library never retries
	// on its own.
	ErrOutcomeUnknown = errors.New("ami: action outcome unknown")

	// ErrRetirementExpired reports that an outcome-unknown retirement
	// record or an abandoned-list drain did not observe its terminal
	// evidence within the configured lifetime. The client closes with
	// this as its root cause rather than risk misclassifying late
	// correlated traffic.
	ErrRetirementExpired = errors.New("ami: retirement expired")
)

// A ProtocolError reports a violation of the AMI wire protocol or of a
// configured wire limit. Its Error text is stable and sanitized: it
// carries the violation's category and dimension and never embeds raw
// remote content. An inbound ProtocolError means the connection has been
// closed, because framing beyond the violation cannot be trusted; a
// ProtocolError from outbound validation is reported before any byte is
// written and leaves the connection usable.
type ProtocolError struct {
	// Category classifies the violation: "limit" for a WireLimits
	// breach, "framing" for a malformed inbound frame, "envelope" for an
	// invalid outbound action envelope or an unclassifiable inbound
	// message, or "correlation" for a response the session cannot
	// attribute to any request.
	Category string

	// Dimension identifies what was violated: the WireLimits field name
	// for limit violations, otherwise a short fixed description.
	Dimension string

	cause error
}

func (e *ProtocolError) Error() string {
	return "ami: protocol violation: " + e.Category + ": " + e.Dimension
}

// Unwrap returns the underlying cause, if any, for use with errors.Is.
func (e *ProtocolError) Unwrap() error {
	return e.cause
}

// A RequestPhase locates where an action dispatch failed.
type RequestPhase uint8

const (
	// PhaseAdmission: waiting for write ownership or a correlation
	// reservation. Nothing was written; the action was definitely not
	// sent.
	PhaseAdmission RequestPhase = 1 + iota

	// PhaseWrite: during the action write. Whether the action was sent
	// depends on how the write failed; MayHaveExecuted distinguishes.
	PhaseWrite

	// PhaseResponse: the complete action was written and the request
	// ended — by context or client death — before a response won. The
	// server may have executed the action.
	PhaseResponse
)

// String returns the stable phase name.
func (p RequestPhase) String() string {
	switch p {
	case PhaseAdmission:
		return "admission"
	case PhaseWrite:
		return "write"
	case PhaseResponse:
		return "awaiting response"
	}
	return "unknown"
}

// A RequestError reports a failed action dispatch: where it failed,
// whether the server may have executed the action anyway, and the
// underlying cause through Unwrap. Its Error text is stable and never
// embeds the cause's text.
//
// errors.Is(err, ErrOutcomeUnknown) is true exactly when
// MayHaveExecuted is true.
type RequestError struct {
	// Phase locates the failure in the dispatch pipeline.
	Phase RequestPhase

	// ActionID is the client-assigned correlation ID, when one was
	// assigned before the failure. It is opaque; callers must not parse
	// it.
	ActionID string

	mayHaveExecuted bool
	cause           error
}

func (e *RequestError) Error() string {
	s := "ami: request failed: " + e.Phase.String()
	if e.mayHaveExecuted {
		s += " (outcome unknown)"
	}
	return s
}

// MayHaveExecuted reports whether at least one action byte may have
// reached the server without proof of the complete exchange: the server
// may have executed the action, and the application must reconcile
// externally instead of retrying blindly.
func (e *RequestError) MayHaveExecuted() bool {
	return e.mayHaveExecuted
}

// Unwrap returns the underlying cause: the context error, transport
// error, or client root cause that ended the request.
func (e *RequestError) Unwrap() error {
	return e.cause
}

// Is reports ErrOutcomeUnknown exactly when MayHaveExecuted is true,
// alongside the causes exposed through Unwrap.
func (e *RequestError) Is(target error) bool {
	return target == ErrOutcomeUnknown && e.mayHaveExecuted
}

// A ResponseError reports that the server answered an action with an
// error response. Its Error text is stable and never embeds the remote
// Message field; the raw response is available — explicitly, as
// untrusted data — through Response.
type ResponseError struct {
	resp Response
}

func (e *ResponseError) Error() string {
	return "ami: server returned an error response"
}

// Response returns the raw error response. Its fields are untrusted
// remote data; the application must classify and redact them before
// logging or acting on them.
func (e *ResponseError) Response() Response {
	return e.resp
}

// A DialError reports where Dial failed: establishing the TCP
// connection, the TLS handshake, reading the banner, or logging in. Its
// Error text is stable and omits endpoint and cause text; Unwrap
// exposes the underlying cause, which the application must classify and
// redact before logging.
type DialError struct {
	// Phase is "dial", "tls", "banner", or "login".
	Phase string

	cause error
}

func (e *DialError) Error() string {
	return "ami: dial failed: " + e.Phase
}

// Unwrap returns the underlying cause. A login rejection unwraps to
// ErrLoginFailed.
func (e *DialError) Unwrap() error {
	return e.cause
}

// A KeepaliveError reports why the keepalive terminated the client. Its
// Error text is stable; timeout phases unwrap to ErrPingWriteTimeout or
// ErrPingTimeout.
type KeepaliveError struct {
	// Phase is "write" (the due Ping could not be admitted and fully
	// written in time), "response" (the written Ping's response missed
	// its deadline), or "rejected" (the server answered the Ping with an
	// error or malformed response).
	Phase string

	cause error
}

func (e *KeepaliveError) Error() string {
	return "ami: keepalive failed: " + e.Phase
}

// Unwrap returns the wrapped sentinel: ErrPingWriteTimeout for the
// write phase, ErrPingTimeout for the response phase, nil for a
// rejection.
func (e *KeepaliveError) Unwrap() error {
	return e.cause
}

// A ListFailure classifies a terminal list failure.
type ListFailure uint8

const (
	// ListCancelled: the server terminated the list with
	// EventList: cancelled.
	ListCancelled ListFailure = 1 + iota

	// ListOverflowed: the list exceeded its queued items/bytes caps or
	// its total observed-bytes budget.
	ListOverflowed

	// ListCountMismatch: the completion event declared a count that did
	// not match the observed items.
	ListCountMismatch

	// ListCountMalformed: the completion event carried a configured
	// count field whose value was unusable — empty, non-numeric,
	// negative, or out of range — so the declared integrity check could
	// not run.
	ListCountMalformed
)

// String returns the stable failure name.
func (f ListFailure) String() string {
	switch f {
	case ListCancelled:
		return "cancelled"
	case ListOverflowed:
		return "overflowed"
	case ListCountMismatch:
		return "count mismatch"
	case ListCountMalformed:
		return "count malformed"
	}
	return "unknown"
}

// A ListError reports a terminal list failure: cancellation by the
// server, overflow of the list's bounded state, or a declared count
// that mismatched the observed items or could not be parsed at all.
// Its Error text is stable and carries no remote data.
type ListError struct {
	// Failure classifies what terminated the list.
	Failure ListFailure
}

func (e *ListError) Error() string {
	return "ami: list failed: " + e.Failure.String()
}

// A RetirementError reports that an outcome-unknown retirement record
// or an abandoned-list drain expired without observing its terminal
// evidence. The client closes with this as its root cause rather than
// risk misclassifying late correlated traffic. It matches
// ErrRetirementExpired through errors.Is.
type RetirementError struct {
	// Kind is "request" for an outcome-unknown response retirement or
	// "list" for an abandoned-list drain.
	Kind string
}

func (e *RetirementError) Error() string {
	return "ami: retirement expired: " + e.Kind + " record"
}

// Unwrap returns ErrRetirementExpired.
func (e *RetirementError) Unwrap() error {
	return ErrRetirementExpired
}
