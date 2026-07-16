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
	// breach, "framing" for a malformed inbound frame, or "envelope" for
	// an invalid outbound action envelope.
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
