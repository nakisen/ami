package ami

import "errors"

// Sentinel errors. Errors returned by this package support errors.Is
// against the sentinel that describes them; richer typed errors wrap
// the sentinels rather than replace them. Sentinel text is stable and
// never embeds remote or connection-specific data.
var (
	// ErrClosed reports that the client, subscription, or list was
	// closed locally before or during the operation.
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
