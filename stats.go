package ami

// Stats is a point-in-time view of one client's internal accounting: what
// the session absorbed by design, and how close its bounded state is to
// its limits.
//
// It exists because the library's honest-failure rules produce questions
// only a gauge can answer. After ErrLagged the first question is how close
// to the bound the consumer was; when sizing SubSpec.BufferItems, the
// documented "worst pull gap times the event rate" is an estimate until
// something measures it. Config.Logger reports a timeline of events, which
// is a different instrument: it says what happened, not what the level is.
//
// Every field is either monotonic since Dial or a current gauge. The whole
// view is taken at one point under the session lock, so the counters and
// gauges are consistent with each other. They are diagnostics: a counter
// never implies a delivery guarantee, and no library behavior depends on
// them.
type Stats struct {
	// Unmatched counts delivered events that matched no subscription. On
	// an unfiltered busy connection this is the common case, not a fault.
	Unmatched uint64

	// Quarantined counts messages absorbed without delivery: correlated
	// traffic held by a live retirement or abandoned-list drain record,
	// and list traffic arriving after a buffered terminal mark.
	Quarantined uint64

	// LateListDiscards counts own list-correlated events with no
	// remaining state — traffic for a completed or retired list — which
	// are discarded permanently.
	LateListDiscards uint64

	// DiagDrops counts diagnostics the bounded internal queue dropped
	// rather than block. It is zero when no Logger is configured, because
	// nothing is queued.
	DiagDrops uint64

	// Subscriptions is the number of subscription-family branches the
	// client holds: those from Subscribe and those adopted from DoFollow,
	// plus a follow still provisional on an in-flight request and any
	// terminal branch whose handle is not yet closed.
	Subscriptions int

	// Lists is the number of list branches held, on the same terms.
	Lists int

	// Pending is the number of in-flight public actions, bounded by
	// Limits.MaxPending. The library's own keepalive slot is excluded.
	Pending int

	// Retirements is the number of live outcome-unknown retirement and
	// abandoned-list drain records, bounded by Limits.MaxRetirement. A
	// number that stays high means requests are being abandoned faster
	// than their evidence arrives.
	Retirements int

	// QueuedSubscriptionBytes is the client-wide charged subscription and
	// follow queue bytes, bounded by Limits.MaxSubscriptionBytes.
	QueuedSubscriptionBytes int

	// QueuedListBytes is the client-wide retained list bytes, stored
	// completion events included, bounded by Limits.MaxListBytes.
	QueuedListBytes int
}

// Stats returns the client's current accounting. It takes the session
// lock, which the read loop also needs, so it is meant for periodic
// observation — a metrics scrape — and not for a tight loop.
//
// A terminated client keeps answering: the counters stop advancing and
// the gauges report whatever state survived termination, which is what
// makes Stats usable after Done closes to explain what the session did.
func (c *Client) Stats() Stats {
	c.mu.Lock()
	snap := c.machine.Snapshot()
	c.mu.Unlock()
	return Stats{
		Unmatched:               snap.Unmatched,
		Quarantined:             snap.Quarantined,
		LateListDiscards:        snap.LateListDiscards,
		DiagDrops:               c.diag.dropped(),
		Subscriptions:           snap.Subscriptions,
		Lists:                   snap.Lists,
		Pending:                 snap.Pending,
		Retirements:             snap.Retirements,
		QueuedSubscriptionBytes: snap.SubscriptionBytes,
		QueuedListBytes:         snap.ListBytes,
	}
}
