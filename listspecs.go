package ami

import "slices"

// A listContract is one verified entry of the known-list table: the
// canonical action spelling and the completion contract to declare for
// it. Every entry carries the source it was verified against and the
// Asterisk range it holds for.
type listContract struct {
	action string
	spec   ListSpec
}

// listContracts records list-action completion contracts verified against
// Asterisk sources. Heuristic completion detection is a documented dead
// end — which is why ListSpec exists — but the library then made every
// consumer restate the same protocol facts by hand, and a consumer who
// gets QueueStatus wrong falls into exactly the failure ListSpec was
// built to prevent.
//
// The table's only value is being more trustworthy than what a consumer
// would write, so a guess would destroy the reason it exists: an entry is
// added only with a source note, and ListSpecFor answering "unknown" is
// the honest alternative. The table may be incomplete; it may not be
// wrong.
//
// Most list actions terminate through the manager's shared helper
// astman_send_list_complete_start (main/manager.c), which emits
// "EventList: Complete" and "ListItems: <count>" alongside the action's
// completion event name — verified against asterisk/asterisk master,
// main/manager.c astman_send_list_complete_start_common. That is why
// ListItems is the count field for every entry below, and why the hybrid
// header convention already covers these actions even without a declared
// name.
var listContracts = []listContract{
	{
		// apps/app_queue.c, manager_queues_status: registered as the
		// QueueStatus action and terminated with
		// astman_send_list_complete_start(s, m, "QueueStatusComplete", ...).
		// Verified against master; the pairing predates the supported
		// floor and holds across Asterisk 12 and newer.
		action: "QueueStatus",
		spec: ListSpec{
			CompletionEvents: []string{"QueueStatusComplete"},
			CountFields:      []string{"ListItems"},
		},
	},
	{
		// main/manager.c, action_status: terminated with
		// astman_send_list_complete_start(s, m, "StatusComplete", channels)
		// immediately followed by an explicit "Items: <channels>" line, so
		// the completion carries the same count twice under two names.
		// This is the pairing that motivated CountFields alternatives.
		// Verified against master; holds across Asterisk 12 and newer.
		action: "Status",
		spec: ListSpec{
			CompletionEvents: []string{"StatusComplete"},
			CountFields:      []string{"ListItems", "Items"},
		},
	},
	{
		// channels/chan_sip.c, manager_sip_show_peers: registered as the
		// SIPpeers action and terminated with
		// astman_send_list_complete_start(s, m, "PeerlistComplete", total).
		// The completion event name deliberately does not follow the
		// action name — the reason this table cannot be generated from a
		// naming rule. Verified against branch 20; chan_sip is absent
		// from branch 21 onward, so the action exists only through
		// Asterisk 20 and ListSpecFor still reports it because the
		// supported range starts at Asterisk 12.
		action: "SIPpeers",
		spec: ListSpec{
			CompletionEvents: []string{"PeerlistComplete"},
			CountFields:      []string{"ListItems"},
		},
	},
}

// ListSpecFor returns the completion contract of a known AMI list action,
// matched under the protocol's ASCII case folding. The second result
// reports whether the action is known; for an unknown action the caller
// declares its own ListSpec, which is always supported and is how any
// action outside this table is used.
//
// The table records protocol facts verified against Asterisk sources, not
// a capability claim about the server at the other end: a known action may
// be absent from that build, gated by the account's permissions, or
// removed in a later release. The returned ListSpec is a fresh copy, so
// mutating it cannot affect a later call.
//
// The table grows as entries are verified, which is a compatible change:
// answering "unknown" is part of the contract, so a caller must already
// handle it.
func ListSpecFor(action string) (ListSpec, bool) {
	for _, c := range listContracts {
		if equalFoldASCII(c.action, action) {
			return ListSpec{
				CompletionEvents: slices.Clone(c.spec.CompletionEvents),
				CountFields:      slices.Clone(c.spec.CountFields),
			}, true
		}
	}
	return ListSpec{}, false
}
