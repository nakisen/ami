package ami

import (
	"strings"

	"github.com/nakisen/ami/internal/demux"
)

// foldASCII returns s with ASCII uppercase letters lowered, allocating
// only when a change is needed. Event names function as protocol
// identifiers; the session folds them once, at the routing boundary.
func foldASCII(s string) string {
	upper := func(c byte) bool { return 'A' <= c && c <= 'Z' }
	i := 0
	for i < len(s) && !upper(s[i]) {
		i++
	}
	if i == len(s) {
		return s
	}
	b := []byte(s)
	for ; i < len(b); i++ {
		if upper(b[i]) {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// equalFoldASCII reports whether two strings are equal under ASCII case
// folding, the protocol's identifier equivalence.
func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// wireSize is the retained-byte charge for every queue a message
// enters: a conservative reconstruction of the frame's wire size —
// per field the key, the value, ": " and CRLF, plus the terminating
// blank line.
func wireSize(m Message) int {
	size := 2
	for _, f := range m.fields {
		size += len(f.Key) + len(f.Value) + 4
	}
	return size
}

// parseMark parses the EventList header disposition. Events are
// lenient: an unknown disposition routes as an ordinary item.
func parseMark(v string) demux.Mark {
	switch {
	case v == "":
		return demux.MarkNone
	case equalFoldASCII(v, "Complete"):
		return demux.MarkComplete
	case equalFoldASCII(v, "Cancelled"):
		return demux.MarkCancelled
	case equalFoldASCII(v, "start"):
		return demux.MarkStart
	}
	return demux.MarkNone
}

// classify extracts the routing envelope from one parsed message. The
// rules are pinned in docs/demux.md: an Event field beats an
// event-specific Response field, conflicting duplicate envelope fields
// classify as invalid — which the machine treats as fatal — and the
// event name is ASCII-folded here, once. A message carrying neither
// field is unclassifiable and likewise fatal: correlation cannot be
// trusted past it.
func (c *Client) classify(msg Message) demux.Envelope {
	env := demux.Envelope{Size: wireSize(msg), Now: c.now()}

	if ids := msg.Values("ActionID"); len(ids) > 0 {
		for _, id := range ids[1:] {
			if id != ids[0] {
				return env // conflicting ActionIDs: ClassInvalid
			}
		}
		env.ActionID = ids[0]
		env.Own, env.Kind = c.parseActionID(ids[0])
	}

	events := msg.Values("Event")
	responses := msg.Values("Response")
	switch {
	case len(events) > 0:
		name := events[0]
		if name == "" {
			return env
		}
		for _, v := range events[1:] {
			if !equalFoldASCII(v, name) {
				return env
			}
		}
		env.Class = demux.ClassEvent
		env.Name = foldASCII(name)
		env.Mark = parseMark(msg.Get("EventList"))
	case len(responses) > 0:
		for _, v := range responses[1:] {
			if !equalFoldASCII(v, responses[0]) {
				return env
			}
		}
		env.Class = demux.ClassResponse
		env.Success = responseSuccess(msg)
	}
	return env
}

// parseActionID recognizes this session's opaque ActionID scheme: the
// random per-session prefix, a kind discriminator, and a monotonic
// suffix. Anything else — including a mangled own-prefix — is foreign:
// foreign events fan out normally and foreign responses are fatal.
func (c *Client) parseActionID(id string) (own bool, kind demux.Kind) {
	rest, ok := strings.CutPrefix(id, c.idPrefix)
	if !ok || len(rest) < 2 {
		return false, 0
	}
	switch rest[0] {
	case 'r':
		return true, demux.KindRequest
	case 'l':
		return true, demux.KindList
	}
	return false, 0
}
