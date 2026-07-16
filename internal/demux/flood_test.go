package demux

import (
	"math/rand/v2"
	"testing"
)

// Conformance target 4: a synthetic stream shaped like a busy PBX with
// no server-side event filter — the 500-concurrent-call scenario. The
// sustained mix is Newexten/VarSet-heavy channel-lifecycle traffic with
// multi-thousand-event teardown bursts. Assertions: unmatched traffic
// drops at zero queue cost, a deliberately stalled subscriber commits
// its terminal at its exact boundary while a healthy subscriber
// observes complete, ordered delivery, and the routing benchmarks
// below document per-event cost as the headroom evidence.

func floodLimits() Limits {
	return Limits{
		MaxPending:           64,
		MaxSubscriptions:     64,
		MaxSubscriptionBytes: 32 << 20,
		MaxLists:             8,
		MaxListBytes:         64 << 20,
		MaxMatcherNames:      16,
		MaxMatcherBytes:      1024,
		MaxRetirement:        64,
		RetirementLifetime:   30_000_000_000,
	}
}

// pickFloodName draws from a busy-PBX event mix: Newexten and VarSet
// dominate, channel lifecycle fills the rest, and the queue event the
// wallboard cares about is rare.
func pickFloodName(rng *rand.Rand) string {
	r := rng.IntN(1000)
	switch {
	case r < 400:
		return "newexten"
	case r < 700:
		return "varset"
	case r < 999:
		names := []string{"newchannel", "newstate", "newcallerid", "dialbegin", "dialend", "bridgeenter", "bridgeleave", "hangup"}
		return names[rng.IntN(len(names))]
	default:
		return "queuememberstatus"
	}
}

func TestUnfilteredFlood(t *testing.T) {
	m := New[int](floodLimits())
	rng := rand.New(rand.NewPCG(500, 0xca11))

	// The wallboard subscriber: narrow filter, consumed continuously.
	healthy, err := m.Subscribe(Matcher{Events: []string{"queuememberstatus"}}, Caps{Items: 512, Bytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	// The stalled subscriber: an accidental full firehose, never
	// consumed. Its item cap must be the first boundary hit.
	stalled, err := m.Subscribe(Matcher{}, Caps{Items: 512, Bytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}

	var expectHealthy, gotHealthy []int
	payload := 0
	stalledLaggedAt := 0
	unmatchedExpected := uint64(0)

	drainHealthy := func() {
		for {
			v, res := m.Take(healthy)
			if res.State != TakeItem {
				if res.State != TakeEmpty {
					t.Fatalf("healthy subscriber left the stream: %+v", res)
				}
				return
			}
			gotHealthy = append(gotHealthy, v)
		}
	}

	route := func(name string, size int) {
		payload++
		fx := m.Route(Envelope{Class: ClassEvent, Name: name, Size: size, Now: int64(payload)}, payload)
		if fx.Fatal != nil {
			t.Fatalf("flood event fatal: %v", fx.Fatal.Reason)
		}
		if name == "queuememberstatus" {
			expectHealthy = append(expectHealthy, payload)
		}
		if stalledLaggedAt == 0 && payload > 512 {
			// The 513th matching event cannot reserve the 513th item.
			stalledLaggedAt = payload
			_, res := m.Take(stalled)
			if res.State != TakeTerminal || res.Reason != ReasonLagged {
				t.Fatalf("stalled subscriber at its boundary: %+v, want ErrLagged terminal", res)
			}
		}
		if stalledLaggedAt != 0 && payload > stalledLaggedAt && name != "queuememberstatus" {
			unmatchedExpected++
		}
		drainHealthy()
	}

	// Sustained traffic: 500 calls' worth of interleaved lifecycle
	// events (~100 events per Newexten/VarSet-heavy call).
	const sustained = 500 * 100
	for range sustained {
		route(pickFloodName(rng), 200+rng.IntN(700))
	}
	// Teardown bursts: thousands of back-to-back VarSet events.
	for range 3 {
		for range 3000 {
			route("varset", 200+rng.IntN(700))
		}
	}

	// The stalled subscriber lagged at exactly its item boundary.
	if stalledLaggedAt != 513 {
		t.Fatalf("stalled subscriber lagged at event %d, want 513", stalledLaggedAt)
	}
	// The healthy subscriber observed complete, ordered delivery.
	if len(gotHealthy) != len(expectHealthy) {
		t.Fatalf("healthy subscriber saw %d events, want %d", len(gotHealthy), len(expectHealthy))
	}
	for i := range gotHealthy {
		if gotHealthy[i] != expectHealthy[i] {
			t.Fatalf("healthy delivery diverged at %d: got %d, want %d", i, gotHealthy[i], expectHealthy[i])
		}
	}
	if len(expectHealthy) == 0 {
		t.Fatalf("the mix produced no queue events; the scenario is vacuous")
	}
	// Unmatched traffic dropped at zero queue cost, counted.
	if got := m.Counters().Unmatched; got != unmatchedExpected {
		t.Fatalf("unmatched counter %d, want %d", got, unmatchedExpected)
	}
	wantAggregates(t, m, 0, 0)
}

// Routing cost benchmarks — the headroom evidence for the flood
// scenario. The unmatched path is the common case on a busy unfiltered
// connection.

func BenchmarkRouteUnmatched(b *testing.B) {
	m := New[int](floodLimits())
	if _, err := m.Subscribe(Matcher{Events: []string{"queuememberstatus"}}, Caps{Items: 512, Bytes: 2 << 20}); err != nil {
		b.Fatal(err)
	}
	env := Envelope{Class: ClassEvent, Name: "varset", Size: 480}
	b.ReportAllocs()
	for b.Loop() {
		m.Route(env, 0)
	}
}

func BenchmarkRouteDeliveredAndTaken(b *testing.B) {
	m := New[int](floodLimits())
	id, err := m.Subscribe(Matcher{}, Caps{Items: 512, Bytes: 2 << 20})
	if err != nil {
		b.Fatal(err)
	}
	env := Envelope{Class: ClassEvent, Name: "varset", Size: 480}
	b.ReportAllocs()
	for b.Loop() {
		m.Route(env, 0)
		m.Take(id)
	}
}

func BenchmarkRouteFanout8(b *testing.B) {
	m := New[int](floodLimits())
	var ids [8]BranchID
	for i := range ids {
		id, err := m.Subscribe(Matcher{}, Caps{Items: 512, Bytes: 2 << 20})
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = id
	}
	env := Envelope{Class: ClassEvent, Name: "varset", Size: 480}
	b.ReportAllocs()
	for b.Loop() {
		m.Route(env, 0)
		for _, id := range ids {
			m.Take(id)
		}
	}
}
