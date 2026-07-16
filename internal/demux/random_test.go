package demux

import (
	"math/rand/v2"
	"testing"
)

// Conformance target 1: randomized transitions against a reference
// oracle. Two layers share the work. TestRandomizedFanoutAgainstOracle
// models ordinary fan-out and request correlation exactly — delivery
// order, lag boundaries, aggregate charges, completion effects — while
// TestRandomizedFullSurface drives every entity through random legal
// interleavings and asserts lifecycle properties: terminal stability,
// exactly-once resolution, and zero retained state after closing every
// branch. Capacity edges for follows and lists are pinned by the
// deterministic transition tests; the random layers target
// interleaving and lifecycle.

// oracleEntry is one queued delivery the oracle expects.
type oracleEntry struct {
	payload int
	size    int
}

// oracleSub is the reference model of one ordinary subscription.
type oracleSub struct {
	id     BranchID
	names  map[string]bool // nil matches everything
	caps   Caps
	queued []oracleEntry
	bytes  int
	dead   bool
	reason Reason
}

func TestRandomizedFanoutAgainstOracle(t *testing.T) {
	names := []string{"alpha", "beta", "gamma", "delta"}
	for seed := range uint64(16) {
		rng := rand.New(rand.NewPCG(seed, 0x0a11ce))
		lim := Limits{
			MaxPending:           4,
			MaxSubscriptions:     6,
			MaxSubscriptionBytes: 400,
			MaxLists:             2,
			MaxListBytes:         1 << 20,
			MaxMatcherNames:      8,
			MaxMatcherBytes:      256,
			MaxRetirement:        8,
			RetirementLifetime:   1 << 40, // no expiry inside a round
		}
		m := New[int](lim)

		var subs []*oracleSub
		aliveSubs := func() int {
			n := 0
			for _, s := range subs {
				if !s.dead {
					n++
				}
			}
			return n
		}
		oracleAgg := func() int {
			total := 0
			for _, s := range subs {
				total += s.bytes
			}
			return total
		}

		type oraclePend struct {
			tk        Ticket
			id        string
			committed bool
		}
		var pends []*oraclePend
		var absorbable []string // abandoned IDs whose one response is still due
		nextAction := 0
		payload := 0

		checkAggregates := func() {
			t.Helper()
			gotSub, _ := m.Aggregates()
			if gotSub != oracleAgg() {
				t.Fatalf("seed %d: machine aggregate %d, oracle %d", seed, gotSub, oracleAgg())
			}
		}

		for step := range 4000 {
			switch op := rng.IntN(10); op {
			case 0: // subscribe
				if aliveSubs() >= lim.MaxSubscriptions {
					continue
				}
				var matcher Matcher
				var set map[string]bool
				if rng.IntN(3) > 0 { // named matcher two thirds of the time
					set = make(map[string]bool)
					for _, n := range names {
						if rng.IntN(2) == 0 {
							set[n] = true
							matcher.Events = append(matcher.Events, n)
						}
					}
					if len(set) == 0 {
						set = nil
						matcher.Events = nil
					}
				}
				caps := Caps{Items: 1 + rng.IntN(4), Bytes: 20 + rng.IntN(80)}
				id, err := m.Subscribe(matcher, caps)
				if err != nil {
					t.Fatalf("seed %d: Subscribe: %v", seed, err)
				}
				subs = append(subs, &oracleSub{id: id, names: set, caps: caps})

			case 1: // close a random subscription
				if len(subs) == 0 {
					continue
				}
				i := rng.IntN(len(subs))
				m.Close(subs[i].id)
				subs = append(subs[:i], subs[i+1:]...)

			case 2, 3: // take
				if len(subs) == 0 {
					continue
				}
				s := subs[rng.IntN(len(subs))]
				got, res := m.Take(s.id)
				switch {
				case s.dead:
					if res.State != TakeTerminal || res.Reason != s.reason {
						t.Fatalf("seed %d: take on dead sub = %+v, oracle wants terminal %v", seed, res, s.reason)
					}
				case len(s.queued) > 0:
					want := s.queued[0]
					if res.State != TakeItem || got != want.payload {
						t.Fatalf("seed %d: take = (%d, %+v), oracle wants item %d", seed, got, res, want.payload)
					}
					s.queued = s.queued[1:]
					s.bytes -= want.size
				default:
					if res.State != TakeEmpty {
						t.Fatalf("seed %d: take on empty sub = %+v", seed, res)
					}
				}

			case 4, 5, 6: // route one plain event through fan-out
				name := names[rng.IntN(len(names))]
				size := 5 + rng.IntN(30)
				payload++
				fx := m.Route(Envelope{Class: ClassEvent, Name: name, Size: size, Now: int64(step)}, payload)
				if fx.Fatal != nil {
					t.Fatalf("seed %d: plain event fatal: %v", seed, fx.Fatal.Reason)
				}
				for _, s := range subs {
					if s.dead || (s.names != nil && !s.names[name]) {
						continue
					}
					if len(s.queued) < s.caps.Items &&
						s.bytes+size <= s.caps.Bytes &&
						oracleAgg()+size <= lim.MaxSubscriptionBytes {
						s.queued = append(s.queued, oracleEntry{payload, size})
						s.bytes += size
					} else {
						s.dead = true
						s.reason = ReasonLagged
						s.queued = nil
						s.bytes = 0
					}
					if !woken(fx, s.id) {
						t.Fatalf("seed %d: recipient %v missing from the wake set", seed, s.id)
					}
				}

			case 7: // admit and commit a request
				if len(pends) >= lim.MaxPending {
					continue
				}
				nextAction++
				id := "r" + string(rune('0'+nextAction%10)) + "-" + itoa(nextAction)
				tk, err := m.Admit(id, KindRequest, AdmitOptions[int]{})
				if err == ErrRetirementLimit || err == ErrPendingLimit {
					continue
				}
				if err != nil {
					t.Fatalf("seed %d: Admit: %v", seed, err)
				}
				p := &oraclePend{tk: tk, id: id}
				if rng.IntN(2) == 0 {
					m.CommitWrite(tk)
					p.committed = true
				}
				pends = append(pends, p)

			case 8: // respond to a random pending, or absorb a late response
				if len(absorbable) > 0 && rng.IntN(3) == 0 {
					i := rng.IntN(len(absorbable))
					id := absorbable[i]
					fx := m.Route(resp(id, KindRequest, true), 0)
					if fx.Fatal != nil || len(fx.Complete) != 0 {
						t.Fatalf("seed %d: absorbed response produced %+v", seed, fx)
					}
					absorbable = append(absorbable[:i], absorbable[i+1:]...)
					continue
				}
				if len(pends) == 0 {
					continue
				}
				i := rng.IntN(len(pends))
				p := pends[i]
				payload++
				fx := m.Route(resp(p.id, KindRequest, rng.IntN(2) == 0), payload)
				c := completedOrNil(fx, p.tk)
				if c == nil || !c.Delivered || c.Response != payload {
					t.Fatalf("seed %d: response completion missing or wrong: %+v", seed, fx.Complete)
				}
				if !p.committed {
					m.CommitWrite(p.tk) // trailing write resolution
				}
				pends = append(pends[:i], pends[i+1:]...)

			case 9: // abandon a committed pending
				if len(pends) == 0 {
					continue
				}
				i := rng.IntN(len(pends))
				p := pends[i]
				if !p.committed {
					continue
				}
				m.Abandon(p.tk, int64(step))
				absorbable = append(absorbable, p.id)
				pends = append(pends[:i], pends[i+1:]...)
			}
			checkAggregates()
		}

		// Quiesce: the death cascade completes the remaining pendings,
		// terminal branches keep their reasons, and closing everything
		// zeroes the machine.
		fx := m.Kill(ReasonKilled)
		for _, p := range pends {
			c := completedOrNil(fx, p.tk)
			if c == nil || c.Delivered {
				t.Fatalf("seed %d: pending %s not completed by death", seed, p.id)
			}
			if !p.committed {
				m.CommitWrite(p.tk)
			}
		}
		for _, s := range subs {
			wantReason := ReasonClientDead
			if s.dead {
				wantReason = s.reason
			}
			_, res := m.Take(s.id)
			if res.State != TakeTerminal || res.Reason != wantReason {
				t.Fatalf("seed %d: post-kill take = %+v, want terminal %v", seed, res, wantReason)
			}
			m.Close(s.id)
		}
		wantAggregates(t, m, 0, 0)
		wantRetirement(t, m, 0, 0)
		if m.Tickets() != 0 {
			t.Fatalf("seed %d: %d unresolved tickets after quiescence: %v", seed, m.Tickets(), m.debugTickets())
		}
	}
}

func completedOrNil(fx Effects[int], tk Ticket) *Completion[int] {
	for i := range fx.Complete {
		if fx.Complete[i].Ticket == tk {
			return &fx.Complete[i]
		}
	}
	return nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// ---------------------------------------------------------------------
// Full-surface random driver, shared with the fuzz target.

// chooser abstracts the randomness source so the same driver runs under
// math/rand seeds and under fuzz-provided bytes.
type chooser interface {
	intn(n int) int
	exhausted() bool
}

type rngChooser struct {
	r     *rand.Rand
	steps int
	limit int
}

func (c *rngChooser) intn(n int) int { return c.r.IntN(n) }
func (c *rngChooser) exhausted() bool {
	c.steps++
	return c.steps > c.limit
}

type byteChooser struct {
	data []byte
	pos  int
}

func (c *byteChooser) intn(n int) int {
	if c.pos >= len(c.data) {
		return 0
	}
	v := int(c.data[c.pos])
	c.pos++
	return v % n
}

func (c *byteChooser) exhausted() bool { return c.pos >= len(c.data) }

// drvPend mirrors the session's per-ticket knowledge.
type drvPend struct {
	tk        Ticket
	id        string
	kind      Kind
	writeDone bool
	committed bool
	outcome   uint8 // see the out* constants
	folOpen   bool
	lstOpen   bool
	markSeen  bool // list kind: a terminal mark was routed while pending
}

const (
	outOpen uint8 = iota
	outSuccess
	outError
	outDead
	outAbandoned
	outAborted
)

// drvRecord mirrors a live retirement record's outstanding evidence.
type drvRecord struct {
	id       string
	kind     Kind
	needResp bool
	needMark bool
	deadline int64
}

// drvBranch is one adopted branch or subscription the driver may take
// from or close.
type drvBranch struct {
	id       BranchID
	pend     *drvPend // nil for ordinary subscriptions
	closed   bool
	terminal Reason    // first observed terminal reason
	sawEOF   bool      // clean end-of-stream observed
	sawState TakeState // last observed state
}

// driveMachine runs one full random session against the machine. It
// asserts response-correlation expectations exactly, terminal
// stability on every branch, and — after quiescing — that no charge,
// record, ticket, or branch survives.
func driveMachine(t testing.TB, ch chooser) {
	lim := Limits{
		MaxPending:           6,
		MaxSubscriptions:     4,
		MaxSubscriptionBytes: 1 << 20,
		MaxLists:             3,
		MaxListBytes:         1 << 20,
		MaxMatcherNames:      8,
		MaxMatcherBytes:      256,
		MaxRetirement:        6,
		RetirementLifetime:   1000,
	}
	m := New[int](lim)
	names := []string{"alpha", "beta", "gamma"}
	// Follow and list caps are effectively unbounded here: capacity
	// edges are pinned by the deterministic tests, and unbounded caps
	// keep the record shadow exact.
	bigCaps := Caps{Items: 1 << 20, Bytes: 1 << 19}

	var pends []*drvPend
	var records []*drvRecord
	var branches []*drvBranch
	var now int64    // the driver's op counter
	var fedNow int64 // the latest timestamp actually fed to the machine
	nextAction := 0
	dead := false
	payload := 0

	findRecord := func(id string) *drvRecord {
		for _, r := range records {
			if r.id == id {
				return r
			}
		}
		return nil
	}
	dropRecord := func(id string) {
		for i, r := range records {
			if r.id == id {
				records = append(records[:i], records[i+1:]...)
				return
			}
		}
	}
	feedResponse := func(r *drvRecord, success bool) {
		r.needResp = false
		if !success {
			r.needMark = false
		}
		if !r.needResp && !r.needMark {
			dropRecord(r.id)
		}
	}
	feedMark := func(r *drvRecord) {
		if r.kind != KindList {
			return
		}
		r.needMark = false
		if !r.needResp {
			dropRecord(r.id)
		}
	}
	takeBranch := func(b *drvBranch) {
		if b.closed {
			return
		}
		_, res := m.Take(b.id)
		switch res.State {
		case TakeTerminal:
			if b.terminal != ReasonNone && b.terminal != res.Reason {
				t.Fatalf("terminal reason changed from %v to %v", b.terminal, res.Reason)
			}
			if b.sawEOF {
				t.Fatalf("terminal %v after a clean end-of-stream", res.Reason)
			}
			b.terminal = res.Reason
		case TakeEOF:
			if b.terminal != ReasonNone {
				t.Fatalf("end-of-stream after terminal %v", b.terminal)
			}
			b.sawEOF = true
		}
		b.sawState = res.State
	}

	for !ch.exhausted() && !dead {
		now++
		switch op := ch.intn(16); op {
		case 0: // subscribe
			id, err := m.Subscribe(Matcher{Events: names[:ch.intn(len(names)+1)]}, Caps{Items: 1 + ch.intn(8), Bytes: 64 + ch.intn(512)})
			if err == ErrSubscriptionLimit {
				continue
			}
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			branches = append(branches, &drvBranch{id: id})

		case 1: // admit a request, sometimes with a follow
			nextAction++
			id := "a" + itoa(nextAction)
			o := AdmitOptions[int]{}
			withFollow := ch.intn(2) == 0
			if withFollow {
				o.Follow = &FollowOptions{Caps: bigCaps}
			}
			tk, err := m.Admit(id, KindRequest, o)
			if err == ErrPendingLimit || err == ErrRetirementLimit {
				continue
			}
			if err != nil {
				t.Fatalf("Admit: %v", err)
			}
			pends = append(pends, &drvPend{tk: tk, id: id, kind: KindRequest, folOpen: withFollow})

		case 2: // admit a list
			nextAction++
			id := "l" + itoa(nextAction)
			tk, err := m.Admit(id, KindList, AdmitOptions[int]{List: &ListOptions[int]{
				Caps:          bigCaps,
				ObservedBytes: 1 << 30,
			}})
			if err == ErrPendingLimit || err == ErrRetirementLimit || err == ErrListLimit {
				continue
			}
			if err != nil {
				t.Fatalf("Admit list: %v", err)
			}
			pends = append(pends, &drvPend{tk: tk, id: id, kind: KindList, lstOpen: true})

		case 3: // resolve a write
			if len(pends) == 0 {
				continue
			}
			p := pends[ch.intn(len(pends))]
			if p.writeDone {
				continue
			}
			p.writeDone = true
			if ch.intn(4) == 0 && p.outcome == outOpen {
				m.AbortNotSent(p.tk)
				p.outcome = outAborted
				p.folOpen = false
				p.lstOpen = false
			} else {
				m.CommitWrite(p.tk)
				p.committed = true
			}

		case 4: // abandon
			if len(pends) == 0 {
				continue
			}
			p := pends[ch.intn(len(pends))]
			if !p.committed || p.outcome != outOpen {
				continue
			}
			m.Abandon(p.tk, now)
			fedNow = now
			p.outcome = outAbandoned
			p.folOpen = false
			p.lstOpen = false
			records = append(records, &drvRecord{
				id:       p.id,
				kind:     p.kind,
				needResp: true,
				needMark: p.kind == KindList && !p.markSeen,
				deadline: fedNow + lim.RetirementLifetime,
			})

		case 5, 6: // route an event: plain, correlated, or stale
			payload++
			env := Envelope{Class: ClassEvent, Name: names[ch.intn(len(names))], Size: 1 + ch.intn(64), Now: now}
			switch ch.intn(4) {
			case 0: // foreign or absent ActionID
				if ch.intn(2) == 0 {
					env.ActionID = "foreign-7"
				}
			default: // an own ID, live or stale
				if len(pends) == 0 {
					continue
				}
				p := pends[ch.intn(len(pends))]
				env.ActionID = p.id
				env.Own = true
				env.Kind = p.kind
				if p.kind == KindList && ch.intn(4) == 0 {
					env.Mark = MarkComplete
					if ch.intn(4) == 0 {
						env.Mark = MarkCancelled
					}
					// The mark reaches the live branch while the
					// pending is open or after a Success response;
					// afterwards it belongs to the record, if any.
					if p.outcome == outOpen || p.outcome == outSuccess {
						p.markSeen = true
					}
					if r := findRecord(p.id); r != nil {
						feedMark(r)
					}
				}
			}
			fx := m.Route(env, payload)
			fedNow = now
			if fx.Fatal != nil {
				t.Fatalf("event routed to a fatality: %v", fx.Fatal.Reason)
			}

		case 7, 8: // route a response
			payload++
			env := resp("ghost", KindRequest, ch.intn(2) == 0)
			env.Now = now
			env.Size = 1 + ch.intn(64)
			wantFatal := ReasonNone
			var target *drvPend
			switch ch.intn(8) {
			case 0:
				env.Own = false
				env.ActionID = "foreign-7"
				wantFatal = ReasonResponseForeign
			case 1:
				env.ActionID = ""
				wantFatal = ReasonResponseNoID
			case 2:
				wantFatal = ReasonResponseUnmatched // the ghost ID
			default:
				if len(pends) == 0 {
					continue
				}
				target = pends[ch.intn(len(pends))]
				env.ActionID = target.id
				env.Kind = target.kind
				switch {
				case target.outcome == outOpen:
					// live: completion expected
				case findRecord(target.id) != nil:
					// absorbed by the record
				default:
					wantFatal = ReasonResponseUnmatched
				}
			}
			fx := m.Route(env, payload)
			fedNow = now
			if wantFatal != ReasonNone {
				if fx.Fatal == nil || fx.Fatal.Reason != wantFatal {
					t.Fatalf("response expected fatality %v, got %+v", wantFatal, fx.Fatal)
				}
				dead = true
				continue
			}
			if fx.Fatal != nil {
				t.Fatalf("unexpected fatality %v", fx.Fatal.Reason)
			}
			if target.outcome == outOpen {
				c := completedOrNil(fx, target.tk)
				if c == nil || !c.Delivered || c.Response != payload {
					t.Fatalf("live response not delivered to its waiter")
				}
				if env.Success {
					target.outcome = outSuccess
				} else {
					target.outcome = outError
					if target.kind == KindList {
						target.lstOpen = false // released machine-side
					}
				}
				if r := findRecord(target.id); r != nil {
					feedResponse(r, env.Success)
				}
			} else if r := findRecord(target.id); r != nil {
				if len(fx.Complete) != 0 {
					t.Fatalf("absorbed response delivered to a waiter")
				}
				feedResponse(r, env.Success)
			}

		case 9: // adopt after success
			for _, p := range pends {
				if p.outcome != outSuccess {
					continue
				}
				if p.folOpen {
					id := m.AdoptFollow(p.tk)
					p.folOpen = false
					branches = append(branches, &drvBranch{id: id, pend: p})
					break
				}
				if p.lstOpen {
					id := m.AdoptList(p.tk)
					p.lstOpen = false
					branches = append(branches, &drvBranch{id: id, pend: p})
					break
				}
			}

		case 10: // close a follow the session will not adopt
			for _, p := range pends {
				if p.folOpen && (p.outcome == outError || p.outcome == outSuccess || p.outcome == outDead) {
					m.CloseFollow(p.tk)
					p.folOpen = false
					break
				}
			}

		case 11, 12: // take from a random branch
			if len(branches) == 0 {
				continue
			}
			takeBranch(branches[ch.intn(len(branches))])

		case 13: // close a random branch
			if len(branches) == 0 {
				continue
			}
			b := branches[ch.intn(len(branches))]
			if b.closed {
				continue
			}
			// Closing an adopted list that is still streaming — no mark
			// ever reached it — converts its reserved slot into a drain
			// record awaiting the mark.
			if p := b.pend; p != nil && p.kind == KindList &&
				p.outcome == outSuccess && !p.markSeen && b.terminal == ReasonNone {
				records = append(records, &drvRecord{
					id:       p.id,
					kind:     KindList,
					needMark: true,
					deadline: fedNow + lim.RetirementLifetime,
				})
			}
			m.Close(b.id)
			b.closed = true

		case 14: // expire
			if ch.intn(4) == 0 && len(records) > 0 {
				// Beyond every live deadline: the earliest record's
				// kind names the fatality.
				earliest := records[0]
				for _, r := range records[1:] {
					if r.deadline < earliest.deadline ||
						(r.deadline == earliest.deadline && r.id < earliest.id) {
						earliest = r
					}
				}
				fx := m.Expire(now + lim.RetirementLifetime + 1)
				fedNow = now + lim.RetirementLifetime + 1
				if fx.Fatal == nil || fx.Fatal.Reason != ReasonRetirementExpired || fx.Fatal.Kind != earliest.kind {
					t.Fatalf("expiry fatality %+v, want kind %v", fx.Fatal, earliest.kind)
				}
				dead = true
				continue
			}
			fx := m.Expire(now)
			fedNow = now
			if fx.Fatal != nil && len(records) == 0 {
				t.Fatalf("expiry with no live record was fatal")
			} else if fx.Fatal != nil {
				dead = true
			}

		case 15: // rare kill
			if ch.intn(8) != 0 {
				continue
			}
			m.Kill(ReasonKilled)
			dead = true
		}
	}

	// Quiesce: kill if still alive, then resolve every obligation the
	// way the session would, and verify nothing is retained.
	if _, isDead := m.Dead(); !isDead {
		m.Kill(ReasonKilled)
	}
	for _, p := range pends {
		if p.outcome == outOpen {
			p.outcome = outDead
		}
		if !p.writeDone {
			m.CommitWrite(p.tk)
			p.writeDone = true
		}
		if p.folOpen {
			m.CloseFollow(p.tk)
			p.folOpen = false
		}
		if p.lstOpen {
			switch p.outcome {
			case outSuccess:
				id := m.AdoptList(p.tk)
				m.Close(id)
			default:
				m.CloseList(p.tk)
			}
			p.lstOpen = false
		}
	}
	for _, b := range branches {
		if !b.closed {
			m.Close(b.id)
		}
	}
	subBytes, listBytes := m.Aggregates()
	held, recs := m.RetirementLoad()
	subs, follows, lists := m.Branches()
	if subBytes != 0 || listBytes != 0 || held != 0 || recs != 0 ||
		subs != 0 || follows != 0 || lists != 0 || m.Tickets() != 0 {
		t.Fatalf("state retained after quiescence: agg=(%d,%d) retirement=(%d,%d) branches=(%d,%d,%d) tickets=%d",
			subBytes, listBytes, held, recs, subs, follows, lists, m.Tickets())
	}
}

func TestRandomizedFullSurface(t *testing.T) {
	for seed := range uint64(24) {
		ch := &rngChooser{r: rand.New(rand.NewPCG(seed, 0xf0e1)), limit: 4000}
		driveMachine(t, ch)
	}
}
