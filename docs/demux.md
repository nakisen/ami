# Demultiplexer State Machine

Design note for `internal/demux`, the isolated state machine the
`Client` session builds on. This note refines the "Demultiplexing"
section of [design.md](design.md) down to implementable transition
tables; on any conflict, design.md wins and the divergence is a bug to
fix in the same change that finds it. The machine is implemented and
tested in the `Client` slice; this note pins its contract first so the
implementation is checked against a reviewed design rather than grown
alongside it.

## Purpose and scope

The machine owns every decision between "a parsed message arrived" and
"a consumer can take it":

- classification of each inbound message against the routing table;
- pending-request correlation, including the internal keepalive Ping;
- follow, list, and ordinary-subscription registries and their bounded
  queues;
- outcome-unknown retirement and abandoned-list drain records;
- all count/byte accounting, per handle and client-wide aggregate;
- terminal-result commits — exactly one per branch — and the wake set
  the session must signal after each call.

It deliberately excludes: I/O and parsing (`Conn` and `internal/wire`),
blocking and goroutine management (session), ActionID generation (the
session generates; the machine only consumes the parsed identity),
clocks and timers (callers supply a logical timestamp; the session arms
real timers), the public error surface (the machine reports internal
reason codes; the session maps them to the documented sentinel and
typed errors), and user code (never runs inside the machine). Like
`internal/wire`, the package never imports the root package: it is
generic over the message payload `T`, so the root instantiates it with
immutable messages while model tests drive it with trivial payloads.

## Concurrency contract

The machine is a passive, synchronous data structure.

- Every call completes in bounded work: no machine call blocks, waits
  on a consumer, sleeps, or runs user code. Routing one message costs
  the classification plus the matching recipients' reserve-or-terminate
  enqueues — nothing else.
- The session serializes all access under one lock. The single reader
  goroutine is the machine's only caller of `Route`; control-plane
  operations — admission, registration, adoption, consumption, close,
  expiry — enter briefly at their linearization points under the same
  lock. The reader goroutine is therefore the machine's only router,
  but not its only caller: registration never round-trips through the
  read loop, which would deadlock registration against a quiet
  connection.
- Effects are data, not actions. A call returns the wake set, response
  completions, and any fatality; the session applies them — signaling
  waiters, terminating the client — after releasing the lock.
- Time is an input. Calls that can observe expiry carry a monotonic
  timestamp; `NextDeadline` tells the session when to arm its earliest
  retirement timer. The machine never reads a wall clock, which keeps
  model tests synchronous and confines `testing/synctest` to the
  session layer.

## Identity: ActionIDs at the machine boundary

The session generates every ActionID as an opaque token: a random
per-session prefix (`crypto/rand.Text`-derived), an internal kind
discriminator — request or list — and a monotonic suffix; the exact
layout is a session concern and consumers must not parse it. Before
routing, the session reduces each inbound ActionID to the facts the
machine consumes: `Own` (carries this session's prefix), `Kind`
(request or list; meaningful only when `Own`), and the verbatim string
as the correlation key.

Correlation is response-strict and event-lenient:

| Inbound | Own request-kind | Own list-kind | Foreign or absent ActionID |
|---|---|---|---|
| Response | correlate or fatal | correlate or fatal | fatal |
| Event | follow / quarantine / ordinary | list / quarantine / late-discard | ordinary fan-out |

Responses are exactly-once request accounting: a response that matches
no active response branch and no live retirement record — unknown,
duplicate, foreign-prefixed, or missing its ActionID — is a terminal
correlation error, because the session can no longer prove which
request anything belongs to. Events are broadcast: Asterisk tags events
with the ActionID of whichever session's action caused them, so a
foreign or absent ActionID is normal and routes as an ordinary event,
and an own request-kind ActionID with no remaining state simply loses
its follow branch and fans out ordinarily. Own list-kind events with no
remaining state are discarded silently and countedly, forever — the
kind discriminator exists precisely so late list traffic is never
misdelivered to ordinary subscriptions and no set of dead list IDs must
be retained.

## Envelope classification

The session extracts one `Envelope` per message; classification is
total input for the machine:

- A message containing `Event:` is an event, even when it also carries
  an event-specific `Response:` field, as `OriginateResponse` does. A
  message containing `Response:` without `Event:` is a response.
  Neither field present is an invalid envelope: fatal.
- Conflicting duplicate envelope fields are an invalid envelope, fatal
  — role-sensitively: on an event-class message the classifying fields
  are `Event:`, `ActionID:`, and `EventList:`; on a response-class
  message they are `Response:` and `ActionID:`. Repeated `Response:`
  fields inside an event-class message are ordered payload, preserved
  verbatim and never envelope. Identical duplicates are tolerated
  everywhere.
- The `EventList:` value is matched case-insensitively: `Complete` is a
  clean terminal mark, `cancelled` a cancellation mark, and `start` is
  confirmatory only — a list response without it still arms the
  stream, because the header convention postdates several list actions.
- Event names and declared completion names are matched
  ASCII-case-insensitively, folded once at registration. Header values
  are otherwise data, but event names function as protocol identifiers,
  and a byte-exact mismatch here would be a silent, costly footgun.
- `Size` is the retained-byte charge for accounting: the frame's wire
  size, Σ(len(key)+len(value)+4)+2 over the message's fields, computed
  once by the session and used for every queue charge.

The wire layer has already normalized legacy `Response: Follows`
command frames into ordinary field messages, so no legacy-framing case
exists at this boundary.

## Entities and transitions

Five entity families. "Fatal" always means: the machine reports a
fatality, the session terminates the client, and the death cascade
commits every remaining branch exactly once with the same root cause.

### Pending request

One per admitted action, including the internal keepalive Ping, which
uses a permanently reserved slot and is otherwise ordinary. Admission
reserves the pending slot **and** the retirement/drain record this
action could ever need; when either reservation is unavailable, the
action is rejected before any byte is written — definitely-not-sent.
Live records are never evicted to admit new work.

| From | Trigger | To | Notes |
|---|---|---|---|
| — | `Admit` | Reserved | pending and retirement slots reserved; the follow or list branch is registered and live from this point |
| Reserved | `CommitWrite` | InFlight | write fully emitted |
| Reserved | own response routed | Completed | legal: the response outran the writer's `CommitWrite`, which then becomes a no-op |
| Reserved | `AbortNotSent` | released | zero bytes reached the wire; every reservation and provisional branch is released |
| InFlight | own response routed | Completed | the single-use response branch commits exactly once |
| InFlight | `Abandon` | retirement record | complete write, no response, request context over: outcome unknown |
| Completed | duplicate own response routed | — | fatal (correlation) |
| any | `Kill` | released | the waiter is completed with the client root cause |

`AbortNotSent` after a response was seen is an invariant violation: a
server cannot answer an unsent action. For request-kind pendings the
retirement reservation is released at `Completed`; for list-kind it is
retained until both the initial response and the terminal mark are
resolved — a list can still require drain after its successful
response (overflow, caller close), and a late response must always
find correlated state, never a fatality.

### Follow branch

Registered atomically with a request-kind pending when the dispatch
carries a `FollowSpec`; routing-active from registration so nothing
outruns it.

| From | Trigger | To |
|---|---|---|
| — | `Admit` with a follow | Provisional (routing-active) |
| Provisional | `AdoptFollow` (`Do` returned nil) | Owned |
| Provisional | `CloseFollow` (`Do` returned non-nil) | released: queue discarded, charges released |
| Provisional, Owned | matching ActionID event | enqueue (reserve-or-terminate); the event also fans out to matching ordinary subscriptions |
| Provisional, Owned | declared completion event | CompletedDraining: the terminal event is charged and enqueued first; if its reservation fails, Lagged wins instead |
| Provisional, Owned | reservation failure | Lagged: queue discarded, charges released |
| Owned | `Close` | Closed: queue discarded |
| any active | `Kill` | ClientDead: queue discarded |
| CompletedDraining | `Take` drains the last item | drained: `Take` reports clean end-of-stream thereafter |

A follow may complete while still provisional; adoption hands over the
already-terminal branch, mirroring the list rule that a returned handle
may already be cleanly complete.

### List branch

Registered atomically with a list-kind pending; routing-active from
registration, so items — and even the terminal mark — arriving before
the initial response are tolerated and buffered. `Buffering*` below
covers Buffering plus its buffered-terminal variants.

| From | Trigger | To |
|---|---|---|
| — | `Admit` (list kind) | Buffering |
| Buffering | correlated non-terminal event | enqueue item (reserve-or-terminate) |
| Buffering | terminal mark (declared name or `EventList`) | BufferedComplete or BufferedCancelled |
| Buffering* | response `Success` routed | Streaming, or the buffered terminal outcome — the returned handle may already be complete or cancelled |
| Buffering* | response `Error` routed | released: buffered items discarded, charges released; stray later events fall to late-list discard |
| Streaming | correlated non-terminal event | enqueue item (reserve-or-terminate) |
| Streaming | completion (declared name or `EventList: Complete`) | CompletedDraining: a declared count field, when configured and present, is verified first — mismatch is Failed(count); the completion event is charged and stored for `Completion()`, not enqueued as an item |
| Streaming | `EventList: cancelled` | Cancelled: queued items discarded |
| Buffering*, Streaming | reservation failure | Failed(overflow) plus a drain record absorbing the remainder until terminal evidence or expiry |
| Streaming | caller `Close`, abandoned adapter | Closed plus a drain record until terminal evidence or expiry |
| any active | `Kill` | ClientDead: queued items discarded |
| CompletedDraining | `Take` drains the last item | drained: clean end-of-stream |

Clean completion preserves queued items until drained; every other
terminal discards them at commit time and releases their charges. List
budgets are per-list items, per-list queued bytes, a cumulative
observed-bytes budget bounding what the remote may stream through one
list regardless of drain rate, and the client-wide retained list
aggregate.

### Ordinary subscription

| From | Trigger | To |
|---|---|---|
| — | `Subscribe` | Active: the matcher is folded and bounded (names/bytes), local caps validated, client-wide subscription count and aggregate capacity checked |
| Active | matching event | enqueue (reserve-or-terminate: local items, local bytes, client-wide queued subscription bytes) |
| Active | reservation failure | Lagged: queue discarded, charges released; fan-out continues with the remaining recipients |
| Active | `Close` | Closed: queue discarded |
| Active | `Kill` | ClientDead: queue discarded |

Fan-out visits matching subscriptions in stable registration order over
an ordered registry — never map iteration — so a capacity victim is
selected deterministically and one lagging subscriber never delays or
starves another.

### Retirement / drain record

Created only from the slot reserved at admission: by `Abandon` (either
kind) and by a list's drain conversions — overflow before or after the
response, caller close. While a record is live, its ActionID's traffic
is quarantined — absorbed and counted, never delivered, per the
routing table's outcome-unknown row. A record is released only by
correlated terminal evidence, tracked as two facts (amended when the
implementation landed: the original mark-only list rule stranded the
late response on every path where the mark resolved first, and a
stranded response is fatal by the response-strict rule):

| Kind | Releasing evidence |
|---|---|
| request | its response (absorbed, never delivered) |
| list | its terminal mark — declared completion name, `EventList: Complete`, or `cancelled` — **and** its response, so a late response always has a home; an `Error` response resolves both at once, because a rejected list never streams |

A record is created holding whatever evidence its action already
observed — a drain converted after the response awaits only the mark,
an abandonment after a buffered mark awaits only the response — and a
slot whose evidence completes while still held releases without ever
becoming a record.

After release, later events carrying that request-kind ActionID fan out
as ordinary events — the same treatment an acknowledged action's events
receive — and later list-kind events fall to permanent late-list
discard. If a record's lifetime expires before evidence arrives, the
client closes with the retirement cause (`RetirementError` wrapping
`ErrRetirementExpired`): the machine never forgets a live record, so a
later message can never be reclassified as an ordinary event after an
unproven quarantine. `NextDeadline` and `Expire` give the session the
earliest deadline to arm and the entry point to commit it.

## The routing function

`Route` is total: every envelope reaches exactly one arm, asserted at
the end of classification.

1. Invalid envelope → fatal (envelope).
2. Response: not own, or no ActionID → fatal (correlation). Own: an
   active pending commits its response branch (per kind, tables above);
   else a live record absorbs it (for request-kind, this is releasing
   evidence); else fatal (correlation) — unknown or duplicate.
3. Event, own list-kind: the active list branch takes it (item or
   terminal mark); else a live record absorbs it (a terminal mark is
   releasing evidence); else silent, counted late-list discard.
4. Event, own request-kind: a live record quarantines it (absorbed,
   never ordinary delivery). Else an active follow branch enqueues it
   **and** it fans out to matching ordinary subscriptions. Else
   ordinary fan-out only.
5. Event, foreign or no ActionID → ordinary fan-out.

Ordinary fan-out folds the event name and walks the ordered
subscription registry with one reserve-or-terminate enqueue per match.
No match means the message is dropped at zero queue cost — on a busy
unfiltered connection this is the common case and the cheapest path
through the machine.

## Queues and accounting

- One delivery primitive everywhere: **reserve-or-terminate**. Before
  enqueue, the recipient's local item count, local bytes, and its
  family's client-wide aggregate are reserved together; if any
  reservation fails, that recipient alone commits its overflow terminal
  (`Lagged` for subscriptions and follows, overflow failure for lists)
  before anything is enqueued, its queue is discarded and its charges
  released, and routing continues. The reader never waits; a slow
  consumer costs itself, never the session.
- Charges are conservative: every queue charges the full message
  `Size`, and aggregates are the sum of live queue charges. Shared
  immutable storage is deliberately overcounted and retained memory is
  never undercounted.
- `Take` removes one queued item, releases its local and aggregate
  charges, and reports the branch state. After a clean terminal (follow
  or list completion) the queue drains to a clean end-of-stream; every
  other terminal discarded its queue at commit time. Single-consumer
  discipline on `Take` is the session's contract; the machine only
  executes it.
- Releasing a branch — terminal or close — releases every charge
  exactly once; after every branch is closed, all aggregates are zero,
  asserted.

## Invariants

Asserted at every transition, in tests and in production:

1. **Totality** — every `Route` reaches exactly one classification arm.
2. **One terminal per branch** — a terminal commit asserts the branch
   was not already terminal (the conformance property).
3. **Reader liveness** — structural: no machine call blocks, waits on a
   consumer, or runs user code; the only delivery primitive is
   reserve-or-terminate.
4. **Accounting exactness** — every aggregate equals the sum of its
   live charges; every charge is released exactly once; no counter goes
   negative; every cap holds after every transition.
5. **Reservation discipline** — retirement records exist only in slots
   reserved at admission; live records are never evicted; a record
   leaves only through evidence, expiry fatality, or client death.
6. **No delivery after terminal** — enqueue to a terminal branch is a
   violation; quarantined ActionIDs never reach ordinary delivery.
7. **Deterministic fan-out** — stable registration order, never map
   iteration.

A violated invariant panics inside `internal/demux`. The session's read
loop converts any read-loop panic into client death with the cause
preserved through `Err()`: the host application stays alive, the client
refuses to continue on corrupted correlation state, and the bug stays
loud — honest failure without turning a library bug into a process
crash.

## Conformance targets

Model-based tests drive the machine synchronously — no goroutines, no
clocks — with a reference oracle where randomized:

1. **Randomized transitions against an oracle** — random interleavings
   of control-plane calls and routed envelopes; property: every
   request, list, and subscription commits exactly one terminal result,
   and final aggregates are zero after closing every branch.
2. **Fuzzed envelope streams** — the demultiplexer fuzz corpus from
   design.md's testing strategy: arbitrary envelope sequences never
   panic, never violate totality, and never leak charges.
3. **QueueStatus interleave** (the wallboard scenario) — a subscription
   matching queue events, then a list whose correlated items carry the
   same event names, interleaved: list-correlated items reach only the
   list, uncorrelated live events reach only the subscription, wire
   order holds within each handle, completion commits exactly once, and
   a declared count is verified.
4. **500-call unfiltered flood** — a synthetic stream shaped like a
   busy PBX with no server-side filter: hundreds to ~1500 events/s
   sustained with multi-thousand-event teardown bursts. Assertions:
   unmatched traffic drops at zero queue cost; a deliberately stalled
   subscriber commits `ErrLagged` at its exact boundary while a healthy
   subscriber observes complete, ordered delivery; a routing benchmark
   documents per-event cost as the headroom evidence.
5. **Exact boundaries** — count and byte caps at limit and limit+1 for
   subscription, follow, list, and every aggregate; the victim's
   charges release fully; "`ErrLagged` wins" when a terminal completion
   event cannot reserve.
6. **Late and stray traffic** — abandonment quarantines correlated
   events (never ordinary delivery); the late response releases the
   record and subsequent events fan out ordinarily; duplicate and
   unknown responses are fatal; foreign-ActionID events deliver
   ordinarily; stale list-kind events discard silently, forever.
7. **Early list arrival** — items and the terminal mark before the
   initial response; a success response returns an already-terminal
   handle; an error response discards and releases every buffered
   charge.
8. **Retirement discipline** — reservation exhaustion rejects admission
   as definitely-not-sent; expiry without evidence is fatal with the
   retirement cause; evidence releases without delivering.
9. **Death cascade** — `Kill` commits every active branch exactly once
   with the same root cause, discards queues, wakes every waiter, and
   zeroes the aggregates.

Session-level timing — retirement timers, keepalive interaction — is
tested separately with `testing/synctest`; nothing in this package
needs a clock.

## Sizing model (informative)

A subscription queue must absorb `arrival rate × consumer stall` plus
one burst. The v0 defaults (512 items / 2 MiB per subscription, 32 MiB
client-wide) are hiccup absorbers for in-memory consumers — GC pauses,
scheduling — not capacity for per-event blocking I/O. A consumer that
performs blocking I/O per event must batch or raise its buffer; a
monitoring subscription on a busy PBX (hundreds to thousands of
events/s) should size for its measured burst, with the client-wide
aggregate as the deliberate spend ceiling. The consumer-facing sizing
guide ships with the `Client` slice documentation, and the
per-subscription defaults are revisited against it there.

## API sketch (signatures only)

Names are indicative; the semantics above are the contract. The
session's per-ticket resolution obligations — one write resolution,
one outcome resolution, and the adoption or closure of any follow or
list branch — are pinned in the package documentation of
`internal/demux`.

```go
package demux

type Kind uint8  // KindRequest, KindList
type Class uint8 // ClassEvent, ClassResponse, ClassInvalid
type Mark uint8  // MarkNone, MarkStart, MarkComplete, MarkCancelled

type Envelope struct {
    Class    Class
    Name     string // folded event name, when Class == ClassEvent
    ActionID string // verbatim correlation key; empty when absent
    Own      bool   // carries this session's prefix
    Kind     Kind   // parsed discriminator; meaningful only when Own
    Mark     Mark   // EventList terminal marker
    Size     int    // retained-byte charge
    Now      int64  // monotonic timestamp
}

type Machine[T any] struct{ /* registries, queues, accounting */ }

func New[T any](lim Limits) *Machine[T]

// Wire plane — the reader goroutine only.
func (m *Machine[T]) Route(env Envelope, msg T) Effects

// Control plane — API goroutines at their linearization points.
func (m *Machine[T]) Admit(actionID string, kind Kind, o AdmitOptions) (Ticket, error)
func (m *Machine[T]) CommitWrite(t Ticket)
func (m *Machine[T]) AbortNotSent(t Ticket)
func (m *Machine[T]) Abandon(t Ticket, now int64)
func (m *Machine[T]) AdoptFollow(t Ticket) BranchID
func (m *Machine[T]) CloseFollow(t Ticket)
func (m *Machine[T]) AdoptList(t Ticket) BranchID
func (m *Machine[T]) CloseList(t Ticket)
func (m *Machine[T]) Subscribe(match Matcher, caps Caps) (BranchID, error)
func (m *Machine[T]) Close(id BranchID)
func (m *Machine[T]) Take(id BranchID) (T, TakeResult)
func (m *Machine[T]) NextDeadline() (int64, bool)
func (m *Machine[T]) Expire(now int64) Effects
func (m *Machine[T]) Kill(cause Reason) Effects

type Effects struct {
    Wake     []BranchID   // consumers to signal after the lock is released
    Complete []Completion // response branches released to their waiters
    Fatal    *Fatality    // the session must terminate the client
}
```

## Deferred to the Client slice

- Numeric defaults for design.md's open question 1 as they reach this
  machine: pending count, matcher names/bytes, per-list and aggregate
  list bytes, retirement/drain count and lifetime. The `Limits` struct
  here consumes them; the session anchors them.
- The `Do`/`StartList` error contract's exact mapping onto
  `AdoptFollow`/`CloseFollow` and response-error surfaces.
- The public mapping from machine reason codes to the documented
  sentinel and typed errors.
- The consumer-facing busy-system sizing guide and the revisit of the
  per-subscription queue defaults against it.
