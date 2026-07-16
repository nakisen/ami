# AMI Client Library Design

A modern, open-source, zero-dependency Go client library for the Asterisk
Manager Interface (AMI). The library is generic and carries no concepts
from any particular consuming application. It is hosted under the personal
account `github.com/nakisen` with module path `github.com/nakisen/ami`;
the root Go package is `ami`.

This document was promoted from the pre-repository design draft on
2026-07-16 and is the authoritative design reference for the library. It
must stay publishable: no private fixtures, live captures, customer or
partner identifiers, credentials, or operational data. The dated sections
record when each direction was decided.

## Decided direction (2026-07-14)

- The library lives in its own repository: it publishes version tags,
  consumers pin a released version without `replace`, and a local
  uncommitted `go.work` is used only during explicitly scoped lockstep
  development with a consumer.
- The independent lifecycle is justified by a stable public module path,
  independent open-source licensing and security policy, clean public
  history from the first commit, and an API boundary that must remain
  generic.
- The design is from first principles. It is explicitly not an extraction
  of any existing application's internal AMI implementation.
- The v0 session surface is centered on `Do`, `Subscribe`, and
  `StartList`, with explicit `Subscription` and `List` handles. An eager
  `iter.Seq2` is not the resource-owning primitive.
- User code never runs on the AMI read loop. Event callbacks are not a v0
  primitive; hook-style consumption is implemented by the consumer, or
  later by a blocking adapter over `Subscription`.
- Application-level Ping keepalive is required in v0. It detects a
  connection that remains locally open but no longer carries successful
  AMI request/response traffic.
- v0 authentication supports plain Login and an explicitly selected legacy
  MD5 challenge/response mode. There is no automatic authentication
  downgrade, and MD5 is not described as transport security.
- Automatic reconnect remains outside the core. A reconnect creates a new
  `Client`; applications must establish a fresh snapshot and generation.
- `amix`, `amitap`, `Redialer`, and an asynchronous `OnEvent` registry are
  deferred beyond v0. Their absence does not weaken the raw core or public
  test server.

## Decided direction (2026-07-16)

Adopted after an ecosystem survey (eight Go AMI libraries; the
Asterisk-Java, AsterNET, Panoramisk, PAMI, and NAMI lineages; current Rust
crates) and source-level verification of protocol behavior against
Asterisk. The survey's key evidence is kept under "Prior art" below.

- Hosting: the personal account `github.com/nakisen`, with module path
  `github.com/nakisen/ami`. No dedicated organization.
- License: Apache-2.0.
- Go floor: `go 1.27` at the first public release, under a README-declared
  policy that the library tracks the latest stable Go release. Development
  may use `toolchain go1.27rc1`; no public tag is published before Go 1.27
  is stable. Generic methods stay out of the v0 core and are reserved for
  the future `amix` typed layer.
- `ListSpec` completion detection is hybrid: `EventList: Complete` always
  commits clean completion and `EventList: cancelled` always commits
  cancellation; declared completion event names remain primary and are
  required for actions predating the header convention. An empty
  `CompletionEvents` selects the pure header convention.
- The single-use iterator adapters have a pinned contract: `Next` returns
  `io.EOF` on clean end; `All` never yields `io.EOF`, ends silently on
  clean completion, and yields exactly one final (zero value, error) pair
  on terminal failure.
- Keepalive defaults: 30s interval, 5s write-attempt deadline, 10s
  response timeout. The zero-value `KeepaliveConfig` selects the defaults;
  disabling is the explicit `Disabled: true`.
- `Dial` sends `Events: off` at Login when no `EventMask` is configured;
  an explicit mask is applied at Login with its pre-subscription gap
  documented.
- Initial limit anchors: banner 1 KiB (pre-authentication, deliberately
  the tightest), inbound line 32 KiB, inbound message 128 KiB,
  per-subscription queue 512 events / 2 MiB, client-wide retained
  subscription bytes 32 MiB. Headroom applies to post-authentication
  byte/count ceilings only; time-based dimensions stay tight.
- `ErrLagged`'s close-on-overflow contract is final for v0; no lossy
  skip-and-continue subscription mode.
- The demultiplexer, correlation, and retirement/drain records form one
  isolated, table-driven state machine under `internal/`, verified by
  model-based tests.
- `amitest` v0 exposes a programmatic Go builder API only; no text script
  format is frozen.
- Live integration matrix: Asterisk 18, 20, 22, 23; legacy-only behaviors
  run against version-tagged synthetic fixtures.
- Minor surface decisions: `Message.Get` convenience accessor;
  diagnostics-only `Client.Banner()`; the blocking `Consume(ctx, handler)`
  adapter is targeted at v0.1.

## Goals

- Correct, bounded, honest AMI protocol and session handling on top of the
  current Go standard library.
- A small public API for connection setup, action/response correlation,
  action-specific list streaming, event subscription, keepalive, and
  lifecycle.
- Explicit action outcome semantics that distinguish definitely-not-sent
  from may-have-executed.
- First-class testability for consumers through a public fake server and
  for the library through synthetic fixtures, fuzzing, `testing/synctest`,
  race testing, and a real-Asterisk integration matrix.

## Non-goals

- No state derivation: extension, queue, or channel state models belong to
  applications.
- No claim that AMI list actions are atomic snapshots or that application
  reducers are idempotent.
- No automatic reconnection, backoff, state replay, or resynchronization
  in the core.
- No full typed catalog of every AMI action/event in v0.
- No authorization policy. The library can send arbitrary AMI actions
  allowed by the supplied AMI identity; applications remain responsible
  for authentication, authorization, target validation, capability checks,
  and audit.
- No logging framework, metrics backend, telemetry, or network egress
  other than the configured AMI endpoint.

## Design principles

1. Zero runtime dependencies. The standard library is sufficient for the
   v0 core and test support.
2. Bound retained work by both count and bytes, including client-wide
   aggregate limits. Treat the remote server as untrusted input.
3. Put `context.Context` on every blocking operation whose deadline
   belongs to the caller. Do not hide blocking deadlines inside `Close`.
4. Keep protocol/session concerns in the library and reconnect/resync/
   state policy in the application. Produce honest terminal signals rather
   than hiding gaps.
5. Never run user code on the read loop. This includes handlers, arbitrary
   match predicates, and user-supplied logging handlers. This is a
   protocol-survival requirement, not only a liveness preference: Asterisk
   disconnects manager sessions whose socket writes block past the
   configured `writetimeout` (100 ms by default), so a reader stalled on
   consumer code kills the connection server-side.
6. Make `Action`, `Message`, `Response`, and `Event` immutable after
   construction so concurrent sends and fan-out are race-free.
7. Never silently drop events, guess correlation, retry state-changing
   actions, or downgrade authentication/TLS behavior.

## Prior art (2026-07-16 survey)

The design was checked against the existing ecosystem before
implementation; the key evidence is kept here as rationale anchors.

- Of eight surveyed Go AMI libraries, none reads the generic `EventList`
  header, six lose duplicate headers to map-based messages, none ships a
  public fake AMI server, and backpressure everywhere degenerates to
  either blocking the reader or silent drops. Correctness — not dependency
  count — is the differentiator: seven of eight are already stdlib-only.
- Asterisk-Java, the most mature AMI client anywhere, converged in 2021 on
  the same reader-isolated bounded-queue architecture this design starts
  from, but drops events silently on overflow and serializes all listeners
  behind one dispatch thread
  ([asterisk-java#592](https://github.com/asterisk-java/asterisk-java/issues/592)).
  Its map-shaped handling of duplicate fields from Asterisk 20 escalated
  into a dispatcher-killing exception and endless reconnect loop
  ([asterisk-java#793](https://github.com/asterisk-java/asterisk-java/issues/793)).
- Heuristic list-completion detection hangs forever when Asterisk deviates
  from convention (panoramisk
  [#54](https://github.com/gawel/panoramisk/issues/54), PAMI
  [#182](https://github.com/marcelog/PAMI/issues/182)); `ListSpec`'s
  declarative-plus-header hybrid exists because of these failures.
- Auto-reconnect hiding snapshot invalidation is a documented failure mode
  (AsterNET [#190](https://github.com/AsterNET/AsterNET/issues/190),
  [#193](https://github.com/AsterNET/AsterNET/issues/193) — where "create
  a new instance each time" is the community workaround this design
  promotes to a rule).
- Protocol facts verified against Asterisk sources: repeated keys are
  legal and `QueueRule` even emits order-dependent pairs
  ([ASTERISK-27072](https://issues-archive.asterisk.org/ASTERISK-27072)),
  so only an ordered field list is faithful; `StatusComplete` emits its
  count under both `ListItems` and `Items`, motivating `CountFields`
  alternatives; a Login without an `Events` header enables the user's full
  `readperm` mask, motivating the events-off default; the `Command` action
  switched to `Output:` framing in Asterisk 14.2 (AMI 3.1.0) while
  `--END COMMAND--` framing survives only in older releases, so both
  framings stay supported.

## Architecture

### Message model

- A message is an ordered list of fields, not a map. Duplicate keys are
  legal and meaningful (`Variable:`, `ChanVariable:`, `Output:`); wire
  order is preserved.
- Keys are matched case-insensitively. Values preserve meaningful
  emptiness and whitespace.
- Accessors distinguish absence from an empty value: `Get(key) string`
  returns the first value or the empty string for the common case,
  `Lookup(key) (string, bool)` distinguishes absence,
  `Values(key) []string` returns a defensive copy, and
  `Fields() iter.Seq2[string, string]` iterates the immutable sequence.
- `Action`, `Response`, and `Event` are validated immutable views over the
  common field representation. A message containing `Event:` is classified
  as an event even when it also carries an event-specific `Response:`
  field, as `OriginateResponse` does.
- Conflicting duplicate envelope fields or ActionIDs are rejected.
  Package-level action construction rejects CR/LF injection, malformed
  keys, and reserved `Action`/`ActionID` fields. Client-configured
  count/byte limits are validated later, before dispatch, and low-level
  connection limits are validated by `WriteAction`.
- The parser handles both `Command` output framings: legacy
  `Response: Follows` terminated by `--END COMMAND--`, and the repeated
  `Output:` header form. Both are covered by line, item, and total-output
  limits.

### Layer 1: `ami.Conn` (public, low level)

`Conn` owns one already-established `net.Conn`, which may be plain TCP or
TLS: banner read, framing, `ReadMessage`, and `WriteAction`. A successful
`NewConn` takes ownership of that connection. If constructor validation
fails, it performs no I/O and leaves ownership with the caller. `Dial`
owns TCP dialing and the optional TLS handshake before constructing the
high-level session. `Conn` is synchronous and single-owner so advanced
users can build a different session layer and tests can target framing
directly.

- A canceled operation before I/O starts leaves the connection usable.
- Cancellation after a read has consumed any frame bytes, or after a write
  has begun, closes the connection unless that operation already
  completed. A caller cannot resume a partially consumed or emitted AMI
  frame.
- A partial or otherwise ambiguous write poisons and closes the
  connection.
- A protocol or inbound-limit violation closes the connection because
  subsequent framing cannot be trusted.
- `Conn` does not start background goroutines or perform login,
  correlation, subscriptions, or keepalive.
- The low-level `WriteAction` accepts a separate caller-owned ActionID
  string, including empty, so an advanced session can implement its own
  correlation. The high-level `Client` prohibition on caller-supplied
  ActionIDs does not apply to this explicit low-level escape hatch.

### Layer 2: `ami.Client` (session)

`Client` owns login, ActionID generation, pending correlation,
demultiplexing, subscriptions, list state, Ping keepalive, and terminal
lifecycle.

- Exactly one goroutine reads AMI messages.
- Writes are serialized; `Do` and `StartList` are safe for concurrent use.
- The client generates an opaque ActionID containing a random per-session
  prefix, an internal request-kind discriminator, and a monotonic suffix.
  Package-level action construction never assigns an ActionID; consumers
  must not parse the internal form.
- Pending correlation and any requested async follow subscription are
  registered before the first action byte can be written, so an immediate
  response or completion cannot outrun registration.
- Action values are reusable descriptions; each dispatch receives a new
  client-owned ActionID. The client never accepts a caller-supplied
  ActionID through the high-level API.
- A normal action with async follow has one composite dispatch state whose
  response and follow branches terminate independently. Committing the
  immediate response does not retire a still-active follow branch.
- Every caller-owned slice or option value retained beyond a public call
  is validated and defensively copied before registration or dispatch.
  Later caller mutation cannot change read-loop routing or create a race.

### Action dispatch and outcome taxonomy

`Do` sends one action and waits for its immediate AMI response. Its error
contract preserves both the underlying cause and whether Asterisk may have
executed the action:

| Phase | Connection | Outcome classification |
|---|---|---|
| Context ends while waiting for admission/write ownership, before any byte | Remains usable | Definitely not sent |
| Transport fails with zero action bytes written | Closed | Definitely not sent |
| Any byte may have been written but the complete write is not proven | Closed | May have executed |
| Complete action written; the request ends from context or client death before a response wins | Temporarily usable under bounded retirement when the transport survives; otherwise closed | May have executed |
| `Response: Error` received | Remains usable | AMI-reported failure; absence of side effects is action-specific |
| Success response received | Remains usable | Immediate AMI acknowledgement; later completion may still exist |

- `RequestError` exposes a machine-readable phase and `MayHaveExecuted()`
  while supporting `errors.Is` for the wrapped context or transport cause.
  `errors.Is(err, ErrOutcomeUnknown)` is true exactly when
  `MayHaveExecuted()` is true.
- Response-versus-cancellation races have one linearized winner. A success
  or rejection already committed to the request is not replaced by a
  concurrent context cancellation.
- The library never retries an action automatically.
- Capacity for every possible outcome-unknown retirement/drain record is
  reserved before the first action byte is written. Records are never
  evicted to admit new work; exhaustion rejects admission as
  definitely-not-sent.
- An outcome-unknown record suppresses its late correlated traffic. If its
  retirement deadline expires before correlated terminal evidence permits
  release, a sanitized `RetirementError` wrapping `ErrRetirementExpired`
  becomes the client root cause. The client closes before forgetting the
  record, so a later message cannot be reclassified as an ordinary event;
  the original request context error is not reused as this later
  client-wide cause.
- The opaque request-kind discriminator permanently distinguishes list IDs
  from normal action IDs without retaining an unbounded set. Late events
  for a completed/retired list ID are never delivered to ordinary
  subscriptions.
- A response carrying an unknown or no-longer-valid ActionID is a
  protocol/correlation failure; the client does not guess a request.
- An ordinary event related to a successfully acknowledged non-list action
  remains eligible for normal event subscriptions. A `MayHaveExecuted`
  error requires application reconciliation because its correlated traffic
  is suppressed during bounded retirement.

AMI actions such as asynchronous Originate can produce a correlated event
after the immediate response. v0 `Do` supports an optional declarative
`FollowSpec`. The client installs the ActionID-specific follow before
writing and returns it with `DoResult`; it does not ask callers to
construct an ActionID and race `Subscribe` against `Do`.

- Follow filters and terminal event names are declarative and bounded. The
  client supplies the ActionID internally; follow options cannot override
  it.
- While active, a follow receives matching ActionID events in addition to
  their ordinary event-subscription delivery. List-correlated events
  remain exclusive to their `List` state.
- `EventNames` selects nonterminal follow events. Every declared
  completion event is implicitly eligible even when it is absent from
  `EventNames`: it is charged and enqueued before clean completion is
  committed. If local or aggregate capacity cannot be reserved,
  `ErrLagged` wins and the terminal event is not silently dropped behind a
  clean EOF. With no declared completion event, the caller must close the
  follow explicitly.
- Only a successful `Do` transfers follow ownership to the caller. Every
  non-nil `Do` error closes the provisional follow and leaves all
  retirement/drain ownership inside the client; no partial result contains
  a caller-owned resource.
- A follow closes cleanly on a declared terminal event, or independently
  on caller `Close`, lag, context-driven consumer exit, or client death.
  Its branch may remain active after the immediate response branch
  completes.

### Demultiplexing

The read loop performs only bounded internal routing and never waits for
consumer code:

| Message | Destination |
|---|---|
| Response carrying an ActionID whose response branch is active | That request's single-use response branch |
| Event carrying an active list ActionID | That list's bounded queue/state machine according to its `ListSpec`; not ordinary delivery |
| Event carrying an active non-list follow ActionID | The matching follow and every matching ordinary subscription |
| Message carrying an outcome-unknown or abandoned-drain ActionID | Bounded discard/drain accounting; never ordinary delivery |
| Event carrying an issued but inactive list-kind ActionID | Discard as late list traffic using the opaque kind discriminator |
| Other event | Every matching ordinary subscription's bounded FIFO, sharing the immutable event value |
| Response whose response branch is inactive or unknown, or structurally invalid envelope | Terminal protocol/correlation error, even if another branch for that ActionID remains active |

Each subscription preserves matching unsolicited events in observed wire
order. Each list preserves its own correlated items in observed wire order
even when multiple actions interleave. The library does not promise a
global consumption order across separate list and subscription handles,
nor FIFO admission order between concurrently invoked `Do` calls unless
the implementation explicitly documents and tests it.

The demultiplexer, pending correlation, and retirement/drain records are
implemented as one isolated, table-driven state machine under `internal/`,
owned by the single reader goroutine, with invariant assertions at every
transition. Its conformance target is the model-based property that every
request, list, and subscription commits exactly one terminal result.

### Explicit subscriptions

`Client.Subscribe(opts...)` validates its options, checks client and
aggregate limits, registers eagerly, and returns
`(*Subscription, error)`. Registration has a linearization point before
`Subscribe` returns.

- `Subscription.Next(ctx)` blocks for the next item and is
  single-consumer. Canceling that call's context cancels only the current
  wait and leaves the subscription registered; concurrent consumption is
  rejected.
- While active, `Close` is idempotent, unregisters immediately, discards
  queued events, and commits the subscription's local `ErrClosed`. After a
  terminal result has already won, `Close` preserves that result; on clean
  follow completion it discards any undrained queue and releases its
  aggregate charge without replacing nil `Err`. `Done` closes on the first
  terminal result; `Err` is stable and meaningful after `Done` and returns
  nil while active.
- Client death and lag discard queued events so applications fence stale
  generations promptly. A clean terminal follow preserves its already
  queued events and yields `io.EOF` only after they drain.
- `All(ctx)` creates no second registration. Once iteration begins, every
  exit path closes the underlying subscription exactly once. If iteration
  never begins, the explicit handle remains caller-owned and must still be
  closed. The adapter is single-use; a second or concurrent consumer is
  rejected. The iterator contract is pinned: clean terminal completion
  ends iteration without yielding an error — `io.EOF` is never yielded —
  and a terminal failure yields exactly one final (zero `Event`, error)
  pair before stopping.
- A blocking `Consume(ctx, handler)` adapter is targeted at v0.1: it calls
  a handler serially on the caller's goroutine and must not create a
  hidden goroutine. An asynchronous `OnEvent` registry is not in v0.
- Ordinary subscription filters are declarative event-name data
  (`MatchEvents` and bounded combinations). ActionID-specific follow and
  list routing is established atomically through `FollowSpec` and
  `ListSpec`; arbitrary predicates do not execute on the read loop.
- Every subscription has bounded item and byte capacity, and the client
  has aggregate subscription count and retained-byte limits. Aggregate
  accounting may overcount conservatively but never undercounts retained
  backing memory or queue overhead.
- On overflow, that subscription's queued items are discarded and it
  closes promptly with `ErrLagged`. The read loop, action path, and other
  subscriptions continue. Nothing is silently dropped while the
  subscription remains healthy.
- Fan-out visits matching subscriptions in stable registration order. A
  recipient that cannot reserve both its local and aggregate charge is
  terminated with `ErrLagged` before enqueue, releases its queue, and
  routing continues; map iteration order never selects the victim.
- `ErrLagged` means the consumer has lost synchronization. It must
  establish a replacement subscription before starting a new snapshot and
  retry if the replacement also lags during reconciliation. No lossy
  skip-and-continue delivery mode exists or is planned for v0: a
  state-deriving consumer cannot recover the identity of lost events from
  a loss count, so resubscribe-and-resnapshot is the only honest recovery.

### List actions and `ListSpec`

AMI list actions are not uniform enough for a universal
`EventList: Complete` plus `ListItems` assumption, yet the header
convention is real and self-describing where it exists. `StartList`
therefore accepts a declarative `ListSpec` whose completion detection is
hybrid: a correlated event carrying `EventList: Complete` always commits
clean completion, declared completion event names (required for actions
predating the header convention) commit completion by name, and an empty
`CompletionEvents` selects the pure header convention. `ListSpec` also
carries optional alternative count fields; it never accepts a user
function that would run on the read loop.

- `StartList`'s context governs admission, action write, and the initial
  response only. Before a successful response, all list state remains
  library-owned.
- The list state is registered before writing and tolerates
  item/completion arrival before the initial response. Completion names
  and the `EventList` header are matched case-insensitively; every
  correlated event that is not terminal — by declared name, by
  `EventList: Complete`, or by `EventList: cancelled` — is an item.
- If initial success wins, ownership transfers through the returned
  `List`, which may already be cleanly complete. If rejection or a
  pre-response list failure wins, buffered correlated data is discarded,
  no handle escapes, and the client owns any required retirement/drain.
- If the complete action was written and the request context ends before
  the initial response, `StartList` returns an outcome-unknown
  `RequestError`, no handle escapes, and the healthy client enters bounded
  internal drain/discard.
- If an ambiguous/partial write or another client-terminal cause wins
  before initial success transfers ownership, no `List` escapes.
  `StartList` returns a `RequestError` with the normal dispatch-phase and
  `MayHaveExecuted` classification, wrapping the client root cause. A
  closed transport needs only local buffer cleanup, not a remote drain; a
  zero-byte failure remains definitely-not-sent. Once initial success has
  committed, later client death instead terminates the returned `List`.
- `List.Next(ctx)` streams item events from a bounded item/byte queue.
  Canceling one `Next` wait leaves the list handle active.
- `List.All(ctx)` is a single-use adapter over the same handle and follows
  the same ownership rules as `Subscription.All`.
- `List.Completion()` exposes the terminal completion event after success.
  A configured count is verified against the first declared count field
  present; no count is required when none is declared. The remote count is
  never trusted for preallocation.
- `EventList: cancelled`, a mismatched count, list overflow, or malformed
  correlated input produces a typed `ListError`. Connection death instead
  terminates an active list with an error wrapping the stable client root
  cause.
- If the caller invokes `Close`, or an owning `All` adapter exits before
  completion, the client continues bounded drain/discard for that ActionID
  until completion. Dropping the Go pointer is not observable and has no
  finalizer-based cleanup guarantee. If the remote never completes within
  the configured abandoned-list limits, the connection closes rather than
  retaining an unbounded tombstone.
- Clean completion preserves queued items; after they drain, `Next`
  returns `io.EOF` and `Err` remains nil. List failure, client death, or
  local close discards provisional queued items and commits one stable
  first-winner error.
- While the list is active, local `Close` commits `ErrClosed`. After any
  terminal result, `Close` preserves that result; after clean completion
  it may discard undrained items and release aggregate charge while
  keeping nil `Err`.
- The public `List` may be terminal while its separate internal
  abandoned-drain record remains active; closing the public handle never
  transfers that internal cleanup to the caller.
- Provisional list data must not be published as a complete snapshot until
  clean terminal completion. Semantic reconciliation with live events
  belongs to the application.

### Snapshot and event-flow startup

Eager subscription prevents a library-created delivery gap after the
subscription's registration point. It does not make an AMI list action an
atomic snapshot, guarantee application idempotency, or decide how
overlapping state transitions are reconciled.

There is a separate connection-start boundary: if Login enables events
before `Dial` returns, events can arrive before a caller can subscribe.
The documented gap-minimizing pattern is:

1. Login with events disabled.
2. Register the live subscription.
3. Enable the desired AMI event mask through an `Events` action sent with
   `Do`.
4. Start the action-specific list snapshot.
5. Stage the list until clean completion, then reconcile the buffered live
   events according to application semantics.

`Dial` sends `Events: off` at Login when `Config.EventMask` is empty;
omitting the header would let Asterisk default the session to the user's
full `readperm` mask. An explicitly configured mask is applied at Login,
and its documented consequence is that events may arrive before any
subscription can be registered; the library never claims that `Dial`
followed later by `Subscribe` is gap-free when Login already enabled
events. Server-side `eventfilter` configuration in `manager.conf` is a
complementary, application-owned mitigation.

### Keepalive

Application-level Ping keepalive is part of v0 and is distinct from
operating-system TCP keepalive.

- The keepalive worker starts only after successful login and stops as
  part of client termination.
- The first Ping becomes due one interval after client readiness. A valid
  matching response schedules the next full interval; ticks never
  accumulate and ordinary AMI traffic does not reset the schedule. A
  previous Ping is never overlapped with another.
- The Ping uses normal client-owned ActionID correlation and one reserved
  internal pending/retirement slot. Writer admission is
  cancellation-aware and bounded; once Ping is due, subsequently admitted
  public writes cannot pass it.
- Failure to acquire write ownership and fully emit Ping within the
  configured write-attempt deadline terminates the client with
  `ErrPingWriteTimeout` through a typed `KeepaliveError`.
- A fully written Ping must receive its response within `PingTimeout`.
  Missing that deadline terminates the client with `ErrPingTimeout`,
  closes the connection, and fails still-active pending calls, lists, and
  subscriptions with errors wrapping the same root cause.
- A Ping `Response: Error` or malformed response terminates keepalive with
  a sanitized typed cause distinct from timeout. `ErrPingTimeout`
  specifically means a complete Ping was sent and its valid matching
  response did not win before the response deadline.
- Ping responses and IDs are internal and never enter user subscriptions.
- Ping response, timeout, EOF, and `Close` compete through the normal
  first-winner client transition. A timeout cannot replace a terminal
  cause that already won.
- Defaults: 30s interval, 5s write-attempt deadline, 10s response timeout.
  The zero-value `KeepaliveConfig` selects these defaults; disabling
  requires an explicit `Disabled: true`, consistent with the rule that a
  zero value never silently changes safety behavior.

### Lifecycle

- `Dial(ctx, cfg)` performs TCP connect, optional TLS handshake, banner
  validation, and login, all bounded by `ctx`. It returns only after the
  session and keepalive state are ready. The client retains the raw banner
  line and exposes it through `Banner()` for diagnostics; the library
  derives no behavioral decisions from it.
- Client state is monotonic: running, closing, closed. Once closing
  begins, new actions, lists, and subscriptions fail with `ErrClosed`.
- The client and every handle have one stable first-winner terminal
  transition. A result already committed by success, request context,
  local close, lag, list failure, or another independent cause is never
  replaced by later client death.
- When a client terminal cause wins, every still-active operation
  transitions exactly once to an error wrapping it. `Client.Close` wins
  with `ErrClosed` when no earlier cause exists.
- Before client `Done` closes, the reader, keepalive worker, and active
  writer have stopped; no connection-owned admission, routing,
  correlation, or terminal-result state can change, and every admitted
  waiter has been made runnable with its committed result. An
  already-terminal detached follow or list may still drain or discard its
  own bounded queue after client `Done`. `Done` does not wait for caller
  code to return or those detached queues to drain; client `Err` is stable
  after `Done`.
- `Close()` is immediate, idempotent, and abortive: it stops admission,
  closes the socket, and does not wait for consumer queues to drain. It
  contains no hidden Logoff deadline.
- Graceful `Logoff`/`Shutdown(ctx)` may be considered later if evidence
  justifies the extra state; it is not required for v0 correctness.
- No auto-reconnect exists in the core. `Redialer` is deferred.
  Applications use `Done`/`Err`, create a new client under bounded
  backoff, and start a fresh snapshot/reconciliation generation.

## Error model

Sentinel errors support `errors.Is`:

- `ErrClosed`
- `ErrLagged`
- `ErrLoginFailed`
- `ErrPingTimeout`
- `ErrPingWriteTimeout`
- `ErrOutcomeUnknown`
- `ErrRetirementExpired`

Typed errors support `errors.As`:

- `RequestError`: dispatch phase, wrapped cause, ActionID when assigned,
  and `MayHaveExecuted`.
- `ResponseError`: stable sanitized `Error()` text plus explicit access to
  the untrusted raw `Response`; it never places the remote `Message` field
  in the error string.
- `DialError`: stable connection/TLS/login phase classification whose
  `Error()` omits endpoint and cause text, while `Unwrap` explicitly
  exposes the underlying cause.
- `KeepaliveError`: Ping write, response timeout, rejection, or
  malformed-response phase whose stable `Error()` omits raw response and
  cause text; timeout variants wrap `ErrPingWriteTimeout` or
  `ErrPingTimeout`.
- `ProtocolError`: framing, envelope, correlation, or limit violation
  identified by sanitized category and dimension, without embedding raw
  remote content.
- `ListError`: cancelled, overflowed, count mismatch, or malformed list
  state.
- `RetirementError`: outcome-unknown response retirement or abandoned-list
  drain expiry, identified only by a sanitized kind and limit dimension
  and matching `ErrRetirementExpired`.

Malformed input never panics or grows retained memory without bound.
Library-authored `Error()` text never formats raw AMI fields,
server-controlled messages, endpoints, certificate names, or the wrapped
OS/network/TLS cause. `Unwrap` still exposes the underlying cause for
programmatic inspection; applications must classify and redact that
potentially topology-bearing cause before logging it. Raw responses and
events are explicit data that applications must classify separately.

## Limits and resource accounting

Every limit is explicit and has a documented nonzero safe default; zero
never silently means unbounded. Connection/client limits are validated at
`NewConn` or `Dial`, while subscription, follow, and list options are
validated and copied at their own registration/admission point before any
retained state or wire I/O is committed. Required dimensions include:

- banner bytes;
- inbound line bytes, fields per message, and message bytes;
- partial-frame age after the first byte, without treating an idle healthy
  connection as a slow frame;
- `Command` output lines and bytes;
- outbound field count, line bytes, and action bytes;
- outbound writer admission and write-attempt duration;
- public pending requests plus one reserved keepalive slot;
- active lists, per-list items/queued bytes/total observed bytes, and
  client-wide retained list bytes;
- subscriptions, per-subscription queued items/bytes, matcher names/bytes,
  and client-wide queued subscription bytes;
- reserved outcome-unknown retirement and abandoned-list drain
  count/lifetime;
- keepalive interval, write-attempt deadline, and response timeout.

Initial v0 anchors for the core inbound and queue dimensions: banner 1 KiB
(the pre-authentication read is deliberately the tightest limit), inbound
line 32 KiB, inbound message 128 KiB, per-subscription queue 512 events /
2 MiB, client-wide retained subscription bytes 32 MiB.
Post-authentication byte/count ceilings follow a headroom-over-strictness
policy — they are ceilings, not allocations, so generosity costs memory
only under attack or pathology — while time-based dimensions are exempt
from that policy and stay tight. Every limit, anchored or not, ships with
a documented rationale and an exact-boundary test.

Inbound protocol/connection-level limit breaches terminate the connection.
Outbound validation and retirement-capacity failures reject the operation
before writing. Retirement records are never evicted to admit work; expiry
commits one sanitized `RetirementError` wrapping `ErrRetirementExpired` as
the client root cause before removal. Subscription overflow closes only
that subscription with `ErrLagged`. List overflow terminates that list and
enters bounded drain/discard; expiry of that drain closes the client with
the same retirement cause while the already-terminal public list retains
its first error.

Immutable message storage may be shared across subscription queues, but
accounting must reflect the actual retained backing memory rather than
only pointer counts.

## Authentication, TLS, logging, and security posture

- TLS is supported. The library clones caller-provided `tls.Config`,
  performs `HandshakeContext`, derives the host portion of `Address` as
  `ServerName` when verification is enabled and the clone leaves it empty,
  never silently disables verification, and does not mutate caller-owned
  configuration. Client certificates remain supported through the cloned
  configuration and are covered by integration tests.
- Plain Login over verified TLS is the primary recommendation. v0 legacy
  MD5 challenge/response is an explicitly selected authentication mode
  with no automatic downgrade and no claim of transport security.
- The login secret is used for authentication and is not retained in the
  established `Client`.
- The generic library does not decide whether a particular plain-TCP
  topology is acceptable. Applications own loopback, protected-transport,
  acknowledgment, and authorization policy.
- The v0 core is silent by default and has no `Logger` in the required
  public surface. Its own errors contain no raw AMI field values or
  server-controlled messages.
- Generic `Action`/`Message` values cannot promise to discover every
  secret-bearing custom field. They are not automatically dumped by the
  library, and a future logging adapter must emit only explicitly
  allowlisted metadata off the read loop.
- `ResponseError.Response()` and other raw accessors are deliberately
  explicit and return untrusted, potentially sensitive data.
- The library is not an authorization boundary. A successful AMI response
  does not prove application-level permission, target scope, capability,
  or audit compliance.
- The remote is untrusted. Robustness against malformed, malicious,
  stalled, or internally inconsistent input is a core requirement.
- No telemetry, crash upload, update check, or other non-AMI egress
  exists.

## Modern Go usage

The floor is `go 1.27` at the first public release, under a
README-declared policy that the library tracks the latest stable Go
release. Until Go 1.27 is stable (expected 2026-08), development uses
`toolchain go1.27rc1` and no public tag is published. CI runs the floor
toolchain plus tip, adopts the `go fix` modernizers, and keeps the default
`stdversion` vet check enabled.

| Feature | Use |
|---|---|
| `iter.Seq2[Event, error]` (Go 1.23) | Optional single-use adapter over an explicitly owned `Subscription`/`List`, not the resource primitive |
| `context.WithCancelCause` (Go 1.21) | One stable root cause for client death |
| `context.AfterFunc` (Go 1.21) | Cancellation-aware waiter wakeups in `Next`/admission paths without a goroutine per wait |
| `crypto/rand.Text` (Go 1.24) | Random per-session ActionID prefix in one call; no UUID dependency or format |
| `runtime.AddCleanup` (Go 1.24) | Debug/test-only reporting of handles dropped without `Close`; it reports and never cleans, so the no-finalizer-cleanup contract stands |
| `unique` (Go 1.23) | Optional interning of recurring header keys, adopted only with benchmarks and retained-byte accounting that reflects sharing |
| `sync.WaitGroup.Go` (Go 1.25) | Reader, writer, and keepalive worker lifecycle |
| `testing/synctest` (Go 1.25) | Deterministic keepalive, cancellation, and lifecycle tests without bare sleeps or hidden clocks |
| `testing.T.ArtifactDir` (Go 1.26) | Redacted integration-test transcripts as CI artifacts |
| `GOEXPERIMENT=goroutineleakprofile` (Go 1.26) | CI-only goroutine-leak profiling over the lifecycle test suite |
| Native fuzzing | Wire parser, envelope classification, outbound validation, and demultiplexer state-machine corpus |

Generics — including Go 1.27 generic methods — stay out of the v0 core
API: the raw core is untyped by design. Generic methods cannot be declared
on interfaces or implement interface methods, which confines them to
concrete types; they are reserved as the foundational tool for the future
`amix` typed surface. `runtime/secret` (experimental in Go 1.26) is
tracked for eventual secure erasure of login material; experimental
features are not used in shipped code.

## Deferred typed layer (`amix`)

The raw core remains intentionally untyped because AMI actions/events vary
across Asterisk versions and modules. `amix` is deferred beyond v0. Go
1.27 generic methods are its anticipated foundational tool — typed decode
accessors living on concrete core values instead of a package-level
function sprawl — noting that generic methods cannot participate in
interfaces. Its future design must resolve before implementation:

- the license and attribution obligations of Asterisk XML documentation
  and generated derivatives;
- exact source release/commit/checksum provenance and reproducible offline
  generation;
- how multiple supported Asterisk versions map into one stable Go API;
- preservation of unknown/version-specific fields;
- the initial generated surface and semver policy;
- the fact that a generated type's existence does not prove runtime
  capability or authorization.

No normal build or test may download schema input. Accepted source
material and generator output must be pinned and reviewable in the
repository.

## v0 package and repository layout

```text
/               root package ami: Conn, Client, immutable messages, errors, limits
/amitest/       scriptable public fake AMI server for consumer tests
/internal/wire/ parser, encoder, bounded framing, fuzz corpus
/examples/      runnable generic examples using synthetic identities and data
```

`amix/`, `cmd/amitap/`, `Redialer`, and `OnEvent` are not part of v0.
`amitest` is an adoption feature, but its public surface starts small:
deterministic scenarios composed through a programmatic Go builder API (no
text script format is frozen in v0), strict unexpected-action failure,
fragmentation/coalescing, delayed/interleaved messages, disconnects, TLS,
and bounded malformed-input scenarios. It binds only loopback on an
ephemeral port by default.

The library is hosted under the personal account `github.com/nakisen` with
module path `github.com/nakisen/ami`; the license is Apache-2.0. The
README positions the library on message fidelity, correlation safety,
bounded resource use, honest failure semantics, and first-class testing —
zero runtime dependencies is a property, not the headline, since most
existing Go AMI libraries are already stdlib-only. The repository
establishes its own `AGENTS.md`, security reporting policy,
compatibility/versioning policy (including the tracks-latest-Go support
statement and an Asterisk/AMI banner version table), CI, and release
process before implementation. Normal tests are offline and deterministic.

## Testing strategy

- Table-driven protocol, validation, state-machine, and error tests.
- `testing/synctest` for all keepalive, deadline, cancellation, and close
  timing.
- Native fuzzing with a committed synthetic corpus. No private fixture,
  customer capture, partner capture, credential, or operational identifier
  is committed to the repository.
- Race-enabled CI across the suite and repeated focused lifecycle runs.
- Tagged integration tests against an explicit real-Asterisk matrix — live
  jobs cover Asterisk 18, 20, 22, and 23, while legacy-only behaviors
  (`--END COMMAND--` Command framing, MD5 challenge against older
  releases, chan_sip-era list actions such as `SIPpeers`/`PeerlistComplete`,
  version-pinned since chan_sip's removal in Asterisk 21) run against
  version-tagged synthetic fixtures; normal `go test` requires neither a
  PBX nor network access. Public integration jobs use only dedicated
  synthetic systems, identities, configuration, and call data. Credentials
  are injected through the CI secret channel, never command-line
  arguments, logs, fixtures, or artifacts; raw AMI payloads and endpoint
  identifiers are redacted from public output. Customer and partner
  live-system captures are never test inputs.
- Public conformance scenarios are independently re-derived from protocol
  contracts and synthetic fixtures, not copied from any consumer's
  implementation tests.

Required concurrency and lifecycle cases include:

- fragmented/coalesced framing (including one read returning a full
  message plus the start of the next), repeated fields, bare-`\n` line
  terminators, header names containing digits, unexpected-case envelope
  fields, non-UTF-8 bytes, legacy/modern Command output, and partial-write
  propagation;
- an immediate response arriving before the sending goroutine resumes;
- many interleaved normal and list requests, including list
  items/completion before the initial response;
- cancellation before write admission, during ambiguous write, after full
  write, and simultaneous with a response;
- late response/list messages after timeout, list close, and subscription
  close;
- async completion before or after the immediate response with atomic
  follow registration;
- follow completion at an exact local/aggregate buffer boundary, including
  `ErrLagged` winning when its terminal event cannot be enqueued;
- exact subscription/list/aggregate count and byte boundaries, including
  isolation of one lagging subscriber;
- never-consumed and early-stopped iterator adapters without leaked
  subscriptions;
- `Close` before and after clean/failing terminal transitions, preserving
  the first result while releasing every queue charge;
- caller mutation of `FollowSpec`, `ListSpec`, and subscription option
  slices after admission without changed routing or data races;
- list completion, list failure, partial write, and client death before
  the initial response, with no partial handle or impossible drain;
- concurrent Close, EOF, protocol failure, Ping timeout, write failure,
  and request cancellation;
- one Ping in flight, reserved Ping capacity, successful deadline renewal,
  exact timeout cause, and keepalive shutdown ordering;
- retirement-capacity exhaustion and retirement deadline expiry with the
  exact stable client cause;
- immutable fan-out under concurrent access;
- randomized demultiplexer transitions with exactly one terminal result
  per request/list/subscription.

## Canonical usage patterns

Documentation cornerstones; runnable versions belong in `examples/`,
including a deliberately minimal listen-to-events example a few lines long
as the first-contact snippet:

- **Snapshot plus live:** disable Login events, create an explicit
  subscription, enable the event mask with `Do`, start a `List` with the
  correct declarative `ListSpec`, stage it through clean completion, then
  apply application-owned overlap reconciliation. `ErrLagged` requires a
  replacement subscription and resnapshot on the same healthy connection.
- **Application-owned reconnect:** `Done`/`Err` drive a bounded outer dial
  loop. `ErrPingTimeout`, EOF, or another client-terminal cause creates a
  new client generation and always requires a fresh snapshot.
- **Async completion:** request an ActionID-specific follow subscription
  as part of `Do`; the client installs it before writing. The caller never
  manufactures an ActionID or races a separate subscription against send.
- **Hook-style handling:** iterate `Subscription.Next`/`All` or call a
  blocking consumer-owned handler loop. The handler runs outside the read
  loop and may safely call `Do`; hidden callback goroutines are not part
  of v0.

## API sketch (signatures only)

Names may still be refined; ownership and lifecycle semantics are the
contract.

```go
func NewConn(conn net.Conn, limits WireLimits) (*Conn, error)
func (c *Conn) ReadBanner(ctx context.Context) (string, error)
func (c *Conn) ReadMessage(ctx context.Context) (Message, error)
func (c *Conn) WriteAction(ctx context.Context, action Action, actionID string) error
func (c *Conn) Close() error

func (m Message) Get(key string) string
func (m Message) Lookup(key string) (string, bool)
func (m Message) Values(key string) []string
func (m Message) Fields() iter.Seq2[string, string]

func Dial(ctx context.Context, cfg Config) (*Client, error)

type AuthMethod uint8

const (
    AuthPlain AuthMethod = iota + 1
    AuthMD5
)

type Config struct {
    Address     string
    Username    string
    Secret      string
    Auth        AuthMethod
    TLS         *tls.Config
    DialContext func(ctx context.Context, network, addr string) (net.Conn, error)
    EventMask   string
    Keepalive   KeepaliveConfig
    Limits      Limits
}

type KeepaliveConfig struct {
    Disabled     bool          // zero-value config selects the enabled defaults
    Interval     time.Duration // default 30s
    WriteTimeout time.Duration // default 5s
    Timeout      time.Duration // default 10s
}

func NewAction(name string, fields ...Field) (Action, error)

type FollowSpec struct {
    EventNames       []string
    CompletionEvents []string
    BufferItems      int
}

type DoResult struct {
    Response Response
    ActionID string
    Follow   *Subscription
}

func (c *Client) Do(ctx context.Context, action Action, opts ...DoOption) (DoResult, error)
func WithFollow(spec FollowSpec) DoOption

func (c *Client) Subscribe(opts ...SubOption) (*Subscription, error)
func MatchEvents(names ...string) SubOption
func Buffer(items int) SubOption

func (s *Subscription) Next(ctx context.Context) (Event, error)
func (s *Subscription) All(ctx context.Context) iter.Seq2[Event, error]
func (s *Subscription) Done() <-chan struct{}
func (s *Subscription) Err() error
func (s *Subscription) Close() error

type ListSpec struct {
    CompletionEvents []string // optional; empty selects the generic EventList-header convention
    CountFields      []string // optional alternatives, checked in order
}

func (c *Client) StartList(ctx context.Context, action Action, spec ListSpec) (*List, error)
func (l *List) Response() Response
func (l *List) Next(ctx context.Context) (Event, error)
func (l *List) All(ctx context.Context) iter.Seq2[Event, error]
func (l *List) Completion() (Event, bool)
func (l *List) Done() <-chan struct{}
func (l *List) Err() error
func (l *List) Close() error

func (c *Client) Banner() string

func (c *Client) Done() <-chan struct{}
func (c *Client) Err() error
func (c *Client) Close() error
```

`DoResult` carries the immediate response, assigned ActionID, and optional
atomically registered follow subscription only when `Do` returns nil
error. On every non-nil error, no caller-owned follow escapes; the client
closes the provisional handle and retains any required retirement/drain
state internally. `StartList` likewise never returns a partial `List` with
an error. Keepalive disabling is explicit through
`KeepaliveConfig.Disabled`; the zero-value configuration selects the
enabled defaults.

## Open questions

1. Exact safe defaults for the dimensions not yet anchored: Command output
   lines/bytes, outbound field/line/action bytes, writer admission and
   write-attempt durations, public pending count, per-list and aggregate
   list bytes, matcher names/bytes, partial-frame age, and
   retirement/abandoned-drain counts and lifetimes. (Keepalive timings and
   the core inbound/queue anchors are decided; see the 2026-07-16
   decisions.)
2. `amix` license/attribution and provenance: acceptable Asterisk XML
   source license, exact release/commit/checksum, generated-derivative
   obligations, reproducible offline generation, initial typed surface,
   and semver strategy.
3. Compatibility-label policy for the decided Asterisk matrix (how
   supported versions are labeled and retired in the README).
