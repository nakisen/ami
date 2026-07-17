# Decision Log

Dated, append-only record of the design and implementation decisions of
`github.com/nakisen/ami`. Each section records one decision batch —
usually one implementation slice — in chronological order; new slices
append a new section rather than editing old ones.

This log is history plus rationale as it stood at decision time. The
current, authoritative state of the design is [design.md](design.md):
when the two disagree, design.md wins and the divergence is a bug to fix
in the same change that finds it. Like every document in this
repository, this file must stay publishable: no private fixtures, live
captures, customer or partner identifiers, credentials, or operational
data.

## 2026-07-14 — Repository split and v0 session surface

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

## 2026-07-16 — Survey-driven direction and protocol floor

Adopted at repository bootstrap after an ecosystem survey (eight Go AMI
libraries; the Asterisk-Java, AsterNET, Panoramisk, PAMI, and NAMI
lineages; current Rust crates) and source-level verification of protocol
behavior against Asterisk. The survey's key evidence is kept under
"Prior art" in [design.md](design.md).

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
- Supported protocol versions: AMI 2.0.0 and newer (Asterisk 12+), i.e.
  every protocol generation from the 2.x series through 13.0.0 on
  today's master. Only AMI 1.x sessions (Asterisk 11 and older) are out
  of scope: no 1.x-specific quirks, fixtures, or conformance targets. The banner stays diagnostic-only —
  the client neither gates nor branches on it, so older servers are
  merely unsupported, not actively refused. The legacy `--END COMMAND--`
  Command framing remains in scope because it ships within the supported
  range (Asterisk 12–14.1).

## 2026-07-16 — internal/wire (commit 444c1a8)

- The parser returns an ordered field list with keys and values
  verbatim, consuming exactly one optional space after the key's colon;
  it never imports the root package, which converts wire fields into an
  immutable `Message` with a single copy. In a legacy `Response: Follows` command frame, only
  `Privilege` and `ActionID` are accepted as trailer headers before raw
  payload begins; payload lines are normalized into synthesized `Output`
  fields so both Command framings present one message shape; and a
  payload line that merely ends with `--END COMMAND--` also terminates
  output, its prefix preserved as the final line, because CLI output
  lacking a trailing newline glues the terminator and treating it as
  payload would stall the frame until a limit failed it.
- Inbound budgets are split by field kind: fields whose key is `Output`
  — synthesized legacy payload or modern repeated headers — charge the
  command-output line/byte limits, and every other line charges the
  per-message field/byte limits, so command output has its own budget
  and cannot consume the bounds meant for ordinary messages. All nine
  wire limit dimensions are explicit; `Limits.Validate` rejects
  non-positive values and an unvalidated zero limit fails closed rather
  than meaning unbounded.
- The outbound encoder validates the injection surface (empty key;
  colon, CR, or LF in a key; CR or LF in a value) and the outbound
  limits before emitting any byte. A message whose first field encodes
  `Response: Follows` re-parses under legacy command framing — the one
  documented round-trip exception — so synthesizing a legacy frame
  requires raw writes by design; `amitest` will compose such frames as
  raw bytes.

## 2026-07-16 — Conn (commit 1629042)

- Context cancellation is implemented by poking a past deadline into the
  owned `net.Conn` through `context.AfterFunc`; the watcher's release
  clears the poked deadline before the outcome is classified, so an
  operation that completed despite a racing cancellation leaves the
  connection usable. The error
  contract is mechanical: a returned context error means the operation
  was abandoned cleanly (no byte of the pending inbound frame consumed —
  `wire.Reader` gained `Dirty()` to attest this — or zero action bytes
  written) and the connection remains usable; every other error means
  the connection closed, with transport errors passed through verbatim
  (never re-wrapped, honoring the no-cause-text rule), wire violations
  mapped to `ProtocolError{Category, Dimension}`, and a cancellation
  that interrupted a partially transferred frame surfacing as the
  transport's deadline error, deliberately not as a context error. This
  refines the "after a write has begun" clause: a cancellation that
  provably wrote zero bytes is classified as clean because no frame
  byte was emitted. Outbound validation `ProtocolError`s are returned
  before any byte is written and leave the connection usable.
  Operations on a closed connection return `ErrClosed`.
- `WriteAction` composes the frame as the `Action` field first, the
  `ActionID` field second when the caller-owned ActionID is non-empty —
  an empty ActionID omits the field entirely — then the action's extra
  fields in wire order. `NewAction` validates shape and injection
  (empty name, CR/LF anywhere, colon in keys, reserved `Action`/
  `ActionID` keys) and defensively copies its fields; size limits remain
  `WriteAction`'s job against the connection's `WireLimits`.
- Newly anchored limit defaults (verified 2026-07-16 against the
  Asterisk master and 18 branches — `manager.h` `AST_MAX_MANHEADERS`,
  `manager.c` `inbuf`/`get_input`/`do_message`): outbound action fields
  128, matching `AST_MAX_MANHEADERS` — the server rejects an action
  with more lines ("Too many lines in message"); outbound line 1022
  content bytes, because the server scans for the terminator within a
  1024-byte window and discards longer lines, so 1022 plus CRLF fills
  that window exactly; outbound action bytes 128 KiB, the server's
  aggregate ceiling of 128 maximal lines and symmetric with the inbound
  message anchor. Headroom-policy inbound anchors: fields per message
  1024 (an order of magnitude above the largest real events), Command
  output 65536 lines / 8 MiB (large `dialplan show`-class dumps,
  proportionate to the 32 MiB client-wide cap). Partial-frame age
  remains open (design.md question 1) and is not enforced by `Conn`; its
  ctx-bounded operations give a synchronous owner full deadline control
  in the meantime.

## 2026-07-16 — Demultiplexer state-machine design ([demux.md](demux.md))

- The demultiplexer is specified before implementation as a passive,
  synchronous, generic machine (`internal/demux`) owning
  classification, correlation, follow/list/subscription registries,
  retirement/drain records, bounded queues, and all accounting — no
  I/O, no locks of its own, no user code, no clock; callers supply a
  monotonic timestamp and the session arms real timers from
  `NextDeadline`. The session serializes access under one lock; the
  reader goroutine is the only message router, and control-plane
  operations enter at their linearization points. design.md's summary
  wording ("owned by the single reader goroutine") was refined in the
  same change to state routing ownership precisely.
- Correlation is response-strict and event-lenient: a response matching
  no active branch and no live retirement record — unknown, duplicate,
  foreign, or missing an ActionID — is a terminal correlation error,
  while events never are. Foreign or absent ActionIDs fan out
  ordinarily, a stale own request-kind ActionID merely loses its follow
  branch, and stale list-kind ActionIDs are discarded silently forever
  through the kind discriminator. Rationale: responses are exactly-once
  request accounting; events are broadcast that other sessions' actions
  also tag.
- Retirement evidence is per kind: a request-kind record releases when
  its late response is absorbed, a list-kind record when its terminal
  mark is absorbed; expiry without evidence closes the client with the
  retirement cause, and records are created only from slots reserved at
  admission, never evicted (per design.md). After release, request-kind
  events degrade to ordinary fan-out — identical to an acknowledged
  action's events.
- The reader-liveness invariant is pinned as a machine invariant with
  reserve-or-terminate as the only delivery primitive. Assertion
  policy: invariant violations panic inside `internal/demux`; the
  session's read loop converts any read-loop panic into client death
  with the cause preserved, keeping the host process alive while
  refusing to run on corrupted correlation state.
- Envelope classification is pinned: `Event:` presence wins over an
  event-specific `Response:` field (`OriginateResponse`); conflicting
  duplicate envelope fields are fatal; `EventList: start` is
  confirmatory only; event and completion names are matched
  ASCII-case-folded; the accounting size of a message is its wire size,
  Σ(len(key)+len(value)+4)+2. Every queue charges the full size and
  aggregates sum queue charges, deliberately overcounting shared
  storage and never undercounting retained memory.
- Conformance scenarios were extended from the 2026-07-16 kmo/volume
  discussion: the QueueStatus wallboard interleave and a 500-call
  unfiltered flood (roughly 300–1500 events/s sustained with
  multi-thousand-event teardown bursts) asserting parse-and-drop of
  unmatched traffic at zero queue cost, exact-boundary laggard
  isolation, and a routing benchmark as headroom evidence; a fuzz
  target over envelope streams joins the corpus. Numeric defaults newly
  visible here (pending count, matcher dimensions, list dimensions,
  retirement count/lifetime) stay in open question 1 for the `Client`
  slice, which also delivers the busy-system queue-sizing guide.

## 2026-07-16 — v0 ergonomics: `Consume` and `Event.Name`

- The blocking `Consume(ctx, handler)` adapter moves from the v0.1
  target into v0, ratified after a UX review of hook-style consumption.
  Its contract is pinned All-consistent: the handler runs serially on
  the caller's goroutine (off the read loop, so it may call `Do`), a
  non-nil handler error stops consumption and is returned, the adapter
  is single-use with concurrent consumers rejected, and once
  consumption begins every exit path closes the underlying subscription
  exactly once. Rationale: it is the first surface newcomers look for,
  it is a few lines over `Next`, and packaging it removes the
  temptation to build an unsafe callback registry above the library.
- `Event.Name()` is added: the classifying event name (the `Event`
  field's value), always non-empty on a classified event — the single
  most-typed accessor in any consumer, made typo-proof.
- The remaining proposals from the same review (`MustAction`,
  `MatchAll`, a `slog` diagnostics hook) stay undecided.

## 2026-07-16 — v0 diagnostics: optional `slog` hook

- The v0 "silent core" stance is amended rather than replaced:
  `Config.Logger` is an optional stdlib `*slog.Logger`, and nil — the
  zero value — keeps the library fully silent, so default behavior is
  unchanged. When set, the library emits internal diagnostics
  (connection lifecycle, keepalive, subscription/list lifecycle
  including lag victims and drop counts, retirement records, terminal
  causes) as explicitly allowlisted metadata only — names, counts,
  durations, reason codes — never message contents, field values,
  credentials, or endpoints, consistent with the sanitized-errors rule.
- The caller's `slog.Handler` is user code, so it never runs on the
  read loop: emission passes through a small bounded internal
  diagnostics queue whose overflow drops diagnostics and counts the
  drops. Dropping diagnostics is acceptable, unlike events. Motivated
  by field operability: the terminal cause alone answers what killed
  the client, but not the timeline that preceded it.
- From the same review, `MustAction` and `MatchAll` are declined for
  v0.

## 2026-07-16 — `internal/demux` implementation

The demultiplexer state machine landed as `internal/demux`: generic
over the routed payload (`Machine[T]`), fully synchronous, driven by
table-driven transition tests covering demux.md's conformance targets
3 and 5–9. The randomized-oracle, fuzz, and flood targets land with
the conformance suite. Decisions made while implementing:

- **List retirement evidence is two facts, not one.**
  [demux.md](demux.md) originally released a list record on its
  terminal mark alone. That rule strands the late response on every
  path where the mark resolves first — an abandonment after a buffered
  completion mark, or a count-mismatch failure before the response —
  and a stranded response is fatal by the response-strict rule. A list
  slot now releases only when both the initial response and the
  terminal mark are resolved; an `Error` response resolves both,
  because a rejected list never streams; and a record is created
  holding whatever evidence its action already observed. Request-kind
  evidence is unchanged (its response). demux.md was amended in this
  change.
- **The ticket resolution protocol is pinned in the package
  documentation.** Every admission is resolved by exactly one write
  resolution (`CommitWrite` or `AbortNotSent`) and one outcome
  (completion by response, completion by death, or `Abandon`); a
  follow or list branch carries one further obligation decided from
  knowledge the session already holds — `AdoptFollow`/`CloseFollow`,
  plus the `AdoptList`/`CloseList` pair the design-note sketch lacked.
  Ticket entries persist until every obligation resolves, so trailing
  resolutions racing client death are tolerated no-ops rather than
  use-after-free hazards.
- **An `Error` list response releases the branch machine-side**, even
  when an earlier overflow already committed the branch terminal: no
  list state survives a rejection (design.md's "no handle escapes").
  After a `Success` response an already-failed branch remains
  adoptable, terminal, so the session decides the public
  `Do`/`StartList` error mapping (still deferred to the session
  slice).
- **A fan-out victim's charges release before routing continues**, so
  a later recipient in registration order reserves against the freed
  aggregate capacity within the same message — the reading of "its
  charges released, and routing continues" that keeps one laggard from
  cascading into its neighbors.
- **A declared count is verified where the completion mark is
  processed**, including a mark buffered before the response, so a
  count failure commits at exactly one place; the note's table
  specified only the streaming case. Correlated traffic arriving after
  a buffered terminal mark but before the response is absorbed and
  counted like record quarantine, and still charges the observed-bytes
  budget.
- **The keepalive slot is one internal pending plus one internal
  retirement slot outside the public pools**; an overlapping internal
  admission is rejected with a distinct sentinel, and an internal
  record occupies the internal slot, never the public pool.
- The accounting invariants run in production: after every mutating
  call the machine re-verifies aggregate-equals-sum-of-charges,
  per-branch caps, the retirement pool, and the pending count at a
  cost linear in registered branches, not queued items; violations
  panic with stable `ami/demux:` messages for the session's read loop
  to convert into client death.
- The conformance suite (targets 1, 2, and 4) landed immediately after:
  a fan-out/correlation oracle plus a full-surface random driver shared
  with the fuzz target, and the 500-call unfiltered flood. The oracle's
  quiescence property caught a real bug on its first seeds — the death
  cascade never released fully-resolved tickets, a leak invisible to
  every deterministic test. Routing benchmarks on the development
  machine (Ryzen 7 9700X) document the headroom the flood discussion
  asked for: an unmatched event — the busy unfiltered connection's
  common case — routes in ~54 ns with zero allocations,
  delivered-and-consumed in ~119 ns, an 8-way fan-out in ~735 ns, so
  the scenario's sustained ~1,500 events/s costs well under 0.1% of
  one core with the production invariant checks included.

## 2026-07-16 — Session-layer default limits (design open question 1 resolved)

The last unanchored dimensions were ratified with the user, closing the
first design.md open question. The standing policy applied throughout:
byte/count ceilings get headroom over strictness (a ceiling is not an
allocation; generosity costs memory only under pathology), time
dimensions stay tight. Ratified values, with the load-bearing rationale:

- **Writer admission 5 s, write attempt 5 s** (a shorter caller context
  wins). A healthy writer admits in milliseconds and completes the
  128 KiB ceiling action in well under a second; waiting longer queues
  work behind a wedged socket. Admission failure is a clean
  definitely-not-sent, so failing early is cheap and safely retryable.
  Both align with the keepalive write-attempt deadline ratified
  earlier.
- **Partial-frame age 30 s, enforced by `Conn`, not the session read
  loop.** The deadline arms at the frame's first byte and clears at
  frame completion, so an idle connection never trips it. 30 s honors
  the 8 MiB `Command` output ceiling on a ~270 KB/s link; ordinary
  128 KiB frames need milliseconds. The design note had expected the
  session read loop to enforce this, but only the framing layer sees
  the first byte: a loop polling `ReadMessage` with a rolling window
  would either kill an innocent frame straddling a window edge or
  double the effective bound, so the knob moved into `Conn` (a small
  additive API on the existing deadline-poke machinery). With
  keepalive enabled a wedged stream already dies in ≤40 s; this bound
  also covers keepalive-disabled sessions.
- **Retirement and abandoned-drain lifetime 60 s.** Deliberately the
  loose exception to time-tightness: expiry is client death, so a
  false positive is expensive, and a slow `Command` or a huge list
  drain can legitimately run tens of seconds — expiring at 30 s only
  to receive the response at 40 s reaches the same death through the
  foreign-response path. True server wedges are keepalive's job
  (≤40 s); the lifetime's abuse surface is bounded by the slot pool,
  not by time.
- **Public pending 256, retirement pool 384, concurrent lists 16.**
  The coupling that sizes the pool: retirement slots are reserved at
  admission, so the pool bounds in-flight work too —
  `MaxRetirement < MaxPending + MaxLists` would make the pending
  ceiling unreachable. 384 = 256 pending + 16 lists + ~112 outstanding
  records. 256 pending is 5×+ headroom over aggressive
  click-to-dial bursts; real deployments run 2–3 concurrent lists.
- **Per-list queue 4096 items / 8 MiB, total observed 32 MiB,
  client-wide queued list bytes 64 MiB.** The largest real lists
  (`CoreShowChannels`, `PJSIPShowEndpoints` at ~10k endpoints) run a
  few thousand entries / a few MiB; 8 MiB matches the `Command` output
  anchor. The observed-bytes ceiling ends a "list that never
  completes" (a completion-matching bug) even when the consumer keeps
  up. 64 MiB aggregate admits 8 worst-case lists queued at once — the
  value the flood conformance suite ran with.
- **Matchers 64 names / 4 KiB, subscriptions 128.** Real matchers
  carry 1–5 names and the longest event names are ~30 bytes; real
  clients open fewer than ten subscriptions. Both are free ceilings —
  the operative brake is the 32 MiB client-wide subscription
  aggregate anchored earlier.
- **Per-subscription 512 events / 2 MiB revisited and confirmed
  final.** The owed revisit closed with flood evidence: an unconsumed
  full-firehose subscription lags at exactly event 513 (the design
  intent — lag fast rather than buffer a firehose) while a consuming
  subscriber never approaches the cap. The busy-system sizing guide,
  owed with the session `Config` documentation, will tell
  intermittent-pull consumers to raise `Items` to poll interval ×
  event rate.

## 2026-07-17 — `Conn` partial-frame age enforcement

The ratified 30 s partial-frame age landed as `WireLimits.MaxPartialFrameAge`
plus a `wire.Reader` frame-start hook. Implementation decisions:

- **The wire package stays clock-free.** The reader only reports the
  moment `Dirty` transitions to true — the first byte of a new frame,
  whether it arrived from the stream or was already buffered — through
  a hook; the deadline arithmetic lives in `Conn`.
- **The read deadline gained a second writer, so pokes take
  ownership.** Cancellation pokes and frame-start armings are
  serialized under the connection lock, and a poke sets a flag that
  frame-start honors: a frame byte racing a cancellation can no longer
  overwrite the poked deadline and stall the cancellation by up to a
  full age.
- **Expiry classification needs no extra state:** an uninterrupted
  `os.ErrDeadlineExceeded` with a dirty frame can only be the armed
  age (a poke implies an interrupted operation), and surfaces as
  `*ProtocolError{limit, MaxPartialFrameAge}` on a closed connection.
  Successful reads disarm the deadline before the next idle wait.
- A frame delivered in one chunk parses out of the buffer without
  another stream read and never observes the deadline — which is also
  what makes the exact-boundary tests deterministic with a 1 ns age.

## 2026-07-17 — `Client` session layer

The session layer landed on top of `internal/demux`: `Dial` (TCP,
optional TLS, banner, plain and explicit-MD5 login), the read loop as
the machine's only router, the writer path, keepalive, the expiry
worker, the public handles (`Do`/`DoResult`/`WithFollow`, `StartList`/
`List`, `Subscribe`/`Subscription` with `Next`/`All`/`Consume`,
`Event.Name`, `Config.Logger`), and the client lifecycle. Decisions:

- **The `Do`/`StartList` error contract over the machine's
  adopt/close primitives** (owed since the demux slice): a success
  response adopts — `DoResult.Follow` or the returned `List`, either
  possibly already terminal, which is honest ownership rather than an
  error. An AMI error response returns `*ResponseError` (follow closed
  by the session; a rejected list was already released machine-side).
  A death completion returns an outcome-unknown `*RequestError`
  wrapping the client root cause, with `CloseFollow`/`CloseList` as
  tolerated post-death bookkeeping. A context end while awaiting the
  response abandons the ticket into a retirement record. Write
  failures split three ways: a clean context abandonment is
  definitely-not-sent on a live connection; a zero-byte transport
  failure is definitely-not-sent on a dead one; any written byte is
  outcome-unknown. The written-byte distinction comes from a
  package-internal `writeAction` returning the byte count — the public
  `WriteAction` signature is unchanged.
- **The reader defers to a poisoning writer for the root cause.** A
  failed action write poisons the connection, which instantly unblocks
  the read loop with `ErrClosed`; dying there would race the writer
  and could commit a generic cause over the real transport error
  (caught as a test flake). On `ErrClosed` with a live session context
  the read loop now waits for the writer's `die` to commit the real
  cause. Every poison path in the session ends in `die`, so the wait
  always resolves.
- **Machine effects are applied while holding the session lock.**
  Completion sends go to single-use buffered channels and branch wakes
  coalesce into capacity-one channels, so nothing under the lock can
  block on a consumer — and lock-scope application is what linearizes
  the response-versus-cancellation race into one winner.
- **`demux` gained a read-only `Terminal` probe** so a wake can close
  a handle's `Done` at its first terminal result without consuming
  queued items — `Done` must fire without a consumer parked in `Next`.
- **Teardown is machine-independent.** `die` runs the death cascade
  with the kill guarded by recover, then sweeps the session-side
  waiter and branch registries directly: even a machine wrecked by the
  panic that killed the client cannot leave a waiter parked or a
  `Done` unclosed.
- **Writer admission is a capacity-one semaphore channel**, whose
  FIFO wait queue is what "a due Ping cannot be passed by later public
  writes" rests on; admission is additionally bounded by the caller's
  context, the ratified `WriteAdmission` default, and client death.
- **ActionIDs** are `crypto/rand.Text()` + `-` + a kind byte
  (`r`/`l`) + a monotonic decimal suffix. An own-prefixed ID with a
  mangled discriminator classifies as foreign: fatal as a response,
  ordinary fan-out as an event.
- **Envelope extraction pins the fail-closed reading**: a message with
  neither `Event` nor `Response`, conflicting duplicate envelope
  fields, or an empty event name classifies invalid and terminates the
  client; response success is `Success` or `Follows` (the legacy
  command frame), everything else — `Error`, `Goodbye`, arbitrary
  text — is a rejection with the raw response available on
  `ResponseError`.
- **Login is a synchronous pre-loop exchange** on the fresh
  connection: an unauthenticated manager session receives nothing
  unsolicited, so each login action's response is the next message,
  strictly matched by ActionID. The secret is used there and never
  retained.
- **The expiry worker** sleeps on the machine's earliest retirement
  deadline, poked through a capacity-one channel by every call that
  can create a record; with keepalive disabled it is the only thing
  standing between a silent stream and an expired record, which is why
  it is a dedicated worker rather than a read-loop side effect.
- **Diagnostics run outside the lifecycle waitgroup**: `Done` promises
  not to wait on caller code, and the `slog` handler is caller code.
  Terminal causes are logged by sanitized class only — this package's
  own error texts, or `"transport error"` for anything that could
  carry endpoints.

## 2026-07-17 — `amitest` public fake server

`amitest` landed as the adoption feature design.md scoped: a
programmatic fake AMI server on a real loopback socket, so consumer
tests run the real `Dial` path without a PBX. Decisions:

- **Scenarios are a handler registry plus reply primitives, not a
  frame script.** `NewServer(Config)` returns a listening server
  (panicking when loopback cannot bind, the httptest convention);
  `HandleAction` registers per-action handlers under case-insensitive
  names, and a handler's `Call` carries the received action —
  ordered-field accessors with the envelope split off — plus
  `Respond`, correlated `Event` (both echo the ActionID
  automatically), `RespondLegacyCommand`, `Raw`, and `Hangup`.
  Uncorrelated traffic is injected through `Server.Event`/`Raw`
  broadcasts at any time, and a `Call` stays valid after its handler
  returns, so delayed and out-of-order replies are scripted with plain
  goroutines and channels. No text script format exists, reaffirming
  the earlier v0 decision.
- **Built-in behavior is exactly what every session needs, and
  deterministic.** The banner, both login schemes (plain secret and
  MD5 challenge with a fixed challenge string — the fake is not a
  security boundary, and a fixed nonce keeps scenarios reproducible),
  and default Ping/Logoff handlers that answer like Asterisk so
  keepalives and shutdown flows work unscripted; each default is
  removable through `HandleAction(name, nil)`. Nothing else is ever
  sent spontaneously — no `FullyBooted`, no periodic events —
  because a scenario must observe exactly the traffic it scripted.
  Broadcasts skip unauthenticated sessions, preserving the login
  invariant the client's synchronous pre-loop exchange relies on.
- **Strictness records and responds.** An action with no handler gets
  an `Error` response — the consumer's pending `Do` resolves loudly
  instead of hanging — and the violation is recorded; `Err()` returns
  the joined violations and `Close()` returns the same value, so one
  `Close` check ends a strict test. Violations are unhandled actions,
  actions before authentication, frames without an `Action` field,
  and wire-protocol violations; a rejected login and an abruptly
  discarded connection are legitimate scenario traffic, not
  violations.
- **The import boundary keeps the dogfooding path open.** `amitest`
  imports `internal/wire` — the same bounded parser and encoder the
  client uses — and never the root package, so the root package's
  in-package tests can later migrate onto `amitest` without an import
  cycle. Its own test suite already dogfoods in the other direction:
  the real client over real TCP covers both login schemes, list
  flows, the wallboard snapshot-plus-live interleave, legacy and
  modern command output, malformed frames terminating the client,
  hangup/reconnect, TLS, fragmented writes, and keepalive pings
  served by the built-in handler.
- **Outbound frames are validated against client-shaped ceilings.**
  The encoder's action dimensions are repurposed with the client's
  inbound defaults (1024 fields, 32 KiB lines, 128 KiB messages),
  because the fake's outbound frames are server messages, bounded by
  what a client accepts rather than what the Asterisk action parser
  does. Builder misuse — odd key/value counts, empty names, header
  injection, oversized frames — panics: scenario scripts are test
  code, and a malformed script is a bug to surface at its call site.
- **Legacy command frames are composed as raw bytes**, per the wire
  slice's round-trip exception. The payload is written verbatim, so a
  trailing newline yields the terminator on its own line and its
  absence yields the glued terminator a real CLI command without a
  final newline produces — both scriptable through one primitive.
- **Real sockets, no `synctest`.** A TCP write completing does not
  mean the client consumed it, so scenarios synchronize through
  protocol barriers — a session's writes and the client's routing are
  both ordered, so a blocking receive or a Ping round-trip proves
  everything earlier was routed — and `testing/synctest` bubbles
  cannot host real-socket tests by design. Write errors to sessions
  that died mid-scenario are discarded; the client under test observes
  the death on its own side.
- **`LocalhostTLS` generates a throwaway self-signed pair at
  runtime** — server and client `tls.Config` covering localhost and
  the loopback addresses for 24 hours — so TLS scenarios are one line
  and no certificate fixture with an embedded expiry can rot in the
  repository.

## 2026-07-17 — External-review hardening: role-sensitive envelope validation

An external review of `c949bc3` found that repeated `EventList:` values
were never compared: classification took the first, so a message
carrying both `Complete` and `cancelled` committed whichever mark came
first — field order deciding between a clean snapshot and a
cancellation. Closing that gap forced the conflicting-duplicate rule
itself to be ratified precisely, and the blanket rule the documents
promised turned out to overpromise:

- **The envelope is role-sensitive.** On an event-class message the
  classifying fields are `Event`, `ActionID`, and `EventList`;
  conflicting duplicates of any of them are fatal. On a response-class
  message they are `Response` and `ActionID`. Identical duplicates stay
  tolerated everywhere.
- **Repeated `Response:` values inside an event-class message are
  ordered payload, not envelope.** The `Event` field alone decides
  classification — `OriginateResponse` legitimately carries both — so a
  `Response` repeat inside an event correlates nothing, and failing the
  session over it would reject plausible real-world traffic for no
  correlation gain. docs/design.md and docs/demux.md previously
  promised the stricter blanket rule and were narrowed to match.
