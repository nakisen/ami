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
