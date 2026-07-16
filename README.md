# ami

[![CI](https://github.com/nakisen/ami/actions/workflows/ci.yml/badge.svg)](https://github.com/nakisen/ami/actions/workflows/ci.yml)

A Go client library for the Asterisk Manager Interface (AMI), built around
message fidelity, correlation safety, bounded resource use, honest failure
semantics, and first-class testing — with zero runtime dependencies.

> [!IMPORTANT]
> **Pre-release.** This repository is under active development and has no
> tagged release. The first tag follows the stable release of Go 1.27
> (expected August 2026). Until then every part of the API may change
> without notice.

## Why another AMI client

The design was checked against the existing ecosystem before
implementation; [docs/design.md](docs/design.md) records the survey and the
evidence. The short version of what this library does differently:

- **Messages are ordered field lists, not maps.** Duplicate keys are legal
  AMI (`Variable:`, `ChanVariable:`, `Output:`) and their order can be
  meaningful; map-based clients silently lose data.
- **User code never runs on the read loop.** Asterisk disconnects manager
  sessions whose socket writes block past `writetimeout` (100 ms by
  default), so a reader stalled on consumer code kills the connection
  server-side. Consumption is pull-based: explicit `Subscription` and
  `List` handles with `Next(ctx)` and `All(ctx) iter.Seq2[Event, error]`.
- **Nothing is silently dropped.** Every queue is bounded by count and
  bytes; overflow closes that subscription with `ErrLagged` instead of
  discarding events behind your back.
- **Honest action outcomes.** Errors distinguish definitely-not-sent from
  may-have-executed (`MayHaveExecuted()`, `ErrOutcomeUnknown`); the
  library never retries an action.
- **List completion that doesn't hang.** `ListSpec` combines declared
  completion event names with the generic `EventList: Complete` header
  convention instead of guessing by substring.
- **Testing as a feature.** `amitest` ships a public, programmatic fake
  AMI server so consumers can test without a PBX.

## Status

| Component | State |
|---|---|
| Design ([docs/design.md](docs/design.md), [decision log](docs/decisions.md)) | decided direction recorded |
| `internal/wire` — parser, encoder, fuzz corpus | landed |
| `Conn` — low-level framing | landed |
| `internal/demux` — correlation state machine ([design note](docs/demux.md)) | landed |
| `Client` — session, correlation, keepalive | landed |
| `amitest` — public fake AMI server | next up |

## Requirements

- **Go:** this library tracks the latest stable Go release. It is
  currently developed against `go1.27rc1`; see
  [docs/compatibility.md](docs/compatibility.md) for the toolchain policy.
- **Asterisk:** supported protocol versions are AMI 2.0 and newer
  (Asterisk 12+); the live-tested matrix is Asterisk 18, 20, 22, and 23.
  The [compatibility table](docs/compatibility.md) interprets older
  banners for diagnostics only.

## License

Apache-2.0 — see [LICENSE](LICENSE).
