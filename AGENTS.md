# AGENTS.md

Instructions for AI coding agents (and a checklist for humans) working in
this repository.

## What this is

`github.com/nakisen/ami` — a zero-dependency Go client library for the
Asterisk Manager Interface (AMI). License: Apache-2.0. Root package `ami`.

## Authoritative documents

- [docs/design.md](docs/design.md) — architecture and the current decided
  state; authoritative. Do not silently contradict it; if a change
  requires deviating, update the document in the same change and call it
  out.
- [docs/decisions.md](docs/decisions.md) — append-only dated decision log,
  one section per decision batch (usually one implementation slice).
  Append each landed slice's decisions there; never rewrite old sections.
  When it disagrees with design.md, design.md wins and the divergence is
  fixed in the same change that finds it.
- [docs/compatibility.md](docs/compatibility.md) — Go toolchain policy and
  the Asterisk/AMI version table.

## Hard rules

- **Zero runtime dependencies.** Only the Go standard library. Never add a
  `require` directive to `go.mod`.
- **Track latest Go.** The floor is the newest stable Go (currently
  `go 1.27`, `toolchain go1.27rc1`). Prefer modern standard-library APIs;
  never lower the floor for compatibility. **No public tag before Go 1.27
  is stable.**
- **Messages are ordered field lists, never maps.** Duplicate keys are
  legal and order-significant; preserve both end to end.
- **No user code on the read loop.** Delivery is pull-based through
  bounded queues; arbitrary predicates or callbacks must not execute on
  the reader goroutine.
- **Bounded everything.** Every queue and buffer has count and byte limits
  and a defined overflow behavior (`ErrLagged` closes that subscription;
  no silent drops, no lossy mode).
- **Honest failures.** Never retry actions, never guess correlation, never
  downgrade authentication or TLS. Errors distinguish definitely-not-sent
  from may-have-executed.
- **Sanitized errors.** Library `Error()` strings never embed raw AMI
  fields, server-controlled text, endpoints, or wrapped-cause text;
  `Unwrap` and explicit accessors expose those for programmatic use.
- **No private data.** No customer or partner fixtures, live captures,
  credentials, or operational identifiers anywhere in the repository.
  Test fixtures are synthetic.

## Workflow

- Build and test: `go build ./...`, `go test -race ./...`, `go vet ./...`.
- Formatting: `gofmt` (CI fails on drift). Modernizers: `go fix ./...`
  must produce no diff.
- Tests are table-driven; timing-sensitive tests use `testing/synctest`
  (no bare sleeps). The wire parser carries a committed fuzz corpus.
- Implementation order for v0: `internal/wire` → `Conn` → demultiplexer
  state-machine design note → `Client` → `amitest`.
