# Compatibility and versioning policy

## Go toolchain

This library tracks the latest stable Go release: the `go` directive in
`go.mod` is raised to each new stable Go shortly after it ships, and new
language and standard-library features may be adopted immediately.
Consumers are expected to build with a current toolchain (the Go project
itself supports only the two most recent releases).

Current state: `go 1.27` with `toolchain go1.27rc1`. **No release is
tagged before Go 1.27 is stable** (expected August 2026); until then the
module is consumable only by commit.

## Asterisk / AMI versions

Asterisk announces its AMI protocol version in the banner sent immediately
after TCP accept, before authentication:
`Asterisk Call Manager/<version>`. The mapping below was verified against
`include/asterisk/manager.h` (`AMI_VERSION`) across release tags of
[asterisk/asterisk](https://github.com/asterisk/asterisk) (2026-07).

**Supported protocol versions are AMI 2.0.0 and newer (Asterisk 12+)** —
everything from the 2.x series through 13.0.0 on today's master. The AMI
1.x rows (Asterisk 1.4–11) are retained for banner diagnostics only:
those sessions are out of scope, and the library neither promises correct
operation there nor actively refuses them.

| Asterisk | AMI version | Notes |
|---|---|---|
| 1.4 | 1.0 | hardcoded literal |
| 1.6 / 1.8 | 1.1 | |
| 10 | 1.2 | |
| 11 | 1.3 | |
| 12 | 2.0.0 | semantic versioning begins |
| 13 | 2.5.0 | grew over the 13.x cycle (e.g. 2.10.x) |
| 14.0 | 2.8.0 | |
| 14.2 | 3.1.0 | `Command` responses switch to repeated `Output:` headers; `--END COMMAND--` framing is gone |
| 15 | 4.0.0 | |
| 16 | 5.0.0 | |
| 17 | 6.0.0 | |
| 18 | 7.0.0 | |
| 19 | 8.0.0 | |
| 20 | 9.0.0 | |
| 21 | 10.0.0 | chan_sip removed (`SIPpeers` and friends gone) |
| 22 | 11.0.0 | |
| 23 | 12.0.0 | |
| master (post-23) | 13.0.0 | |

The library derives no behavioral decisions from the banner;
`Client.Banner()` exposes the raw line for diagnostics only. Both
`Command` output framings (pre- and post-14.2) are supported by the
parser.

## Support policy (pre-release)

- Supported protocol versions: AMI 2.0.0 and newer (Asterisk 12+); only
  AMI 1.x is out of scope.
- Planned live integration tests target Asterisk 18, 20, 22, and 23.
- Legacy-only behaviors — `--END COMMAND--` Command framing (pre-14.2),
  MD5 challenge login against older releases, and chan_sip-era list
  actions such as `SIPpeers`/`PeerlistComplete` — run against
  version-tagged synthetic fixtures instead of live systems.
- How supported Asterisk versions are labeled and retired in the README
  is an open question tracked in [design.md](design.md).
