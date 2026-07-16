# Security Policy

## Supported versions

This library is pre-release: only the latest commit on `main` is
supported. There are no tagged releases yet.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: **Security → Report a
vulnerability** on this repository
(<https://github.com/nakisen/ami/security/advisories/new>). Do not open
public issues for security reports. You can expect an initial response
within 7 days.

## Scope

- Parsing and resource-bounding robustness against untrusted, malformed,
  or malicious AMI server input is in scope; fuzz findings are welcome.
- Credential handling (login secret lifetime) and TLS verification
  behavior are in scope.
- The library is a protocol client, not an authorization boundary:
  application-level permission checks, target validation, and audit are
  the consumer's responsibility (see docs/design.md).
