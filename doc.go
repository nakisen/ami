// Package ami is a client for the Asterisk Manager Interface (AMI),
// built around message fidelity, correlation safety, bounded resource
// use, honest failure semantics, and first-class testing, with zero
// dependencies outside the Go standard library.
//
// A Message is an immutable, ordered field sequence: duplicate keys are
// legal AMI and their wire order is preserved. Conn frames messages over
// one established network connection under explicit WireLimits, with
// every blocking operation bounded by a context. Higher-level session
// features build on these primitives.
//
// The library supports AMI protocol versions 2.0.0 and newer
// (Asterisk 12+).
//
// The package is under active development and has no tagged release yet;
// every part of the exported API may change. See docs/design.md in the
// repository for the architecture and decided direction.
package ami
