package ami

import (
	"fmt"
	"time"

	"github.com/nakisen/ami/internal/wire"
)

// Wire limit defaults. Inbound ceilings follow the headroom-over-
// strictness policy: they are ceilings, not allocations, so generosity
// costs memory only under attack or pathology. Outbound ceilings are
// anchored to the Asterisk manager parser (verified 2026-07-16 against
// the master and 18 branches), which rejects what exceeds them anyway.
const (
	defaultMaxBannerBytes        = 1 << 10   // pre-authentication read, deliberately the tightest
	defaultMaxLineBytes          = 32 << 10  // inbound line content
	defaultMaxFields             = 1024      // an order of magnitude above the largest real events
	defaultMaxMessageBytes       = 128 << 10 // inbound message outside command output
	defaultMaxCommandOutputLines = 64 << 10  // large "dialplan show"-class dumps with headroom
	defaultMaxCommandOutputBytes = 8 << 20   // 64Ki output lines at ~128 bytes each
	defaultMaxActionFields       = 128       // AST_MAX_MANHEADERS: the server rejects longer actions
	defaultMaxActionLineBytes    = 1022      // the server's 1024-byte input window minus CRLF
	defaultMaxActionBytes        = 128 << 10 // the server's aggregate ceiling of 128 lines x 1024 bytes

	// defaultMaxPartialFrameAge is a time dimension, so it is exempt
	// from the headroom policy and stays tight: 30 seconds still honors
	// the 8 MiB command-output ceiling arriving over a ~280 KiB/s link,
	// while ordinary frames complete in milliseconds.
	defaultMaxPartialFrameAge = 30 * time.Second
)

// WireLimits bounds every wire dimension of one connection. Each zero
// field selects the documented default — zero never means unbounded —
// and negative fields are rejected by NewConn. Line limits bound a
// line's content excluding its terminator; byte limits bound raw
// consumed or produced bytes including terminators.
//
// An inbound limit violation closes the connection, because framing
// beyond the violation cannot be trusted. An outbound limit violation
// rejects the action before any byte is written and leaves the
// connection usable.
type WireLimits struct {
	// MaxBannerBytes bounds the banner line. The banner is read before
	// authentication, so this is deliberately the tightest inbound
	// limit. Default 1024 (1 KiB).
	MaxBannerBytes int

	// MaxLineBytes bounds one inbound line, command output included.
	// Default 32768 (32 KiB).
	MaxLineBytes int

	// MaxFields bounds the fields of one inbound message, excluding
	// command output fields, which MaxCommandOutputLines bounds instead.
	// Default 1024.
	MaxFields int

	// MaxMessageBytes bounds one inbound message outside command output:
	// field lines, framing lines, and the terminating blank line.
	// Default 131072 (128 KiB).
	MaxMessageBytes int

	// MaxCommandOutputLines bounds the command output lines of one
	// inbound message under either Command framing: legacy payload lines
	// and modern repeated Output headers alike. Default 65536.
	MaxCommandOutputLines int

	// MaxCommandOutputBytes bounds the bytes of one inbound message's
	// command output lines. Default 8388608 (8 MiB).
	MaxCommandOutputBytes int

	// MaxActionFields bounds the encoded fields of one outbound action,
	// including the Action and ActionID envelope fields. The default of
	// 128 matches the Asterisk manager parser's AST_MAX_MANHEADERS: the
	// server rejects an action with more lines outright.
	MaxActionFields int

	// MaxActionLineBytes bounds one encoded outbound line. The default
	// of 1022 matches the Asterisk manager parser, which scans for the
	// line terminator within a 1024-byte window and discards longer
	// lines; 1022 content bytes plus CRLF fill that window exactly.
	MaxActionLineBytes int

	// MaxActionBytes bounds one outbound action's total encoded bytes.
	// Default 131072 (128 KiB), the server's aggregate ceiling of 128
	// maximal lines.
	MaxActionBytes int

	// MaxPartialFrameAge bounds the wall-clock life of one partially
	// read inbound frame. The clock starts when the frame's first byte
	// is consumed and stops when the frame completes, so an idle
	// connection with no pending frame is never affected. A frame still
	// incomplete past the age is an inbound violation: the read fails
	// and the connection closes, because a partial frame cannot be
	// resumed. Default 30 seconds.
	MaxPartialFrameAge time.Duration
}

// resolve applies defaults to zero fields, rejects negative fields, and
// returns the effective wire limits and partial-frame age.
func (l WireLimits) resolve() (wire.Limits, time.Duration, error) {
	w := wire.Limits{
		MaxBannerBytes:        defaultMaxBannerBytes,
		MaxLineBytes:          defaultMaxLineBytes,
		MaxFields:             defaultMaxFields,
		MaxMessageBytes:       defaultMaxMessageBytes,
		MaxCommandOutputLines: defaultMaxCommandOutputLines,
		MaxCommandOutputBytes: defaultMaxCommandOutputBytes,
		MaxActionFields:       defaultMaxActionFields,
		MaxActionLineBytes:    defaultMaxActionLineBytes,
		MaxActionBytes:        defaultMaxActionBytes,
	}
	for _, d := range []struct {
		name string
		set  int
		dst  *int
	}{
		{"MaxBannerBytes", l.MaxBannerBytes, &w.MaxBannerBytes},
		{"MaxLineBytes", l.MaxLineBytes, &w.MaxLineBytes},
		{"MaxFields", l.MaxFields, &w.MaxFields},
		{"MaxMessageBytes", l.MaxMessageBytes, &w.MaxMessageBytes},
		{"MaxCommandOutputLines", l.MaxCommandOutputLines, &w.MaxCommandOutputLines},
		{"MaxCommandOutputBytes", l.MaxCommandOutputBytes, &w.MaxCommandOutputBytes},
		{"MaxActionFields", l.MaxActionFields, &w.MaxActionFields},
		{"MaxActionLineBytes", l.MaxActionLineBytes, &w.MaxActionLineBytes},
		{"MaxActionBytes", l.MaxActionBytes, &w.MaxActionBytes},
	} {
		if d.set < 0 {
			return wire.Limits{}, 0, fmt.Errorf("ami: WireLimits.%s is negative", d.name)
		}
		if d.set > 0 {
			*d.dst = d.set
		}
	}
	age := defaultMaxPartialFrameAge
	if l.MaxPartialFrameAge < 0 {
		return wire.Limits{}, 0, fmt.Errorf("ami: WireLimits.MaxPartialFrameAge is negative")
	}
	if l.MaxPartialFrameAge > 0 {
		age = l.MaxPartialFrameAge
	}
	return w, age, nil
}
