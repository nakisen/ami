package ami

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/nakisen/ami/internal/demux"
)

// An AuthMethod selects how Dial authenticates. The zero value selects
// AuthPlain.
type AuthMethod uint8

const (
	// AuthPlain sends the secret in the Login action. Use it over
	// verified TLS; over plain TCP the secret crosses the network in
	// clear text.
	AuthPlain AuthMethod = 1 + iota

	// AuthMD5 performs the legacy MD5 challenge/response login. It is an
	// explicitly selected compatibility mode with no automatic downgrade
	// and no claim of transport security: it keeps the secret off the
	// wire but authenticates nothing else about the connection.
	AuthMD5
)

// Config configures Dial. Address, Username, and Secret identify the
// session; everything else has a working zero value.
type Config struct {
	// Address is the server's host:port.
	Address string

	// Username is the manager account name.
	Username string

	// Secret authenticates the account. It is used during login and not
	// retained in the established Client.
	Secret string

	// Auth selects the authentication method; zero selects AuthPlain.
	Auth AuthMethod

	// TLS, when non-nil, enables TLS: Dial clones the configuration,
	// derives ServerName from Address when verification is enabled and
	// the clone leaves it empty, and never disables verification on its
	// own. Nil dials plain TCP.
	TLS *tls.Config

	// DialContext, when non-nil, replaces the default dialer for the
	// underlying TCP connection.
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)

	// EventMask is the Events header value sent with Login. Empty sends
	// "Events: off", the documented gap-minimizing default: register
	// subscriptions first, then enable the desired mask with an Events
	// action through Do. A non-empty mask is applied at login, with the
	// documented consequence that events may arrive before any
	// subscription can be registered.
	EventMask string

	// Keepalive configures the application-level Ping keepalive. The
	// zero value selects the enabled defaults; disabling requires an
	// explicit Disabled: true.
	Keepalive KeepaliveConfig

	// Limits bounds every wire and session dimension. Zero fields select
	// the documented defaults.
	Limits Limits

	// Logger, when non-nil, receives internal diagnostics: lifecycle,
	// keepalive, subscription and list terminals, retirement records —
	// as allowlisted metadata only (names, counts, durations, reason
	// codes), never message contents, field values, credentials, or
	// endpoints. The handler never runs on the read loop; diagnostics
	// pass through a small bounded queue whose overflow drops
	// diagnostics and counts the drops. Nil is fully silent.
	Logger *slog.Logger
}

// KeepaliveConfig configures the application-level Ping keepalive. The
// zero value selects the enabled defaults; a zero value never silently
// changes safety behavior, so disabling requires an explicit Disabled.
type KeepaliveConfig struct {
	// Disabled turns the keepalive off entirely.
	Disabled bool

	// Interval is the quiet period between a valid Ping response and
	// the next Ping. Default 30s.
	Interval time.Duration

	// WriteTimeout bounds acquiring write ownership and fully emitting
	// one due Ping; missing it terminates the client with
	// ErrPingWriteTimeout. Default 5s.
	WriteTimeout time.Duration

	// Timeout bounds the wait for a fully written Ping's response;
	// missing it terminates the client with ErrPingTimeout. Default 10s.
	Timeout time.Duration
}

// Session limit defaults, ratified 2026-07-16 (see docs/decisions.md
// for the rationale). Byte and count ceilings follow the
// headroom-over-strictness policy — a ceiling is not an allocation —
// while time dimensions stay tight. The exception is the retirement
// lifetime: its expiry is client death, so it is deliberately looser
// than the keepalive timings that catch true server wedges.
const (
	defaultWriteAdmission     = 5 * time.Second
	defaultWriteAttempt       = 5 * time.Second
	defaultMaxPending         = 256
	defaultMaxRetirement      = 384 // pending 256 + lists 16 + record headroom
	defaultRetirementLifetime = 60 * time.Second

	defaultMaxSubscriptions       = 128
	defaultSubscriptionQueueItems = 512
	defaultSubscriptionQueueBytes = 2 << 20
	defaultMaxSubscriptionBytes   = 32 << 20
	defaultMaxMatcherNames        = 64
	defaultMaxMatcherBytes        = 4096

	defaultMaxLists          = 16
	defaultListQueueItems    = 4096
	defaultListQueueBytes    = 8 << 20
	defaultListObservedBytes = 32 << 20
	defaultMaxListBytes      = 64 << 20

	defaultKeepaliveInterval     = 30 * time.Second
	defaultKeepaliveWriteTimeout = 5 * time.Second
	defaultKeepaliveTimeout      = 10 * time.Second
)

// Limits bounds every dimension of one client. Each zero field selects
// the documented default — zero never means unbounded — and negative
// fields are rejected by Dial.
type Limits struct {
	// Wire bounds the connection's framing dimensions.
	Wire WireLimits

	// WriteAdmission bounds how long one dispatch may wait for write
	// ownership. A shorter caller context wins; failing admission is a
	// clean definitely-not-sent. Default 5s.
	WriteAdmission time.Duration

	// WriteAttempt bounds one action's socket write once admitted. A
	// shorter caller context wins. Default 5s.
	WriteAttempt time.Duration

	// MaxPending bounds concurrently in-flight public actions. Default
	// 256.
	MaxPending int

	// MaxRetirement bounds the reserved outcome-unknown retirement and
	// abandoned-list drain slots. Every admission holds one slot until
	// its action's outcome is proven, so this pool also bounds in-flight
	// work: it must exceed MaxPending plus MaxLists for those ceilings
	// to be reachable. Default 384.
	MaxRetirement int

	// RetirementLifetime bounds how long a retirement or drain record
	// may wait for its terminal evidence; expiry closes the client with
	// ErrRetirementExpired. Default 60s.
	RetirementLifetime time.Duration

	// MaxSubscriptions bounds concurrently registered subscriptions.
	// Default 128.
	MaxSubscriptions int

	// SubscriptionQueueItems and SubscriptionQueueBytes bound one
	// subscription's queue. Buffer overrides the item bound per
	// subscription; overflow closes that subscription with ErrLagged.
	// Defaults 512 items, 2 MiB.
	SubscriptionQueueItems int
	SubscriptionQueueBytes int

	// MaxSubscriptionBytes bounds the client-wide queued bytes across
	// subscriptions and follows. Default 32 MiB.
	MaxSubscriptionBytes int

	// MaxMatcherNames and MaxMatcherBytes bound one registration's
	// declarative name set — subscription matchers, follow selections,
	// and completion sets alike. Defaults 64 names, 4096 bytes.
	MaxMatcherNames int
	MaxMatcherBytes int

	// MaxLists bounds concurrently active lists. Default 16.
	MaxLists int

	// ListQueueItems and ListQueueBytes bound one list's queued state,
	// its stored completion event included. Defaults 4096 items, 8 MiB.
	ListQueueItems int
	ListQueueBytes int

	// ListObservedBytes bounds the cumulative wire bytes the remote may
	// stream through one list regardless of drain rate, ending a list
	// that never completes. Default 32 MiB.
	ListObservedBytes int

	// MaxListBytes bounds the client-wide retained list bytes. Default
	// 64 MiB.
	MaxListBytes int
}

// sessionLimits is the resolved form of Limits.
type sessionLimits struct {
	writeAdmission time.Duration
	writeAttempt   time.Duration

	subItems int
	subBytes int

	listItems    int
	listBytes    int
	listObserved int

	machine demux.Limits
}

// resolve applies defaults to zero fields, rejects negative fields, and
// returns the effective session limits. Wire limits resolve separately
// at NewConn.
func (l Limits) resolve() (sessionLimits, error) {
	s := sessionLimits{
		writeAdmission: defaultWriteAdmission,
		writeAttempt:   defaultWriteAttempt,
		subItems:       defaultSubscriptionQueueItems,
		subBytes:       defaultSubscriptionQueueBytes,
		listItems:      defaultListQueueItems,
		listBytes:      defaultListQueueBytes,
		listObserved:   defaultListObservedBytes,
		machine: demux.Limits{
			MaxPending:           defaultMaxPending,
			MaxSubscriptions:     defaultMaxSubscriptions,
			MaxSubscriptionBytes: defaultMaxSubscriptionBytes,
			MaxLists:             defaultMaxLists,
			MaxListBytes:         defaultMaxListBytes,
			MaxMatcherNames:      defaultMaxMatcherNames,
			MaxMatcherBytes:      defaultMaxMatcherBytes,
			MaxRetirement:        defaultMaxRetirement,
			RetirementLifetime:   int64(defaultRetirementLifetime),
		},
	}
	for _, d := range []struct {
		name string
		set  time.Duration
		dst  *time.Duration
	}{
		{"WriteAdmission", l.WriteAdmission, &s.writeAdmission},
		{"WriteAttempt", l.WriteAttempt, &s.writeAttempt},
	} {
		if d.set < 0 {
			return sessionLimits{}, fmt.Errorf("ami: Limits.%s is negative", d.name)
		}
		if d.set > 0 {
			*d.dst = d.set
		}
	}
	if l.RetirementLifetime < 0 {
		return sessionLimits{}, fmt.Errorf("ami: Limits.RetirementLifetime is negative")
	}
	if l.RetirementLifetime > 0 {
		s.machine.RetirementLifetime = int64(l.RetirementLifetime)
	}
	for _, d := range []struct {
		name string
		set  int
		dst  *int
	}{
		{"MaxPending", l.MaxPending, &s.machine.MaxPending},
		{"MaxRetirement", l.MaxRetirement, &s.machine.MaxRetirement},
		{"MaxSubscriptions", l.MaxSubscriptions, &s.machine.MaxSubscriptions},
		{"SubscriptionQueueItems", l.SubscriptionQueueItems, &s.subItems},
		{"SubscriptionQueueBytes", l.SubscriptionQueueBytes, &s.subBytes},
		{"MaxSubscriptionBytes", l.MaxSubscriptionBytes, &s.machine.MaxSubscriptionBytes},
		{"MaxMatcherNames", l.MaxMatcherNames, &s.machine.MaxMatcherNames},
		{"MaxMatcherBytes", l.MaxMatcherBytes, &s.machine.MaxMatcherBytes},
		{"MaxLists", l.MaxLists, &s.machine.MaxLists},
		{"ListQueueItems", l.ListQueueItems, &s.listItems},
		{"ListQueueBytes", l.ListQueueBytes, &s.listBytes},
		{"ListObservedBytes", l.ListObservedBytes, &s.listObserved},
		{"MaxListBytes", l.MaxListBytes, &s.machine.MaxListBytes},
	} {
		if d.set < 0 {
			return sessionLimits{}, fmt.Errorf("ami: Limits.%s is negative", d.name)
		}
		if d.set > 0 {
			*d.dst = d.set
		}
	}
	if err := s.machine.Validate(); err != nil {
		return sessionLimits{}, err
	}
	return s, nil
}

// resolve applies the keepalive defaults and rejects negative timings.
func (k KeepaliveConfig) resolve() (KeepaliveConfig, error) {
	r := KeepaliveConfig{
		Disabled:     k.Disabled,
		Interval:     defaultKeepaliveInterval,
		WriteTimeout: defaultKeepaliveWriteTimeout,
		Timeout:      defaultKeepaliveTimeout,
	}
	for _, d := range []struct {
		name string
		set  time.Duration
		dst  *time.Duration
	}{
		{"Interval", k.Interval, &r.Interval},
		{"WriteTimeout", k.WriteTimeout, &r.WriteTimeout},
		{"Timeout", k.Timeout, &r.Timeout},
	} {
		if d.set < 0 {
			return KeepaliveConfig{}, fmt.Errorf("ami: KeepaliveConfig.%s is negative", d.name)
		}
		if d.set > 0 {
			*d.dst = d.set
		}
	}
	return r, nil
}
