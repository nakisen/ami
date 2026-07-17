// Package amitest provides a scriptable fake AMI server for testing
// AMI consumers without an Asterisk system.
//
// A Server listens on a real loopback socket, greets with a banner,
// authenticates plain and MD5 challenge logins, and dispatches every
// subsequent action to the handler registered for its name:
//
//	srv := amitest.NewServer(amitest.Config{})
//	defer srv.Close()
//	srv.HandleAction("QueueStatus", func(call *amitest.Call) {
//		call.Respond("Success", "EventList", "start")
//		call.Event("QueueMember", "Queue", "synthetic-support")
//		call.Event("QueueStatusComplete", "EventList", "Complete", "ListItems", "1")
//	})
//
//	client, err := ami.Dial(ctx, ami.Config{
//		Address:  srv.Addr(),
//		Username: amitest.DefaultUsername,
//		Secret:   amitest.DefaultSecret,
//	})
//
// # Scripting model
//
// Scenarios are composed in Go; no text script format exists. Handlers
// answer actions through their [Call], and [Server.Event] injects
// unsolicited traffic at any point. The server is strict: an action
// with no registered handler is answered with an error response and
// recorded as a scenario violation, and [Server.Err] — also returned
// by [Server.Close] — reports every violation, so a test can end with
//
//	if err := srv.Close(); err != nil {
//		t.Fatal(err)
//	}
//
// Built-in behavior is limited to what every session needs: the
// banner, the authentication phase, and default Ping, Logoff, and
// Events handlers that answer like Asterisk so keepalives, shutdown
// flows, and event-mask changes work unscripted. Each default can be
// replaced or removed through [Server.HandleAction]. Beyond that the
// server sends nothing on its own — no FullyBooted after login, no
// periodic events — so a session carries exactly the traffic its
// scenario scripted.
//
// Sessions carry an Asterisk-like event mask: the Login action's
// Events field sets it and the built-in Events action updates it, and
// [Server.Event] delivers only to sessions whose mask is on. The root
// client's zero configuration logs in with Events: off, so a consumer
// that subscribes without enabling events misses broadcasts here
// exactly as it would against a real Asterisk.
//
// # Synchronization
//
// The server writes over real sockets, so a returned send means the
// frame reached the socket, not the consumer. Observe delivery through
// the client under test: a blocking receive observes one event, and a
// Ping round-trip after scripted traffic proves the client routed
// everything sent earlier on that session, because the session's
// writes and the client's routing are both ordered. Real sockets also
// mean testing/synctest bubbles cannot host these tests; scenarios
// stay deterministic through such protocol barriers instead of a fake
// clock.
//
// The fake trusts its test code: frame-builder misuse — an odd
// key/value count, an empty name, header injection, an oversized
// frame — panics instead of returning an error, because a malformed
// scenario script is a bug in the test, not a condition to handle.
package amitest

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/nakisen/ami/internal/wire"
)

// Credentials and banner selected by the zero Config. They are
// exported so a test can dial the zero-configured server without
// repeating literals.
const (
	DefaultUsername = "amitest"
	DefaultSecret   = "amitest-secret"
	DefaultBanner   = "Asterisk Call Manager/9.0.0"
)

// Config configures a Server. The zero value is fully usable: a plain
// TCP listener on a loopback ephemeral port, the default credentials
// and banner, and whole-frame writes.
type Config struct {
	// Username and Secret are the credentials the server accepts, for
	// both plain and MD5 challenge login. Empty fields select
	// DefaultUsername and DefaultSecret. Usernames match
	// case-insensitively, like the Asterisk manager; secrets match
	// exactly.
	Username string
	Secret   string

	// Banner is the greeting line sent on connect, without its line
	// terminator. Empty selects DefaultBanner.
	Banner string

	// Addr is the listen address. Empty selects "127.0.0.1:0":
	// loopback only, ephemeral port.
	Addr string

	// TLS, when non-nil, serves TLS with this configuration.
	// LocalhostTLS generates a ready-made self-signed pair.
	TLS *tls.Config

	// WriteChunk, when positive, splits every outbound write into
	// chunks of at most this many bytes so consumers meet fragmented
	// delivery. Fragmentation is best effort: TCP may regroup chunks
	// in flight.
	WriteChunk int
}

// A Server is one fake AMI server. Its methods are safe for
// concurrent use.
type Server struct {
	cfg      Config
	listener net.Listener

	mu         sync.Mutex
	handlers   map[string]func(*Call)
	sessions   map[*session]struct{}
	accepted   int
	violations []error
	closed     bool

	wg sync.WaitGroup
}

// NewServer starts a server for cfg and returns it listening. It
// panics when the listener cannot be created, because a test host
// that cannot bind loopback is broken, not a condition to handle.
func NewServer(cfg Config) *Server {
	if cfg.Username == "" {
		cfg.Username = DefaultUsername
	}
	if cfg.Secret == "" {
		cfg.Secret = DefaultSecret
	}
	if cfg.Banner == "" {
		cfg.Banner = DefaultBanner
	}
	addr := cfg.Addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		panic("amitest: listen: " + err.Error())
	}
	if cfg.TLS != nil {
		l = tls.NewListener(l, cfg.TLS)
	}
	s := &Server{
		cfg:      cfg,
		listener: l,
		handlers: map[string]func(*Call){
			"ping":   defaultPing,
			"logoff": defaultLogoff,
			"events": defaultEvents,
		},
		sessions: map[*session]struct{}{},
	}
	s.wg.Go(s.acceptLoop)
	return s
}

// defaultPing answers like Asterisk, keeping keepalive-enabled clients
// healthy without scripting.
func defaultPing(c *Call) {
	c.Respond("Success", "Ping", "Pong")
}

// defaultLogoff answers Goodbye and hangs up, like Asterisk.
func defaultLogoff(c *Call) {
	c.Respond("Goodbye")
	c.Hangup()
}

// defaultEvents updates the session's event mask like Asterisk: only
// an explicit EventMask of off disables unsolicited delivery; on and
// class lists enable it.
func defaultEvents(c *Call) {
	on := !strings.EqualFold(c.Get("EventMask"), "off")
	c.sess.srv.mu.Lock()
	c.sess.events = on
	c.sess.srv.mu.Unlock()
	if on {
		c.Respond("Success", "Events", "On")
		return
	}
	c.Respond("Success", "Events", "Off")
}

// Addr returns the server's listen address in host:port form, ready
// for the client configuration's address field.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// HandleAction registers h for the named action under
// case-insensitive matching, replacing any previous handler. A nil h
// unregisters the name; unregistering Ping or Logoff removes the
// built-in default when a scenario wants those actions strict too.
//
// Handlers run on the receiving session's goroutine, one at a time
// per session in arrival order, so a handler's sends are ordered
// before the next action's. A handler that blocks delays only its own
// session. Registration is allowed at any time, including from
// handlers.
func (s *Server) HandleAction(name string, h func(*Call)) {
	key := strings.ToLower(name)
	s.mu.Lock()
	defer s.mu.Unlock()
	if h == nil {
		delete(s.handlers, key)
		return
	}
	s.handlers[key] = h
}

// Event sends one unsolicited event to every authenticated session
// whose event mask is on: a session that logged in with Events: off —
// the root client's zero-configuration default — receives nothing
// here until it enables events, exactly like a real Asterisk. kv
// lists the event's extra fields as ordered key/value pairs;
// duplicate keys are legal. The Event envelope field is composed here
// — do not pass it in kv. Correlated events belong to [Call.Event],
// which echoes the action's ActionID and ignores the mask.
func (s *Server) Event(name string, kv ...string) {
	s.broadcast(encodeFrame(eventFields(name, "", kv)), true)
}

// Raw writes b verbatim to every authenticated session, for malformed
// or hand-crafted traffic the frame builders refuse to compose. Raw
// is a wire tool, not a simulation of unsolicited delivery: it
// bypasses the event mask. Several frames concatenated into one Raw
// write exercise coalesced delivery, the counterpart of
// Config.WriteChunk fragmentation.
func (s *Server) Raw(b []byte) {
	s.broadcast(b, false)
}

// broadcast writes one pre-encoded chunk of bytes to every
// authenticated session — masked deliveries only to sessions whose
// event mask is on. Unauthenticated sessions are always excluded so
// background traffic can never interleave with a login exchange: like
// a real Asterisk, the fake sends an unauthenticated session nothing
// unsolicited.
func (s *Server) broadcast(b []byte, masked bool) {
	s.mu.Lock()
	live := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		if !sess.authed || (masked && !sess.events) {
			continue
		}
		live = append(live, sess)
	}
	s.mu.Unlock()
	for _, sess := range live {
		sess.writeRaw(b)
	}
}

// Hangup closes every live session's connection while keeping the
// listener open, so a reconnecting client finds the server again.
func (s *Server) Hangup() {
	s.mu.Lock()
	live := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		live = append(live, sess)
	}
	s.mu.Unlock()
	for _, sess := range live {
		sess.conn.Close()
	}
}

// SessionCount returns the number of connections the server has
// accepted since it started, ended sessions included. Reconnect
// scenarios assert on it.
func (s *Server) SessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

// Err returns every scenario violation observed so far, joined, or
// nil when the traffic matched the script. Violations are unexpected
// actions, actions before authentication, and inbound frames that
// break the wire protocol; a rejected login is a legitimate scenario,
// not a violation.
func (s *Server) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.violations...)
}

// Close stops the listener, hangs up every session, waits for the
// server's goroutines, and returns Err. It is idempotent. Close must
// not be called from a handler — it would wait on the handler's own
// session; hang up with [Call.Hangup] or [Server.Hangup] instead.
func (s *Server) Close() error {
	s.mu.Lock()
	first := !s.closed
	s.closed = true
	live := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		live = append(live, sess)
	}
	s.mu.Unlock()
	if first {
		s.listener.Close()
		for _, sess := range live {
			sess.conn.Close()
		}
	}
	s.wg.Wait()
	return s.Err()
}

// violate records one scenario violation for Err.
func (s *Server) violate(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.violations = append(s.violations, fmt.Errorf("amitest: "+format, args...))
}

// acceptLoop owns the listener: it registers each accepted connection
// as a session and serves it on its own goroutine. Session goroutines
// join the server's waitgroup, which is safe against a concurrent
// Close because the accept loop itself holds the group open.
func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				s.violate("accept failed: %v", err)
			}
			return
		}
		sess := &session{srv: s, conn: conn}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return
		}
		s.accepted++
		s.sessions[sess] = struct{}{}
		s.mu.Unlock()
		s.wg.Go(sess.serve)
	}
}

// encodeFrame renders one outbound frame, panicking on builder
// misuse: scenario scripts are test code, and a frame that cannot be
// encoded is a bug to surface at its call site.
func encodeFrame(fields []wire.Field) []byte {
	b, err := wire.AppendMessage(nil, fields, serverLimits)
	if err != nil {
		panic("amitest: invalid frame: " + err.Error())
	}
	return b
}

// eventFields composes an event frame's fields, echoing id when
// non-empty.
func eventFields(name, id string, kv []string) []wire.Field {
	if name == "" {
		panic("amitest: empty event name")
	}
	fields := []wire.Field{{Key: "Event", Value: name}}
	return appendKV(appendEnvelopeID(fields, id), kv)
}

// appendEnvelopeID echoes a correlated frame's ActionID. Hand-rolled
// clients may omit ActionID entirely; then their frames carry none
// back.
func appendEnvelopeID(fields []wire.Field, id string) []wire.Field {
	if id != "" {
		fields = append(fields, wire.Field{Key: "ActionID", Value: id})
	}
	return fields
}

// appendKV converts variadic ordered key/value pairs into fields.
func appendKV(fields []wire.Field, kv []string) []wire.Field {
	if len(kv)%2 != 0 {
		panic("amitest: odd key/value pair count")
	}
	for i := 0; i < len(kv); i += 2 {
		fields = append(fields, wire.Field{Key: kv[i], Value: kv[i+1]})
	}
	return fields
}
