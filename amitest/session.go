package amitest

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"iter"
	"net"
	"strings"
	"sync"

	"github.com/nakisen/ami/internal/wire"
)

// serverLimits bounds both directions of a fake session. The inbound
// dimensions mirror the client library's inbound defaults so the fake
// tolerates at least what a real server tolerates; the action
// dimensions bound the fake's own outbound frames, so they carry the
// server-message ceilings a client is expected to accept, not the
// Asterisk action-parser ceilings.
var serverLimits = wire.Limits{
	MaxBannerBytes:        1 << 10,
	MaxLineBytes:          32 << 10,
	MaxFields:             1024,
	MaxMessageBytes:       128 << 10,
	MaxCommandOutputLines: 64 << 10,
	MaxCommandOutputBytes: 8 << 20,
	MaxActionFields:       1024,
	MaxActionLineBytes:    32 << 10,
	MaxActionBytes:        128 << 10,
}

// challengeText is the deterministic MD5 challenge every session
// issues. The fake is not a security boundary, and a fixed challenge
// keeps scenarios reproducible.
const challengeText = "112233445566"

// errNoAction reports an inbound frame without an Action field where
// an action is required.
var errNoAction = errors.New("frame carries no Action field")

// A session serves one accepted connection.
type session struct {
	srv  *Server
	conn net.Conn

	// wmu serializes writes: handler replies and broadcasts from other
	// goroutines interleave at frame granularity.
	wmu sync.Mutex

	// authed marks login completion; guarded by srv.mu so broadcasts
	// snapshot it consistently.
	authed bool
}

// serve runs one session to completion: banner, authentication, then
// action dispatch until the stream ends.
func (sess *session) serve() {
	defer func() {
		sess.conn.Close()
		sess.srv.mu.Lock()
		delete(sess.srv.sessions, sess)
		sess.srv.mu.Unlock()
	}()
	sess.writeRaw([]byte(sess.srv.cfg.Banner + "\r\n"))
	r := wire.NewReader(sess.conn, serverLimits)
	if !sess.authenticate(r) {
		return
	}
	for {
		call, err := sess.readCall(r)
		if err != nil {
			sess.reportReadEnd(err)
			return
		}
		sess.dispatch(call)
	}
}

// authenticate runs the pre-session phase. Challenge and Login are the
// only actions a session may send before authenticating; anything else
// is a violation. It reports whether dispatch may begin.
func (sess *session) authenticate(r *wire.Reader) bool {
	issued := false
	for {
		call, err := sess.readCall(r)
		if err != nil {
			sess.reportReadEnd(err)
			return false
		}
		switch strings.ToLower(call.name) {
		case "challenge":
			if !strings.EqualFold(call.Get("AuthType"), "MD5") {
				call.Respond("Error", "Message", "Authentication type not supported")
				continue
			}
			issued = true
			call.Respond("Success", "Challenge", challengeText)
		case "login":
			if !sess.checkLogin(call, issued) {
				call.Respond("Error", "Message", "Authentication failed")
				return false
			}
			// Mark before responding: once the client holds the success
			// response, broadcasts must already see this session.
			sess.srv.mu.Lock()
			sess.authed = true
			sess.srv.mu.Unlock()
			call.Respond("Success", "Message", "Authentication accepted")
			return true
		default:
			sess.srv.violate("action %q before authentication", call.name)
			call.Respond("Error", "Message", "Permission denied")
			return false
		}
	}
}

// checkLogin verifies a Login action: an MD5 Key when one was
// challenged for, the plain Secret otherwise. Usernames match
// case-insensitively like the Asterisk manager; secrets exactly.
func (sess *session) checkLogin(call *Call, issued bool) bool {
	cfg := sess.srv.cfg
	if !strings.EqualFold(call.Get("Username"), cfg.Username) {
		return false
	}
	if key := call.Get("Key"); key != "" {
		if !issued {
			return false
		}
		sum := md5.Sum([]byte(challengeText + cfg.Secret))
		return key == hex.EncodeToString(sum[:])
	}
	return call.Get("Secret") == cfg.Secret
}

// dispatch routes one authenticated action to its handler. A missing
// handler is the strictness contract: the action is answered with an
// error response so the client is not left waiting, and the violation
// is recorded for Err.
func (sess *session) dispatch(call *Call) {
	sess.srv.mu.Lock()
	h := sess.srv.handlers[strings.ToLower(call.name)]
	sess.srv.mu.Unlock()
	if h == nil {
		sess.srv.violate("unexpected action %q", call.name)
		call.Respond("Error", "Message", "amitest: unexpected action")
		return
	}
	h(call)
}

// readCall reads one inbound frame and shapes it as a received action:
// the first Action value becomes the name, the first ActionID value
// the id, and every other field stays in wire order.
func (sess *session) readCall(r *wire.Reader) (*Call, error) {
	raw, err := r.ReadMessage()
	if err != nil {
		return nil, err
	}
	call := &Call{sess: sess}
	for _, f := range raw {
		switch {
		case call.name == "" && strings.EqualFold(f.Key, "Action"):
			call.name = f.Value
		case call.id == "" && strings.EqualFold(f.Key, "ActionID"):
			call.id = f.Value
		default:
			call.fields = append(call.fields, f)
		}
	}
	if call.name == "" {
		return nil, errNoAction
	}
	return call, nil
}

// reportReadEnd classifies why a session's inbound stream ended.
// Clean ends pass silently: EOF, a locally closed connection, and
// transport failures, because an abruptly discarded client is
// ordinary test traffic. Frames that break the wire protocol are
// scenario violations.
func (sess *session) reportReadEnd(err error) {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
	case errors.Is(err, errNoAction):
		sess.srv.violate("inbound frame carries no Action field")
	case wireViolation(err):
		sess.srv.violate("malformed inbound frame: %v", err)
	}
}

// wireViolation reports whether err is one of the parser's protocol
// violations, as opposed to a transport failure.
func wireViolation(err error) bool {
	for _, w := range []error{
		wire.ErrLineTooLong,
		wire.ErrTooManyFields,
		wire.ErrMessageTooLarge,
		wire.ErrTooManyOutputLines,
		wire.ErrOutputTooLarge,
		wire.ErrMalformedLine,
		wire.ErrEmptyMessage,
		wire.ErrCommandFraming,
	} {
		if errors.Is(err, w) {
			return true
		}
	}
	return false
}

// writeFrame encodes and writes one frame to this session.
func (sess *session) writeFrame(fields []wire.Field) {
	sess.writeRaw(encodeFrame(fields))
}

// writeRaw writes b under the session write lock, split into
// WriteChunk-sized chunks when configured. Write errors are
// discarded: the session died mid-scenario, and the client under test
// observes that on its own side of the socket.
func (sess *session) writeRaw(b []byte) {
	sess.wmu.Lock()
	defer sess.wmu.Unlock()
	chunk := sess.srv.cfg.WriteChunk
	if chunk <= 0 {
		chunk = len(b)
	}
	for len(b) > 0 {
		n := min(chunk, len(b))
		if _, err := sess.conn.Write(b[:n]); err != nil {
			return
		}
		b = b[n:]
	}
}

// A Call is one action the server received, offered to its handler
// with the reply primitives. The receive accessors are immutable
// views. The reply methods write to the session that sent the action
// and remain valid after the handler returns: a handler may capture
// its Call and reply later, from any goroutine, to script delayed or
// out-of-order traffic. Replies to a session that has since ended are
// discarded.
type Call struct {
	sess   *session
	name   string
	id     string
	fields []wire.Field
}

// Name returns the action name.
func (c *Call) Name() string {
	return c.name
}

// ActionID returns the action's ActionID, empty when the client sent
// none. Replies echo it automatically.
func (c *Call) ActionID() string {
	return c.id
}

// Get returns the value of the action's first extra field whose key
// equals key under case-insensitive matching, or the empty string when
// no such field exists. The Action and ActionID envelope fields are
// not part of the extra fields.
func (c *Call) Get(key string) string {
	for _, f := range c.fields {
		if strings.EqualFold(f.Key, key) {
			return f.Value
		}
	}
	return ""
}

// Values returns the values of every extra field whose key equals key
// under case-insensitive matching, in wire order, or nil when no such
// field exists.
func (c *Call) Values(key string) []string {
	var vals []string
	for _, f := range c.fields {
		if strings.EqualFold(f.Key, key) {
			vals = append(vals, f.Value)
		}
	}
	return vals
}

// Fields returns an iterator over the action's extra fields as
// (key, value) pairs in wire order, duplicates included. The Action
// and ActionID envelope fields are excluded; Name and ActionID carry
// them.
func (c *Call) Fields() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for _, f := range c.fields {
			if !yield(f.Key, f.Value) {
				return
			}
		}
	}
}

// Respond sends the action's response: the Response field carrying
// disposition — Success, Error, Goodbye, or whatever the scenario
// needs — the echoed ActionID, then kv as ordered key/value pairs.
func (c *Call) Respond(disposition string, kv ...string) {
	if disposition == "" {
		panic("amitest: empty response disposition")
	}
	fields := []wire.Field{{Key: "Response", Value: disposition}}
	c.sess.writeFrame(appendKV(appendEnvelopeID(fields, c.id), kv))
}

// Event sends one correlated event echoing the action's ActionID:
// list items, list completion events, and action-triggered events.
// Uncorrelated background traffic goes through [Server.Event].
func (c *Call) Event(name string, kv ...string) {
	c.sess.writeFrame(eventFields(name, c.id, kv))
}

// RespondLegacyCommand sends a legacy Command response (the
// Asterisk 12–14.1 framing): "Response: Follows" with raw payload
// terminated by "--END COMMAND--". The frame is composed as raw bytes
// because the encoder refuses this shape by design — a first field of
// "Response: Follows" re-parses as legacy framing. The payload is
// written verbatim: a payload ending in a newline keeps the
// terminator on its own line, and one that does not glues the
// terminator to its last line, exactly what a CLI command without a
// trailing newline produces.
func (c *Call) RespondLegacyCommand(payload string) {
	var b strings.Builder
	b.WriteString("Response: Follows\r\nPrivilege: Command\r\n")
	if c.id != "" {
		b.WriteString("ActionID: " + c.id + "\r\n")
	}
	b.WriteString(payload)
	b.WriteString("--END COMMAND--\r\n\r\n")
	c.sess.writeRaw([]byte(b.String()))
}

// Raw writes b verbatim to the calling session, for malformed or
// hand-crafted frames the builders refuse to compose.
func (c *Call) Raw(b []byte) {
	c.sess.writeRaw(b)
}

// Hangup closes the calling session's connection: the scripted abrupt
// disconnect.
func (c *Call) Hangup() {
	c.sess.conn.Close()
}
