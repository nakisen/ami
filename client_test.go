package ami

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

// script drives the server side of a piped session from test code. A
// pipe write completes only when the client consumed it, so sends
// double as synchronization points.
type script struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func newScript(t *testing.T, conn net.Conn) *script {
	return &script{t: t, conn: conn, br: bufio.NewReader(conn)}
}

func (s *script) send(raw string) {
	s.t.Helper()
	if _, err := s.conn.Write([]byte(raw)); err != nil {
		s.t.Errorf("script send: %v", err)
	}
}

// event sends one event frame from key/value pairs.
func (s *script) event(name string, kv ...string) {
	s.t.Helper()
	var b strings.Builder
	b.WriteString("Event: " + name + "\r\n")
	for i := 0; i < len(kv); i += 2 {
		b.WriteString(kv[i] + ": " + kv[i+1] + "\r\n")
	}
	b.WriteString("\r\n")
	s.send(b.String())
}

// respond sends one response frame for the given ActionID.
func (s *script) respond(id, disposition string, kv ...string) {
	s.t.Helper()
	var b strings.Builder
	b.WriteString("Response: " + disposition + "\r\n")
	if id != "" {
		b.WriteString("ActionID: " + id + "\r\n")
	}
	for i := 0; i < len(kv); i += 2 {
		b.WriteString(kv[i] + ": " + kv[i+1] + "\r\n")
	}
	b.WriteString("\r\n")
	s.send(b.String())
}

// action is one frame the fake server received.
type action struct {
	name   string
	id     string
	fields map[string]string
}

// readAction reads one action frame.
func (s *script) readAction() action {
	s.t.Helper()
	act := action{fields: make(map[string]string)}
	for {
		line, err := s.br.ReadString('\n')
		if err != nil {
			s.t.Fatalf("script read: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if act.name == "" {
				s.t.Fatal("script read an empty frame")
			}
			return act
		}
		key, value, ok := strings.Cut(line, ": ")
		if !ok {
			key, value, _ = strings.Cut(line, ":")
		}
		switch {
		case strings.EqualFold(key, "Action"):
			act.name = value
		case strings.EqualFold(key, "ActionID"):
			act.id = value
		default:
			act.fields[key] = value
		}
	}
}

// serveLogin performs the banner and login handshake, returning the
// received Login action for assertions.
func (s *script) serveLogin() action {
	s.t.Helper()
	s.send("Asterisk Call Manager/9.0.0\r\n")
	act := s.readAction()
	if act.name != "Login" {
		s.t.Errorf("first action = %q, want Login", act.name)
	}
	s.respond(act.id, "Success", "Message", "Authentication accepted")
	return act
}

// sync flushes the routing pipeline: everything sent before it has
// been routed once the barrier Do completes, because one goroutine
// reads and routes in order.
func (s *script) sync(c *Client) {
	s.t.Helper()
	done := make(chan error, 1)
	go func() {
		act, err := NewAction("Ping")
		if err != nil {
			done <- err
			return
		}
		_, err = c.Do(context.Background(), act)
		done <- err
	}()
	act := s.readAction()
	s.respond(act.id, "Success", "Ping", "Pong")
	if err := <-done; err != nil {
		s.t.Fatalf("sync barrier: %v", err)
	}
}

// testConfig is the synthetic base configuration piping to client.
func testConfig(client net.Conn) Config {
	return Config{
		Address:  "synthetic-test:5038",
		Username: "amitest",
		Secret:   "synthetic-secret",
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
		Keepalive: KeepaliveConfig{Disabled: true},
	}
}

// dialTest dials a piped client through the standard login handshake.
func dialTest(t *testing.T, mutate func(*Config)) (*Client, *script) {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	cfg := testConfig(clientEnd)
	if mutate != nil {
		mutate(&cfg)
	}
	s := newScript(t, serverEnd)
	handshake := make(chan struct{})
	go func() {
		defer close(handshake)
		s.serveLogin()
	}()
	c, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	<-handshake
	t.Cleanup(func() {
		c.Close()
		<-c.Done()
		serverEnd.Close()
	})
	return c, s
}

func mustDo(t *testing.T, c *Client, s *script, name string, respKV ...string) DoResult {
	t.Helper()
	done := make(chan struct{})
	var res DoResult
	var doErr error
	go func() {
		defer close(done)
		act, err := NewAction(name)
		if err != nil {
			doErr = err
			return
		}
		res, doErr = c.Do(context.Background(), act)
	}()
	act := s.readAction()
	if act.name != name {
		t.Errorf("server received action %q, want %q", act.name, name)
	}
	s.respond(act.id, "Success", respKV...)
	<-done
	if doErr != nil {
		t.Fatalf("Do(%s) error = %v", name, doErr)
	}
	return res
}

func TestDialPlainLogin(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	s := newScript(t, serverEnd)
	got := make(chan action, 1)
	go func() {
		got <- s.serveLogin()
	}()
	c, err := Dial(context.Background(), testConfig(clientEnd))
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() {
		c.Close()
		<-c.Done()
		serverEnd.Close()
	}()

	login := <-got
	if login.fields["Username"] != "amitest" || login.fields["Secret"] != "synthetic-secret" {
		t.Errorf("login credentials not sent: %v", login.fields)
	}
	if login.fields["Events"] != "off" {
		t.Errorf("login Events = %q, want off (empty EventMask)", login.fields["Events"])
	}
	if login.id == "" {
		t.Error("login carried no ActionID")
	}
	if c.Banner() != "Asterisk Call Manager/9.0.0" {
		t.Errorf("Banner() = %q", c.Banner())
	}
	if err := c.Err(); err != nil {
		t.Errorf("Err() on a running client = %v, want nil", err)
	}
}

func TestDialLoginRejected(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	defer serverEnd.Close()
	s := newScript(t, serverEnd)
	go func() {
		s.send("Asterisk Call Manager/9.0.0\r\n")
		act := s.readAction()
		s.respond(act.id, "Error", "Message", "Authentication failed")
	}()
	_, err := Dial(context.Background(), testConfig(clientEnd))
	var de *DialError
	if !errors.As(err, &de) || de.Phase != "login" {
		t.Fatalf("Dial() = %v, want DialError{login}", err)
	}
	if !errors.Is(err, ErrLoginFailed) {
		t.Fatalf("Dial() = %v, want ErrLoginFailed", err)
	}
	if strings.Contains(err.Error(), "Authentication") {
		t.Fatalf("DialError text embeds remote content: %q", err.Error())
	}
}

func TestDialMD5Login(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	s := newScript(t, serverEnd)
	handshake := make(chan struct{})
	go func() {
		defer close(handshake)
		s.send("Asterisk Call Manager/9.0.0\r\n")
		challenge := s.readAction()
		if challenge.name != "Challenge" || challenge.fields["AuthType"] != "MD5" {
			s.t.Errorf("challenge action = %+v", challenge)
		}
		s.respond(challenge.id, "Success", "Challenge", "112233445566")
		login := s.readAction()
		sum := md5.Sum([]byte("112233445566" + "synthetic-secret"))
		if login.fields["Key"] != hex.EncodeToString(sum[:]) {
			s.t.Errorf("login Key = %q, want the challenge digest", login.fields["Key"])
		}
		if _, ok := login.fields["Secret"]; ok {
			s.t.Error("MD5 login sent the plain secret")
		}
		s.respond(login.id, "Success")
	}()
	cfg := testConfig(clientEnd)
	cfg.Auth = AuthMD5
	c, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	<-handshake
	c.Close()
	<-c.Done()
	serverEnd.Close()
}

func TestDialEventMask(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	s := newScript(t, serverEnd)
	got := make(chan action, 1)
	go func() {
		got <- s.serveLogin()
	}()
	cfg := testConfig(clientEnd)
	cfg.EventMask = "system,call"
	c, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() {
		c.Close()
		<-c.Done()
		serverEnd.Close()
	}()
	if login := <-got; login.fields["Events"] != "system,call" {
		t.Errorf("login Events = %q, want the configured mask", login.fields["Events"])
	}
}

func TestDialValidation(t *testing.T) {
	bg := context.Background()
	if _, err := Dial(bg, Config{Username: "u"}); err == nil || !strings.Contains(err.Error(), "address") {
		t.Errorf("empty address: %v", err)
	}
	if _, err := Dial(bg, Config{Address: "a"}); err == nil || !strings.Contains(err.Error(), "username") {
		t.Errorf("empty username: %v", err)
	}
	if _, err := Dial(bg, Config{Address: "a", Username: "u", Auth: AuthMethod(9)}); err == nil || !strings.Contains(err.Error(), "auth") {
		t.Errorf("unknown auth: %v", err)
	}
	cfg := Config{Address: "a", Username: "u", Limits: Limits{MaxPending: -1}}
	if _, err := Dial(bg, cfg); err == nil || !strings.Contains(err.Error(), "MaxPending") {
		t.Errorf("negative limit: %v", err)
	}
	cfg = Config{Address: "a", Username: "u", Keepalive: KeepaliveConfig{Interval: -1}}
	if _, err := Dial(bg, cfg); err == nil || !strings.Contains(err.Error(), "Interval") {
		t.Errorf("negative keepalive: %v", err)
	}
}

func TestClientCloseLifecycle(t *testing.T) {
	c, _ := dialTest(t, nil)
	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	<-c.Done()
	if err := c.Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Err() after Close = %v, want ErrClosed", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	// A closed client rejects new work with ErrClosed.
	if _, err := c.Subscribe(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe() after Close = %v, want ErrClosed", err)
	}
	ping, err := NewAction("Ping")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(context.Background(), ping); !errors.Is(err, ErrClosed) {
		t.Fatalf("Do() after Close = %v, want ErrClosed", err)
	}
	if _, err := c.StartList(context.Background(), ping, ListSpec{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("StartList() after Close = %v, want ErrClosed", err)
	}
}

func TestServerEOFTerminatesClient(t *testing.T) {
	c, s := dialTest(t, nil)
	sub, err := c.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	s.conn.Close()
	<-c.Done()
	if err := c.Err(); !errors.Is(err, io.EOF) {
		t.Fatalf("Err() after remote close = %v, want io.EOF", err)
	}
	<-sub.Done()
	if err := sub.Err(); !errors.Is(err, io.EOF) {
		t.Fatalf("subscription Err() = %v, want the client root cause", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next() after death = %v, want the client root cause", err)
	}
}

func TestForeignResponseIsFatal(t *testing.T) {
	c, s := dialTest(t, nil)
	s.respond("alien-id", "Success")
	<-c.Done()
	var pe *ProtocolError
	if err := c.Err(); !errors.As(err, &pe) || pe.Category != "correlation" {
		t.Fatalf("Err() = %v, want a correlation ProtocolError", err)
	}
}

func TestResponseWithoutIDIsFatal(t *testing.T) {
	c, s := dialTest(t, nil)
	s.respond("", "Success")
	<-c.Done()
	var pe *ProtocolError
	if err := c.Err(); !errors.As(err, &pe) || pe.Category != "correlation" {
		t.Fatalf("Err() = %v, want a correlation ProtocolError", err)
	}
}

func TestUnclassifiableMessageIsFatal(t *testing.T) {
	c, s := dialTest(t, nil)
	s.send("Foo: bar\r\n\r\n")
	<-c.Done()
	var pe *ProtocolError
	if err := c.Err(); !errors.As(err, &pe) || pe.Category != "envelope" {
		t.Fatalf("Err() = %v, want an envelope ProtocolError", err)
	}
}

func TestClientDeathFailsPendingDo(t *testing.T) {
	c, s := dialTest(t, nil)
	doDone := make(chan error, 1)
	go func() {
		act, _ := NewAction("Originate")
		_, err := c.Do(context.Background(), act)
		doDone <- err
	}()
	s.readAction() // the action is fully written and awaiting its response
	s.conn.Close() // the server dies instead of answering
	err := <-doDone
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseResponse || !re.MayHaveExecuted() {
		t.Fatalf("Do() = %v, want RequestError{awaiting response, outcome unknown}", err)
	}
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("Do() = %v, want ErrOutcomeUnknown", err)
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Do() = %v, want the client root cause wrapped", err)
	}
}

func TestGoroutinesReleasedAfterClose(t *testing.T) {
	c, _ := dialTest(t, nil)
	c.Close()
	<-c.Done()
	prof := pprof.Lookup("goroutineleak")
	if prof == nil {
		t.Skip("goroutineleak profile unavailable")
	}
	if n := prof.Count(); n > 0 {
		var buf bytes.Buffer
		prof.WriteTo(&buf, 1)
		t.Fatalf("%d goroutines leaked after client teardown:\n%s", n, buf.String())
	}
}

func TestEventMaskDefaultIsOffAndBannerRetained(t *testing.T) {
	c, _ := dialTest(t, nil)
	if got := c.Banner(); got != "Asterisk Call Manager/9.0.0" {
		t.Fatalf("Banner() = %q", got)
	}
}

// TestLoginScrubsSecretFromWriteBuffer pins the credential-hygiene
// contract: after Dial, the library's only long-lived copy of the
// plain-auth secret — the connection's reused encode buffer — has been
// zeroed to its full capacity.
func TestLoginScrubsSecretFromWriteBuffer(t *testing.T) {
	c, _ := dialTest(t, nil)
	buf := c.conn.wbuf[:cap(c.conn.wbuf)]
	if bytes.Contains(buf, []byte("synthetic-secret")) {
		t.Fatal("the login secret survives in the connection's write buffer")
	}
}

// drainDeadline guards tests that wait on Done without synctest.
func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not terminate", what)
	}
}
