// Package amitest_test exercises the fake server through the real
// client: every test here is a full dogfooding round trip — Dial over
// a real socket, scripted handlers, pull-based consumption — the exact
// shape consumer tests take.
package amitest_test

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nakisen/ami"
	"github.com/nakisen/ami/amitest"
)

// newServer starts a strict server whose cleanup fails the test on
// scenario violations. Tests that expect violations construct their
// server manually instead.
func newServer(t *testing.T, cfg amitest.Config) *amitest.Server {
	t.Helper()
	srv := amitest.NewServer(cfg)
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("scenario violations: %v", err)
		}
	})
	return srv
}

// dialServer connects a real client to the server with the default
// synthetic credentials.
func dialServer(t *testing.T, srv *amitest.Server, mutate func(*ami.Config)) *ami.Client {
	t.Helper()
	cfg := ami.Config{
		Address:  srv.Addr(),
		Username: amitest.DefaultUsername,
		Secret:   amitest.DefaultSecret,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := ami.Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial(%s) error = %v", srv.Addr(), err)
	}
	t.Cleanup(func() {
		c.Close()
		<-c.Done()
	})
	return c
}

func mustDo(t *testing.T, c *ami.Client, name string, fields ...ami.Field) ami.DoResult {
	t.Helper()
	act, err := ami.NewAction(name, fields...)
	if err != nil {
		t.Fatalf("NewAction(%s) error = %v", name, err)
	}
	res, err := c.Do(context.Background(), act)
	if err != nil {
		t.Fatalf("Do(%s) error = %v", name, err)
	}
	return res
}

// waitDone bounds waits on lifecycle channels so a scenario bug fails
// fast instead of burning the suite timeout.
func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not complete", what)
	}
}

func TestDialAndBuiltinPing(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	c := dialServer(t, srv, nil)

	if got := c.Banner(); got != amitest.DefaultBanner {
		t.Errorf("Banner() = %q, want %q", got, amitest.DefaultBanner)
	}
	res := mustDo(t, c, "Ping")
	if got := res.Response.Get("Ping"); got != "Pong" {
		t.Errorf("built-in Ping answered %q, want Pong", got)
	}
	if n := srv.SessionCount(); n != 1 {
		t.Errorf("SessionCount() = %d, want 1", n)
	}
}

func TestMD5Login(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	c := dialServer(t, srv, func(cfg *ami.Config) { cfg.Auth = ami.AuthMD5 })
	mustDo(t, c, "Ping")
}

func TestLoginRejected(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	for name, mutate := range map[string]func(*ami.Config){
		"plain wrong secret": func(cfg *ami.Config) { cfg.Secret = "wrong" },
		"md5 wrong secret":   func(cfg *ami.Config) { cfg.Secret = "wrong"; cfg.Auth = ami.AuthMD5 },
		"unknown username":   func(cfg *ami.Config) { cfg.Username = "nobody" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := ami.Config{
				Address:  srv.Addr(),
				Username: amitest.DefaultUsername,
				Secret:   amitest.DefaultSecret,
			}
			mutate(&cfg)
			_, err := ami.Dial(context.Background(), cfg)
			var de *ami.DialError
			if !errors.As(err, &de) || de.Phase != "login" || !errors.Is(err, ami.ErrLoginFailed) {
				t.Fatalf("Dial() = %v, want DialError{login} wrapping ErrLoginFailed", err)
			}
		})
	}
	// Rejected logins are legitimate scenarios, not violations; the
	// strict cleanup on srv asserts that.
}

func TestUnexpectedActionIsStrict(t *testing.T) {
	srv := amitest.NewServer(amitest.Config{})
	c := dialServer(t, srv, nil)

	act, err := ami.NewAction("Reload")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Do(context.Background(), act)
	var re *ami.ResponseError
	if !errors.As(err, &re) {
		t.Fatalf("Do(unscripted) = %v, want *ResponseError", err)
	}
	if got := re.Response().Get("Message"); got != "amitest: unexpected action" {
		t.Errorf("error response Message = %q", got)
	}
	verr := srv.Err()
	if verr == nil || !strings.Contains(verr.Error(), `"Reload"`) {
		t.Fatalf("Err() = %v, want a violation naming Reload", verr)
	}
	if cerr := srv.Close(); cerr == nil || cerr.Error() != verr.Error() {
		t.Errorf("Close() = %v, want the recorded violations", cerr)
	}
}

func TestUnregisterBuiltinPing(t *testing.T) {
	srv := amitest.NewServer(amitest.Config{})
	defer srv.Close()
	srv.HandleAction("Ping", nil)
	c := dialServer(t, srv, nil)

	act, _ := ami.NewAction("Ping")
	_, err := c.Do(context.Background(), act)
	if _, ok := errors.AsType[*ami.ResponseError](err); !ok {
		t.Fatalf("Do(Ping) after unregister = %v, want *ResponseError", err)
	}
	if srv.Err() == nil {
		t.Error("Err() = nil, want the unexpected-Ping violation")
	}
}

func TestListFlow(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	srv.HandleAction("QueueStatus", func(call *amitest.Call) {
		call.Respond("Success", "EventList", "start", "Message", "Queue status will follow")
		call.Event("QueueParams", "Queue", "synthetic-support", "Max", "0")
		call.Event("QueueMember", "Queue", "synthetic-support", "Location", "PJSIP/synthetic-100")
		call.Event("QueueStatusComplete", "EventList", "Complete", "ListItems", "2")
	})
	c := dialServer(t, srv, nil)

	act, err := ami.NewAction("QueueStatus")
	if err != nil {
		t.Fatal(err)
	}
	list, err := c.StartList(context.Background(), act, ami.ListSpec{})
	if err != nil {
		t.Fatalf("StartList() error = %v", err)
	}
	defer list.Close()
	if got := list.Response().Get("Message"); got != "Queue status will follow" {
		t.Errorf("list response Message = %q", got)
	}
	var names []string
	for ev, err := range list.All(context.Background()) {
		if err != nil {
			t.Fatalf("All yielded error %v", err)
		}
		names = append(names, ev.Name())
	}
	if len(names) != 2 || names[0] != "QueueParams" || names[1] != "QueueMember" {
		t.Fatalf("list items = %v, want [QueueParams QueueMember]", names)
	}
	if err := list.Err(); err != nil {
		t.Errorf("Err() after clean drain = %v", err)
	}
}

// TestSnapshotPlusLiveWallboard is the queue-wallboard shape: a state
// snapshot list interleaved with live status events, each stream
// arriving on its own handle.
func TestSnapshotPlusLiveWallboard(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	srv.HandleAction("QueueStatus", func(call *amitest.Call) {
		call.Respond("Success", "EventList", "start")
		call.Event("QueueMember", "Queue", "synthetic-support", "Interface", "PJSIP/synthetic-100")
		srv.Event("QueueMemberStatus", "Queue", "synthetic-support", "Interface", "PJSIP/synthetic-100", "Status", "2")
		call.Event("QueueMember", "Queue", "synthetic-support", "Interface", "PJSIP/synthetic-101")
		srv.Event("QueueMemberStatus", "Queue", "synthetic-support", "Interface", "PJSIP/synthetic-101", "Status", "6")
		call.Event("QueueStatusComplete", "EventList", "Complete")
	})
	// The wallboard wants live events, so it must log in with them on:
	// the default Events: off would mask the broadcasts, here and on a
	// real Asterisk alike.
	c := dialServer(t, srv, func(cfg *ami.Config) { cfg.EventMask = "on" })

	sub, err := c.Subscribe(ami.MatchEvents("QueueMemberStatus"))
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	act, _ := ami.NewAction("QueueStatus")
	list, err := c.StartList(context.Background(), act, ami.ListSpec{})
	if err != nil {
		t.Fatalf("StartList() error = %v", err)
	}
	defer list.Close()
	var snapshot []string
	for ev, err := range list.All(context.Background()) {
		if err != nil {
			t.Fatalf("All yielded error %v", err)
		}
		snapshot = append(snapshot, ev.Get("Interface"))
	}
	if len(snapshot) != 2 {
		t.Fatalf("snapshot = %v, want two members", snapshot)
	}

	// The live events were written before the list completion on one
	// ordered session, so they are already buffered.
	for _, want := range []string{"2", "6"} {
		ev, err := sub.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if ev.Name() != "QueueMemberStatus" || ev.Get("Status") != want {
			t.Errorf("live event = %s status %q, want QueueMemberStatus status %q", ev.Name(), ev.Get("Status"), want)
		}
	}
}

// TestEventMaskGatesBroadcasts pins the Asterisk-like event mask: the
// root client's zero-configuration login sends Events: off, so a
// consumer that subscribes without enabling events misses broadcasts
// here exactly as it would in production — and the built-in Events
// action turns them on mid-session.
func TestEventMaskGatesBroadcasts(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	c := dialServer(t, srv, nil) // zero config: Events: off
	sub, err := c.Subscribe(ami.MatchEvents("PeerStatus"))
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer sub.Close()

	// Masked: never written to this session.
	srv.Event("PeerStatus", "Peer", "PJSIP/masked")

	res := mustDo(t, c, "Events", ami.Field{Key: "EventMask", Value: "on"})
	if got := res.Response.Get("Events"); got != "On" {
		t.Errorf("Events response = %q, want On", got)
	}

	srv.Event("PeerStatus", "Peer", "PJSIP/visible")
	ev, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if got := ev.Get("Peer"); got != "PJSIP/visible" {
		t.Fatalf("first delivered event Peer = %q: the masked broadcast leaked", got)
	}
}

// skipFrame reads and discards one server frame up to its blank line.
func skipFrame(t *testing.T, br *bufio.Reader) {
	t.Helper()
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("frame read: %v", err)
		}
		if line == "\r\n" {
			return
		}
	}
}

// TestMD5LoginRequiresAuthType pins login fidelity: like Asterisk, a
// Login carrying a Key but no AuthType: MD5 falls to the plaintext
// path and is rejected.
func TestMD5LoginRequiresAuthType(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("banner read: %v", err)
	}
	if _, err := conn.Write([]byte("Action: Challenge\r\nAuthType: MD5\r\nActionID: 1\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	skipFrame(t, br) // the challenge response; the challenge text is fixed

	sum := md5.Sum([]byte("112233445566" + amitest.DefaultSecret))
	login := "Action: Login\r\nUsername: " + amitest.DefaultUsername +
		"\r\nKey: " + hex.EncodeToString(sum[:]) + "\r\nActionID: 2\r\n\r\n"
	if _, err := conn.Write([]byte(login)); err != nil {
		t.Fatal(err)
	}
	reply, _ := io.ReadAll(br) // rejection, then hangup
	if !strings.Contains(string(reply), "Response: Error") {
		t.Fatalf("AuthType-less Key login reply = %q, want an Error response", reply)
	}
	// A rejected login is a legitimate scenario: the strict cleanup on
	// srv asserts no violation was recorded.
}

// TestDuplicateEnvelopeViolation: a conforming client never repeats
// the Action or ActionID envelope fields in one frame; the fake
// records the repeat while still dispatching on the first values.
func TestDuplicateEnvelopeViolation(t *testing.T) {
	srv := amitest.NewServer(amitest.Config{})
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("banner read: %v", err)
	}
	login := "Action: Login\r\nUsername: " + amitest.DefaultUsername +
		"\r\nSecret: " + amitest.DefaultSecret +
		"\r\nActionID: a\r\nActionID: b\r\n\r\n"
	if _, err := conn.Write([]byte(login)); err != nil {
		t.Fatal(err)
	}
	skipFrame(t, br) // login still succeeds on the first ActionID
	conn.Close()

	if err := srv.Close(); err == nil || !strings.Contains(err.Error(), "duplicate ActionID") {
		t.Fatalf("Close() = %v, want the duplicate-envelope violation", err)
	}
}

func TestLegacyCommandResponse(t *testing.T) {
	tests := map[string]struct {
		payload string
		want    []string
	}{
		"terminated payload": {"synthetic line one\nsynthetic line two\n", []string{"synthetic line one", "synthetic line two"}},
		"glued terminator":   {"no trailing newline", []string{"no trailing newline"}},
		"blank payload line": {"first\n\nlast\n", []string{"first", "", "last"}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			srv := newServer(t, amitest.Config{})
			srv.HandleAction("Command", func(call *amitest.Call) {
				call.RespondLegacyCommand(tc.payload)
			})
			c := dialServer(t, srv, nil)

			res := mustDo(t, c, "Command", ami.Field{Key: "Command", Value: "synthetic show"})
			if got := res.Response.Get("Response"); got != "Follows" {
				t.Errorf("Response = %q, want Follows", got)
			}
			if got := res.Response.Get("Privilege"); got != "Command" {
				t.Errorf("Privilege = %q, want Command", got)
			}
			got := res.Response.Values("Output")
			if len(got) != len(tc.want) {
				t.Fatalf("Output = %q, want %q", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("Output = %q, want %q", got, tc.want)
				}
			}
		})
	}
}

func TestModernCommandOutput(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	srv.HandleAction("Command", func(call *amitest.Call) {
		call.Respond("Success", "Output", "synthetic line one", "Output", "synthetic line two")
	})
	c := dialServer(t, srv, nil)

	res := mustDo(t, c, "Command", ami.Field{Key: "Command", Value: "synthetic show"})
	got := res.Response.Values("Output")
	if len(got) != 2 || got[0] != "synthetic line one" || got[1] != "synthetic line two" {
		t.Fatalf("Output = %q", got)
	}
}

func TestCallAccessors(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	type seen struct {
		name, id, channel string
		vars              []string
		ordered           []string
	}
	got := make(chan seen, 1)
	srv.HandleAction("Originate", func(call *amitest.Call) {
		s := seen{
			name:    call.Name(),
			id:      call.ActionID(),
			channel: call.Get("channel"), // case-insensitive
			vars:    call.Values("Variable"),
		}
		for k, v := range call.Fields() {
			s.ordered = append(s.ordered, k+"="+v)
		}
		got <- s
		call.Respond("Success")
	})
	c := dialServer(t, srv, nil)

	mustDo(t, c, "Originate",
		ami.Field{Key: "Channel", Value: "PJSIP/synthetic-100"},
		ami.Field{Key: "Variable", Value: "A=1"},
		ami.Field{Key: "Variable", Value: "B=2"},
	)
	s := <-got
	if s.name != "Originate" || s.id == "" {
		t.Errorf("envelope = %q/%q, want Originate with an ActionID", s.name, s.id)
	}
	if s.channel != "PJSIP/synthetic-100" {
		t.Errorf("Get(channel) = %q", s.channel)
	}
	if len(s.vars) != 2 || s.vars[0] != "A=1" || s.vars[1] != "B=2" {
		t.Errorf("Values(Variable) = %q", s.vars)
	}
	want := []string{"Channel=PJSIP/synthetic-100", "Variable=A=1", "Variable=B=2"}
	if len(s.ordered) != len(want) {
		t.Fatalf("Fields() = %q, want %q", s.ordered, want)
	}
	for i := range want {
		if s.ordered[i] != want[i] {
			t.Fatalf("Fields() = %q, want %q", s.ordered, want)
		}
	}
}

func TestDeferredReplyAcrossActions(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	held := make(chan *amitest.Call, 1)
	arrived := make(chan struct{})
	srv.HandleAction("Hold", func(call *amitest.Call) {
		held <- call
		close(arrived)
	})
	srv.HandleAction("Flush", func(call *amitest.Call) {
		h := <-held
		h.Respond("Success", "Origin", "held")
		call.Respond("Success", "Origin", "flush")
	})
	c := dialServer(t, srv, nil)

	type outcome struct {
		res ami.DoResult
		err error
	}
	holdDone := make(chan outcome, 1)
	go func() {
		act, err := ami.NewAction("Hold")
		if err != nil {
			holdDone <- outcome{err: err}
			return
		}
		res, err := c.Do(context.Background(), act)
		holdDone <- outcome{res: res, err: err}
	}()
	waitDone(t, arrived, "Hold arrival")

	if res := mustDo(t, c, "Flush"); res.Response.Get("Origin") != "flush" {
		t.Errorf("Flush response Origin = %q", res.Response.Get("Origin"))
	}
	select {
	case o := <-holdDone:
		if o.err != nil {
			t.Fatalf("Do(Hold) error = %v", o.err)
		}
		if got := o.res.Response.Get("Origin"); got != "held" {
			t.Errorf("Hold response Origin = %q, want held", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("deferred Hold response never arrived")
	}
}

func TestRawMalformedFrameKillsClient(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	c := dialServer(t, srv, nil)

	srv.Raw([]byte("::::\r\n\r\n"))
	waitDone(t, c.Done(), "client death")
	var pe *ami.ProtocolError
	if err := c.Err(); !errors.As(err, &pe) || pe.Category != "framing" {
		t.Fatalf("Err() = %v, want a framing ProtocolError", err)
	}
	// The client's teardown close is a clean end server-side; the
	// strict cleanup on srv asserts no violation was recorded.
}

func TestHangupAndReconnect(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	c1 := dialServer(t, srv, nil)

	srv.Hangup()
	waitDone(t, c1.Done(), "hung-up client death")
	if c1.Err() == nil {
		t.Error("Err() after server hangup = nil, want the transport cause")
	}

	c2 := dialServer(t, srv, nil)
	mustDo(t, c2, "Ping")
	if n := srv.SessionCount(); n != 2 {
		t.Errorf("SessionCount() = %d, want 2", n)
	}
}

func TestBuiltinLogoff(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	c := dialServer(t, srv, nil)

	act, _ := ami.NewAction("Logoff")
	_, err := c.Do(context.Background(), act)
	var re *ami.ResponseError
	if !errors.As(err, &re) || re.Response().Get("Response") != "Goodbye" {
		t.Fatalf("Do(Logoff) = %v, want a Goodbye ResponseError", err)
	}
	waitDone(t, c.Done(), "post-Logoff hangup")
}

func TestTLS(t *testing.T) {
	serverCfg, clientCfg := amitest.LocalhostTLS()
	srv := newServer(t, amitest.Config{TLS: serverCfg})
	c := dialServer(t, srv, func(cfg *ami.Config) { cfg.TLS = clientCfg })
	mustDo(t, c, "Ping")
}

func TestWriteChunkFragmentation(t *testing.T) {
	srv := newServer(t, amitest.Config{WriteChunk: 3})
	srv.HandleAction("QueueStatus", func(call *amitest.Call) {
		call.Respond("Success", "EventList", "start")
		call.Event("QueueMember", "Queue", "synthetic-support")
		call.Event("QueueStatusComplete", "EventList", "Complete")
	})
	c := dialServer(t, srv, nil)

	act, _ := ami.NewAction("QueueStatus")
	list, err := c.StartList(context.Background(), act, ami.ListSpec{})
	if err != nil {
		t.Fatalf("StartList() over fragmented writes error = %v", err)
	}
	defer list.Close()
	var items int
	for _, err := range list.All(context.Background()) {
		if err != nil {
			t.Fatalf("All yielded error %v", err)
		}
		items++
	}
	if items != 1 {
		t.Fatalf("items = %d, want 1", items)
	}
}

func TestKeepalivePingsFlowThroughServer(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	pinged := make(chan struct{}, 1)
	srv.HandleAction("Ping", func(call *amitest.Call) {
		select {
		case pinged <- struct{}{}:
		default:
		}
		call.Respond("Success", "Ping", "Pong")
	})
	c := dialServer(t, srv, func(cfg *ami.Config) {
		cfg.Keepalive = ami.KeepaliveConfig{Interval: 20 * time.Millisecond}
	})

	waitDone(t, pinged, "first keepalive ping")
	if err := c.Err(); err != nil {
		t.Fatalf("Err() after a served keepalive = %v", err)
	}
}

func TestPreAuthViolations(t *testing.T) {
	srv := amitest.NewServer(amitest.Config{})

	// A frame with no Action field.
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("banner read: %v", err)
	}
	if _, err := conn.Write([]byte("Foo: bar\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, br) // the server hangs up without replying
	conn.Close()

	// An ordinary action before authentication.
	conn2, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	br2 := bufio.NewReader(conn2)
	if _, err := br2.ReadString('\n'); err != nil {
		t.Fatalf("banner read: %v", err)
	}
	if _, err := conn2.Write([]byte("Action: Reload\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	reply, _ := io.ReadAll(br2)
	conn2.Close()
	if !strings.Contains(string(reply), "Response: Error") {
		t.Errorf("pre-auth action reply = %q, want an Error response", reply)
	}

	verr := srv.Close()
	if verr == nil {
		t.Fatal("Close() = nil, want the two recorded violations")
	}
	for _, want := range []string{"no Action field", "before authentication"} {
		if !strings.Contains(verr.Error(), want) {
			t.Errorf("violations = %v, missing %q", verr, want)
		}
	}
}

func TestBuilderMisusePanics(t *testing.T) {
	srv := newServer(t, amitest.Config{})
	mustPanic := func(what string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", what)
			}
		}()
		f()
	}
	mustPanic("odd key/value count", func() { srv.Event("Odd", "key") })
	mustPanic("empty event name", func() { srv.Event("") })
	mustPanic("value injection", func() { srv.Event("Evil", "Key", "a\r\nInjected: b") })
}

func TestServerCloseIdempotentAndRefusesDial(t *testing.T) {
	srv := amitest.NewServer(amitest.Config{})
	addr := srv.Addr()
	if err := srv.Close(); err != nil {
		t.Fatalf("first Close() = %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	_, err := ami.Dial(context.Background(), ami.Config{
		Address:  addr,
		Username: amitest.DefaultUsername,
		Secret:   amitest.DefaultSecret,
	})
	var de *ami.DialError
	if !errors.As(err, &de) || de.Phase != "dial" {
		t.Fatalf("Dial() after Close = %v, want DialError{dial}", err)
	}
}
