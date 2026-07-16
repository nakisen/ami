package ami

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// failWriteConn fails writes once armed: with n == 0 for notSent mode,
// or with a fake partial count for partial mode.
type failWriteConn struct {
	net.Conn
	armed   atomic.Bool
	partial bool
	err     error
}

func (f *failWriteConn) Write(p []byte) (int, error) {
	if !f.armed.Load() {
		return f.Conn.Write(p)
	}
	if f.partial {
		return len(p) / 2, f.err
	}
	return 0, f.err
}

func dialFailWrite(t *testing.T, partial bool, cause error) (*Client, *script, *failWriteConn) {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	f := &failWriteConn{Conn: clientEnd, partial: partial, err: cause}
	cfg := testConfig(f)
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
	return c, s, f
}

func TestDoTransportFailureZeroBytesIsNotSent(t *testing.T) {
	cause := errors.New("synthetic transport failure")
	c, _, f := dialFailWrite(t, false, cause)
	f.armed.Store(true)
	act, _ := NewAction("Originate")
	_, err := c.Do(context.Background(), act)
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseWrite || re.MayHaveExecuted() {
		t.Fatalf("Do() = %v, want not-sent write RequestError", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("Do() = %v, want the transport cause wrapped", err)
	}
	// A zero-byte transport failure still closes the client.
	<-c.Done()
	if err := c.Err(); !errors.Is(err, cause) {
		t.Fatalf("Err() = %v, want the transport cause", err)
	}
}

func TestDoPartialWriteIsOutcomeUnknown(t *testing.T) {
	cause := errors.New("synthetic mid-write failure")
	c, _, f := dialFailWrite(t, true, cause)
	f.armed.Store(true)
	act, _ := NewAction("Originate")
	_, err := c.Do(context.Background(), act,
		WithFollow(FollowSpec{CompletionEvents: []string{"OriginateResponse"}}))
	var re *RequestError
	if !errors.As(err, &re) || re.Phase != PhaseWrite || !re.MayHaveExecuted() {
		t.Fatalf("Do() = %v, want outcome-unknown write RequestError", err)
	}
	if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, cause) {
		t.Fatalf("Do() = %v, want ErrOutcomeUnknown and the transport cause", err)
	}
	<-c.Done()
	if err := c.Err(); !errors.Is(err, cause) {
		t.Fatalf("Err() = %v, want the transport cause", err)
	}
}

func TestListOverflowTerminatesOnlyTheList(t *testing.T) {
	c, s := dialTest(t, func(cfg *Config) {
		cfg.Limits.ListQueueItems = 1
	})
	done := make(chan struct{})
	var list *List
	go func() {
		defer close(done)
		act, _ := NewAction("QueueStatus")
		list, _ = c.StartList(context.Background(), act, ListSpec{})
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done
	defer list.Close()

	s.event("QueueMember", "ActionID", act.id, "Queue", "one")
	s.event("QueueMember", "ActionID", act.id, "Queue", "two") // exceeds the single-item cap
	waitDone(t, list.Done(), "overflowed list")
	var le *ListError
	if err := list.Err(); !errors.As(err, &le) || le.Failure != ListOverflowed {
		t.Fatalf("Err() = %v, want ListError{overflowed}", err)
	}
	// The failure is local: the drain absorbs the rest and the client
	// lives.
	s.event("QueueStatusComplete", "ActionID", act.id, "EventList", "Complete")
	mustDo(t, c, s, "Ping")
}

func TestFollowLag(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan struct{})
	var res DoResult
	go func() {
		defer close(done)
		act, _ := NewAction("Originate")
		res, _ = c.Do(context.Background(), act, WithFollow(FollowSpec{BufferItems: 1}))
	}()
	act := s.readAction()
	s.respond(act.id, "Success")
	<-done
	defer res.Follow.Close()

	s.event("DialBegin", "ActionID", act.id)
	s.event("DialState", "ActionID", act.id) // exceeds the single-item buffer
	waitDone(t, res.Follow.Done(), "lagged follow")
	if err := res.Follow.Err(); !errors.Is(err, ErrLagged) {
		t.Fatalf("follow Err() = %v, want ErrLagged", err)
	}
	mustDo(t, c, s, "Ping")
}

func TestConflictingEnvelopeIsFatal(t *testing.T) {
	c, s := dialTest(t, nil)
	s.send("Event: Alpha\r\nEvent: Beta\r\n\r\n")
	<-c.Done()
	var pe *ProtocolError
	if err := c.Err(); !errors.As(err, &pe) || pe.Category != "envelope" {
		t.Fatalf("Err() = %v, want an envelope ProtocolError", err)
	}
}

func TestDoLegacyCommandResponse(t *testing.T) {
	c, s := dialTest(t, nil)
	done := make(chan struct{})
	var res DoResult
	var doErr error
	go func() {
		defer close(done)
		act, _ := NewAction("Command", Field{Key: "Command", Value: "core show version"})
		res, doErr = c.Do(context.Background(), act)
	}()
	act := s.readAction()
	s.send("Response: Follows\r\nActionID: " + act.id + "\r\nsynthetic line one\nsynthetic line two\n--END COMMAND--\r\n\r\n")
	<-done
	if doErr != nil {
		t.Fatalf("Do(Command) = %v", doErr)
	}
	out := res.Response.Values("Output")
	if len(out) != 2 || out[0] != "synthetic line one" || out[1] != "synthetic line two" {
		t.Fatalf("command output = %q", out)
	}
}

func TestDiagnosticsEmitAllowlistedMetadataOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		clientEnd, serverEnd := net.Pipe()
		cfg := testConfig(clientEnd)
		cfg.Logger = logger
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

		sub, err := c.Subscribe(MatchEvents("SecretEvent"))
		if err != nil {
			t.Fatal(err)
		}
		s.event("SecretEvent", "Password", "synthetic-super-secret")
		if _, err := sub.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
		c.Close()
		<-c.Done()
		s.conn.Close()
		synctest.Wait() // the diagnostics worker flushes and exits

		logged := buf.String()
		if !strings.Contains(logged, "session ready") {
			t.Fatalf("diagnostics missing lifecycle line:\n%s", logged)
		}
		if !strings.Contains(logged, "client terminated") {
			t.Fatalf("diagnostics missing terminal line:\n%s", logged)
		}
		if strings.Contains(logged, "synthetic-super-secret") || strings.Contains(logged, "SecretEvent") {
			t.Fatalf("diagnostics leaked message data:\n%s", logged)
		}
	})
}
