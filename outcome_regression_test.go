package ami

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/nakisen/ami/internal/demux"
)

// nthWriteFailureConn passes the login write through, then reports a
// caller-selected result for the next action write. When n is positive,
// those bytes really reach the peer before the supplied error is returned.
type nthWriteFailureConn struct {
	net.Conn

	mu     sync.Mutex
	writes int
	failAt int
	n      int
	err    error
}

func (c *nthWriteFailureConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	fail := c.writes == c.failAt
	n := c.n
	err := c.err
	c.mu.Unlock()
	if !fail {
		return c.Conn.Write(p)
	}
	if n <= 0 {
		return 0, err
	}
	if n > len(p) {
		n = len(p)
	}
	written, writeErr := c.Conn.Write(p[:n])
	if writeErr != nil {
		return written, writeErr
	}
	return written, err
}

// closeGateConn makes terminal Close observable and holds it until the
// test releases the gate. Its embedded I/O and deadlines retain normal
// net.Pipe behavior.
type closeGateConn struct {
	net.Conn

	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newCloseGateConn(conn net.Conn) *closeGateConn {
	return &closeGateConn{
		Conn:    conn,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *closeGateConn) Close() error {
	c.enteredOnce.Do(func() { close(c.entered) })
	<-c.release
	return c.Conn.Close()
}

func (c *closeGateConn) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func dialCloseGated(t *testing.T) (*Client, *closeGateConn) {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	gated := newCloseGateConn(clientEnd)
	s := newScript(t, serverEnd)
	handshake := make(chan struct{})
	go func() {
		defer close(handshake)
		s.serveLogin()
	}()
	c, err := Dial(context.Background(), testConfig(gated))
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	<-handshake
	t.Cleanup(func() {
		gated.unblock()
		c.Close()
		serverEnd.Close()
		<-c.Done()
	})
	return c, gated
}

func assertWriterHeldAtTerminalClose(t *testing.T, c *Client, gated *closeGateConn, result <-chan error) error {
	t.Helper()
	<-gated.entered
	select {
	case c.writeSem <- struct{}{}:
		c.releaseWriter()
		t.Fatal("a queued writer acquired ownership before failed-write resolution committed")
	default:
	}
	gated.unblock()
	err := <-result
	select {
	case c.writeSem <- struct{}{}:
		c.releaseWriter()
	default:
		t.Fatal("queued writer did not acquire ownership after failed-write resolution committed")
	}
	return err
}

func TestDispatchFailedWriteRetainsWriterThroughResolution(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, gated := dialCloseGated(t)

		c.writeSem <- struct{}{}
		action, _ := NewAction("Originate")
		id := c.newActionID(demux.KindRequest)
		c.mu.Lock()
		tkt, err := c.machine.Admit(id, demux.KindRequest, demux.AdmitOptions[Message]{})
		if err != nil {
			c.mu.Unlock()
			t.Fatal(err)
		}
		w := make(chan demux.Completion[Message], 1)
		c.waiters[tkt] = w
		response := newMessage([]Field{
			{Key: "Response", Value: "Success"},
			{Key: "ActionID", Value: id},
		})
		fx := c.machine.Route(demux.Envelope{
			Class: demux.ClassResponse, ActionID: id, Own: true,
			Kind: demux.KindRequest, Success: true, Size: 64,
		}, response)
		c.applyLockedFx(fx)
		c.mu.Unlock()
		// A response to a cleanly canceled, therefore provably unsent,
		// action is a terminal correlation contradiction. This makes the
		// full failure-resolution path observable at terminal Close.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result := make(chan error, 1)
		go func() {
			result <- c.writeAdmitted(ctx, action, id, tkt, w, demux.AdmitOptions[Message]{})
		}()
		err = assertWriterHeldAtTerminalClose(t, c, gated, result)
		var pe *ProtocolError
		if !errors.As(err, &pe) || pe.Category != "correlation" {
			t.Fatalf("writeAdmitted() = %v, want correlation ProtocolError", err)
		}
	})
}

func TestPingFailedWriteRetainsWriterThroughDeath(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, gated := dialCloseGated(t)

		// The server leaves the Ping unread. Fake time advances to the
		// write deadline, producing a clean zero-byte cancellation.
		result := make(chan error, 1)
		go func() { result <- c.ping() }()
		err := assertWriterHeldAtTerminalClose(t, c, gated, result)
		if !errors.Is(err, ErrPingWriteTimeout) {
			t.Fatalf("ping() = %v, want ErrPingWriteTimeout", err)
		}
		if !errors.Is(c.Err(), ErrPingWriteTimeout) {
			t.Fatalf("Err() = %v, want ErrPingWriteTimeout", c.Err())
		}
	})
}

func TestPingClosedConnDefersToTerminalOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := dialTest(t, nil)

		// Model the interval after another terminal path has marked the
		// Conn closed but before it commits the session's real root cause.
		// Keeping the underlying pipe open holds the reader out of this
		// deliberately controlled ownership handoff.
		c.conn.mu.Lock()
		c.conn.closed = true
		c.conn.mu.Unlock()

		result := make(chan error, 1)
		go func() { result <- c.ping() }()
		synctest.Wait()
		if err := c.Err(); err != nil {
			t.Fatalf("Ping committed generic closed cause = %v", err)
		}
		select {
		case err := <-result:
			t.Fatalf("ping() returned before the terminal owner committed: %v", err)
		default:
		}
		select {
		case c.writeSem <- struct{}{}:
			c.releaseWriter()
			t.Fatal("Ping released writer ownership before the terminal owner committed")
		default:
		}

		realCause := errors.New("synthetic reader root cause")
		c.die(realCause)
		synctest.Wait()
		if err := <-result; err != nil {
			t.Fatalf("ping() after terminal owner commit = %v, want nil", err)
		}
		if !errors.Is(c.Err(), realCause) {
			t.Fatalf("Err() = %v, want synthetic reader root cause", c.Err())
		}
		select {
		case c.writeSem <- struct{}{}:
			c.releaseWriter()
		default:
			t.Fatal("Ping retained writer ownership after terminal owner committed")
		}
	})
}

func TestClientWriteOutcomeUsesDispositionNotErrorChain(t *testing.T) {
	protocolCause := &ProtocolError{Category: "synthetic", Dimension: "transport"}
	for _, tt := range []struct {
		name       string
		n          int
		cause      error
		mayExecute bool
	}{
		{name: "zero-byte context-like transport failure", cause: context.Canceled},
		{name: "zero-byte protocol-like transport failure", cause: protocolCause},
		{name: "partial context-like transport failure", n: 1, cause: context.Canceled, mayExecute: true},
		{name: "partial protocol-like transport failure", n: 1, cause: protocolCause, mayExecute: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clientEnd, serverEnd := net.Pipe()
			wrapped := &nthWriteFailureConn{
				Conn:   clientEnd,
				failAt: 2, // Login is the first write.
				n:      tt.n,
				err:    tt.cause,
			}
			cfg := testConfig(wrapped)
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

			result := make(chan error, 1)
			go func() {
				action, actionErr := NewAction("Originate")
				if actionErr != nil {
					result <- actionErr
					return
				}
				_, actionErr = c.Do(context.Background(), action)
				result <- actionErr
			}()
			if tt.n > 0 {
				buf := make([]byte, tt.n)
				if _, err := serverEnd.Read(buf); err != nil {
					t.Fatalf("reading partial action: %v", err)
				}
			}

			err = <-result
			var re *RequestError
			if !errors.As(err, &re) || re.Phase != PhaseWrite || re.MayHaveExecuted() != tt.mayExecute {
				t.Fatalf("Do() = %v, want write RequestError with MayHaveExecuted=%v", err, tt.mayExecute)
			}
			if errors.Is(err, ErrOutcomeUnknown) != tt.mayExecute {
				t.Fatalf("errors.Is(ErrOutcomeUnknown) = %v, want %v", errors.Is(err, ErrOutcomeUnknown), tt.mayExecute)
			}
			waitDone(t, c.Done(), "client after transport write failure")
			if !errors.Is(c.Err(), tt.cause) {
				t.Fatalf("Err() = %v, want transport cause %v", c.Err(), tt.cause)
			}
		})
	}
}

func TestUnsentContradictoryResponseReleasesProvisionalBranches(t *testing.T) {
	for _, tt := range []struct {
		name  string
		kind  demux.Kind
		admit demux.AdmitOptions[Message]
		prime func(*Client, string)
	}{
		{
			name: "follow already completed",
			kind: demux.KindRequest,
			admit: demux.AdmitOptions[Message]{Follow: &demux.FollowOptions{
				Completions: []string{"OriginateResponse"},
				Caps:        demux.Caps{Items: 4, Bytes: 4096},
			}},
			prime: func(c *Client, id string) {
				fx := c.machine.Route(demux.Envelope{
					Class: demux.ClassEvent, Name: "originateresponse", ActionID: id,
					Own: true, Kind: demux.KindRequest, Size: 64,
				}, newMessage([]Field{{Key: "Event", Value: "OriginateResponse"}}))
				c.applyLockedFx(fx)
			},
		},
		{
			name: "list already completed",
			kind: demux.KindList,
			admit: demux.AdmitOptions[Message]{List: &demux.ListOptions[Message]{
				Caps:          demux.Caps{Items: 4, Bytes: 4096},
				ObservedBytes: 4096,
			}},
			prime: func(c *Client, id string) {
				fx := c.machine.Route(demux.Envelope{
					Class: demux.ClassEvent, Name: "statuscomplete", ActionID: id,
					Own: true, Kind: demux.KindList, Mark: demux.MarkComplete, Size: 64,
				}, newMessage([]Field{{Key: "Event", Value: "StatusComplete"}}))
				c.applyLockedFx(fx)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := dialTest(t, nil)
			id := c.newActionID(tt.kind)

			c.mu.Lock()
			tkt, err := c.machine.Admit(id, tt.kind, tt.admit)
			if err != nil {
				c.mu.Unlock()
				t.Fatal(err)
			}
			w := make(chan demux.Completion[Message], 1)
			c.waiters[tkt] = w
			tt.prime(c, id)
			response := newMessage([]Field{
				{Key: "Response", Value: "Success"},
				{Key: "ActionID", Value: id},
			})
			fx := c.machine.Route(demux.Envelope{
				Class: demux.ClassResponse, ActionID: id, Own: true,
				Kind: tt.kind, Success: true, Size: 64,
			}, response)
			c.applyLockedFx(fx)
			c.mu.Unlock()

			err = c.resolveNotSent(tkt, w, tt.admit)
			var pe *ProtocolError
			if !errors.As(err, &pe) || pe.Category != "correlation" {
				t.Fatalf("resolveNotSent() = %v, want correlation ProtocolError", err)
			}
			waitDone(t, c.Done(), "client after impossible unsent response")
			if _, dead := c.machine.Dead(); !dead {
				t.Fatal("demux machine remained alive after impossible unsent response")
			}
			if len(w) != 0 {
				t.Fatal("contradictory completion remained available to the caller")
			}
		})
	}
}
