package ami

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nakisen/ami/internal/wire"
)

func newPipeConn(t *testing.T, limits WireLimits) (*Conn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	c, err := NewConn(client, limits)
	if err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}
	t.Cleanup(func() {
		c.Close()
		server.Close()
	})
	return c, server
}

func mustAction(t *testing.T, name string, fields ...Field) Action {
	t.Helper()
	a, err := NewAction(name, fields...)
	if err != nil {
		t.Fatalf("NewAction(%q) error = %v", name, err)
	}
	return a
}

// signalConn closes a channel the first time Read or Write is entered,
// letting tests cancel an operation that is provably in flight.
type signalConn struct {
	net.Conn
	readEntered  chan struct{}
	writeEntered chan struct{}
	readOnce     sync.Once
	writeOnce    sync.Once
}

func newSignalConn(c net.Conn) *signalConn {
	return &signalConn{
		Conn:         c,
		readEntered:  make(chan struct{}),
		writeEntered: make(chan struct{}),
	}
}

func (c *signalConn) Read(p []byte) (int, error) {
	c.readOnce.Do(func() { close(c.readEntered) })
	return c.Conn.Read(p)
}

func (c *signalConn) Write(p []byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writeEntered) })
	return c.Conn.Write(p)
}

func TestNewConnValidation(t *testing.T) {
	if _, err := NewConn(nil, WireLimits{}); err == nil {
		t.Fatal("NewConn(nil) succeeded")
	}
	client, server := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	if _, err := NewConn(client, WireLimits{MaxLineBytes: -1}); err == nil || !strings.Contains(err.Error(), "MaxLineBytes") {
		t.Fatalf("NewConn with negative limit: err = %v, want error naming MaxLineBytes", err)
	}
	if _, err := NewConn(client, WireLimits{MaxPartialFrameAge: -time.Second}); err == nil || !strings.Contains(err.Error(), "MaxPartialFrameAge") {
		t.Fatalf("NewConn with negative age: err = %v, want error naming MaxPartialFrameAge", err)
	}
	// Constructor failure leaves ownership with the caller: the same
	// connection must still be fully usable.
	c, err := NewConn(client, WireLimits{})
	if err != nil {
		t.Fatalf("NewConn() after failed construction: %v", err)
	}
	go server.Write([]byte("Event: Reused\r\n\r\n"))
	msg, err := c.ReadMessage(context.Background())
	if err != nil || msg.Get("Event") != "Reused" {
		t.Fatalf("ReadMessage() = (%v, %v)", msg, err)
	}
}

func TestConnReadBannerAndMessage(t *testing.T) {
	c, server := newPipeConn(t, WireLimits{})
	go server.Write([]byte("Asterisk Call Manager/5.0.2\r\nEvent: FullyBooted\r\nUptime: 1\r\n\r\n"))
	banner, err := c.ReadBanner(context.Background())
	if err != nil || banner != "Asterisk Call Manager/5.0.2" {
		t.Fatalf("ReadBanner() = (%q, %v)", banner, err)
	}
	msg, err := c.ReadMessage(context.Background())
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if msg.Get("Event") != "FullyBooted" || msg.Get("Uptime") != "1" {
		t.Fatalf("unexpected message: %v", msg)
	}
}

func TestConnReadMessageLegacyCommand(t *testing.T) {
	c, server := newPipeConn(t, WireLimits{})
	go server.Write([]byte("Response: Follows\r\nPrivilege: Command\r\nActionID: 7\r\nrow one\nrow two\n--END COMMAND--\r\n\r\n"))
	msg, err := c.ReadMessage(context.Background())
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if got, want := msg.Values("Output"), []string{"row one", "row two"}; !equalStrings(got, want) {
		t.Fatalf("Values(Output) = %q, want %q", got, want)
	}
	if msg.Get("Response") != "Follows" || msg.Get("ActionID") != "7" {
		t.Fatalf("unexpected envelope: %v", msg)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestConnWriteActionWire(t *testing.T) {
	tests := []struct {
		name     string
		actionID string
		want     string
	}{
		{
			"with action id",
			"id-1",
			"Action: Originate\r\nActionID: id-1\r\nChannel: PJSIP/synthetic-0001\r\nVariable: a=1\r\nVariable: b=2\r\n\r\n",
		},
		{
			"empty action id omits the field",
			"",
			"Action: Originate\r\nChannel: PJSIP/synthetic-0001\r\nVariable: a=1\r\nVariable: b=2\r\n\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, server := newPipeConn(t, WireLimits{})
			act := mustAction(t, "Originate",
				Field{Key: "Channel", Value: "PJSIP/synthetic-0001"},
				Field{Key: "Variable", Value: "a=1"},
				Field{Key: "Variable", Value: "b=2"},
			)
			got := make([]byte, len(tt.want))
			readDone := make(chan error, 1)
			go func() {
				_, err := io.ReadFull(server, got)
				readDone <- err
			}()
			if err := c.WriteAction(context.Background(), act, tt.actionID); err != nil {
				t.Fatalf("WriteAction() error = %v", err)
			}
			if err := <-readDone; err != nil {
				t.Fatalf("reading the frame: %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("frame = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConnWriteActionValidationLeavesUsable(t *testing.T) {
	// "Action: Ping\r\n\r\n" is exactly 16 bytes, so the Ping without an
	// ActionID fits and anything more is rejected before I/O.
	c, server := newPipeConn(t, WireLimits{MaxActionBytes: 16})
	ping := mustAction(t, "Ping")

	var pe *ProtocolError
	if err := c.WriteAction(context.Background(), Action{}, ""); !errors.As(err, &pe) ||
		pe.Category != "envelope" || pe.Dimension != "empty action name" {
		t.Fatalf("zero action: err = %v, want envelope/empty action name", err)
	}
	if err := c.WriteAction(context.Background(), ping, "a\r\nb"); !errors.As(err, &pe) ||
		pe.Category != "envelope" || pe.Dimension != "action id" {
		t.Fatalf("bad action id: err = %v, want envelope/action id", err)
	}
	if err := c.WriteAction(context.Background(), ping, "0123456789"); !errors.As(err, &pe) ||
		pe.Category != "limit" || pe.Dimension != "MaxActionBytes" {
		t.Fatalf("oversized action: err = %v, want limit/MaxActionBytes", err)
	}

	// Every rejection above happened before I/O; the connection works.
	go io.Copy(io.Discard, server)
	if err := c.WriteAction(context.Background(), ping, ""); err != nil {
		t.Fatalf("WriteAction() after rejections: %v", err)
	}
}

func TestConnPreCanceledContext(t *testing.T) {
	c, server := newPipeConn(t, WireLimits{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.ReadMessage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadMessage(canceled) = %v, want context.Canceled", err)
	}
	ping := mustAction(t, "Ping")
	if err := c.WriteAction(ctx, ping, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteAction(canceled) = %v, want context.Canceled", err)
	}
	// No I/O happened; the connection is untouched and usable.
	go server.Write([]byte("Event: Alive\r\n\r\n"))
	msg, err := c.ReadMessage(context.Background())
	if err != nil || msg.Get("Event") != "Alive" {
		t.Fatalf("ReadMessage() after pre-canceled ops = (%v, %v)", msg, err)
	}
}

func TestConnReadCancelCleanLeavesUsable(t *testing.T) {
	client, server := net.Pipe()
	sc := newSignalConn(client)
	c, err := NewConn(sc, WireLimits{})
	if err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}
	t.Cleanup(func() {
		c.Close()
		server.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sc.readEntered
		cancel()
	}()
	if _, err := c.ReadMessage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadMessage() = %v, want context.Canceled", err)
	}
	// No frame byte was consumed, so the connection must remain usable
	// and the poked deadline must have been cleared.
	go server.Write([]byte("Event: Later\r\n\r\n"))
	msg, err := c.ReadMessage(context.Background())
	if err != nil || msg.Get("Event") != "Later" {
		t.Fatalf("ReadMessage() after clean cancel = (%v, %v)", msg, err)
	}
}

func TestConnReadCancelMidFrameCloses(t *testing.T) {
	c, server := newPipeConn(t, WireLimits{})
	ctx, cancel := context.WithCancel(context.Background())
	resCh := make(chan error, 1)
	go func() {
		_, err := c.ReadMessage(ctx)
		resCh <- err
	}()
	// A pipe write completes only when fully consumed, so after Write
	// returns the parser has provably consumed bytes of the open frame.
	if _, err := server.Write([]byte("Event: X\r\nPartial")); err != nil {
		t.Fatalf("priming the frame: %v", err)
	}
	cancel()
	err := <-resCh
	if errors.Is(err, context.Canceled) {
		t.Fatal("mid-frame interruption surfaced as a clean context error")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ReadMessage() = %v, want the transport deadline error", err)
	}
	if _, err := c.ReadMessage(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadMessage() after poisoning = %v, want ErrClosed", err)
	}
}

func TestConnWriteCancelZeroBytesLeavesUsable(t *testing.T) {
	client, server := net.Pipe()
	sc := newSignalConn(client)
	c, err := NewConn(sc, WireLimits{})
	if err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}
	t.Cleanup(func() {
		c.Close()
		server.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sc.writeEntered
		cancel()
	}()
	ping := mustAction(t, "Ping")
	// Nobody reads the server end, so the write cannot transfer a byte.
	if err := c.WriteAction(ctx, ping, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteAction() = %v, want context.Canceled", err)
	}
	go io.Copy(io.Discard, server)
	if err := c.WriteAction(context.Background(), ping, ""); err != nil {
		t.Fatalf("WriteAction() after zero-byte cancel = %v", err)
	}
}

func TestConnWriteCancelPartialCloses(t *testing.T) {
	c, server := newPipeConn(t, WireLimits{})
	ctx, cancel := context.WithCancel(context.Background())
	consumed := make(chan struct{})
	go func() {
		io.ReadFull(server, make([]byte, 3))
		close(consumed)
	}()
	go func() {
		<-consumed
		cancel()
	}()
	ping := mustAction(t, "Ping")
	err := c.WriteAction(ctx, ping, "12345")
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("partial write surfaced as a clean context error")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("WriteAction() = %v, want the transport deadline error", err)
	}
	if err := c.WriteAction(context.Background(), ping, ""); !errors.Is(err, ErrClosed) {
		t.Fatalf("WriteAction() after poisoning = %v, want ErrClosed", err)
	}
}

func TestConnPartialFrameAgeExpires(t *testing.T) {
	c, server := newPipeConn(t, WireLimits{MaxPartialFrameAge: time.Nanosecond})
	resCh := make(chan error, 1)
	go func() {
		_, err := c.ReadMessage(context.Background())
		resCh <- err
	}()
	// A pipe write completes only when fully consumed, so after Write
	// returns the parser has provably consumed the frame's first bytes
	// and armed the already-expired deadline; the frame never completes.
	if _, err := server.Write([]byte("Event: X\r\nPar")); err != nil {
		t.Fatalf("priming the frame: %v", err)
	}
	err := <-resCh
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Category != "limit" || pe.Dimension != "MaxPartialFrameAge" {
		t.Fatalf("ReadMessage() = %v, want limit/MaxPartialFrameAge", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("partial-frame expiry surfaced as a context error")
	}
	if _, err := c.ReadMessage(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadMessage() after expiry = %v, want ErrClosed", err)
	}
}

func TestConnPartialFrameAgeIdleUnaffected(t *testing.T) {
	client, server := net.Pipe()
	sc := newSignalConn(client)
	c, err := NewConn(sc, WireLimits{MaxPartialFrameAge: time.Nanosecond})
	if err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}
	t.Cleanup(func() {
		c.Close()
		server.Close()
	})
	// An idle wait holds no pending frame, so even the tightest possible
	// age must not run; the provably-in-flight read abandons cleanly.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sc.readEntered
		cancel()
	}()
	if _, err := c.ReadMessage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("idle ReadMessage() = %v, want context.Canceled", err)
	}
	// A frame delivered in one chunk parses from the buffer without a
	// further stream read, so even a 1ns age admits it.
	go server.Write([]byte("Event: Quick\r\n\r\n"))
	msg, err := c.ReadMessage(context.Background())
	if err != nil || msg.Get("Event") != "Quick" {
		t.Fatalf("ReadMessage() = (%v, %v)", msg, err)
	}
	// The success disarmed the long-expired deadline: the next read must
	// block on the idle stream instead of failing instantly.
	go server.Write([]byte("Event: Again\r\n\r\n"))
	msg, err = c.ReadMessage(context.Background())
	if err != nil || msg.Get("Event") != "Again" {
		t.Fatalf("ReadMessage() after disarm = (%v, %v)", msg, err)
	}
}

// deadlineRecorder records every SetReadDeadline value so tests can
// assert exactly who owns the read deadline.
type deadlineRecorder struct {
	net.Conn
	mu   sync.Mutex
	sets []time.Time
}

func (d *deadlineRecorder) SetReadDeadline(t time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sets = append(d.sets, t)
	return nil
}

func (d *deadlineRecorder) snapshot() []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.sets)
}

func TestConnFrameStartYieldsToCancelPoke(t *testing.T) {
	rec := &deadlineRecorder{}
	c := &Conn{conn: rec, age: time.Minute}
	c.frameStarted() // no poke in flight: arms the age deadline
	c.readPoke()     // the cancellation poke takes ownership
	c.frameStarted() // must not extend the poked deadline
	c.readClear()    // release re-enables arming
	c.frameStarted()
	got := rec.snapshot()
	if len(got) != 4 {
		t.Fatalf("SetReadDeadline calls = %d (%v), want 4: the poked frame start must not set a deadline", len(got), got)
	}
	future := time.Now().Add(30 * time.Second)
	if !got[0].After(future) {
		t.Fatalf("first arming = %v, want the age in the future", got[0])
	}
	if !got[1].Equal(aLongTimeAgo) {
		t.Fatalf("poke set %v, want the past instant", got[1])
	}
	if !got[2].IsZero() {
		t.Fatalf("clear set %v, want the zero time", got[2])
	}
	if !got[3].After(future) {
		t.Fatalf("re-arming = %v, want the age in the future", got[3])
	}
}

func TestConnInboundViolationCloses(t *testing.T) {
	c, server := newPipeConn(t, WireLimits{MaxLineBytes: 8})
	go server.Write([]byte("A: 123456789\r\n\r\n"))
	_, err := c.ReadMessage(context.Background())
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Category != "limit" || pe.Dimension != "MaxLineBytes" {
		t.Fatalf("ReadMessage() = %v, want limit/MaxLineBytes", err)
	}
	if _, err := c.ReadMessage(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadMessage() after violation = %v, want ErrClosed", err)
	}
}

func TestConnRemoteClose(t *testing.T) {
	t.Run("at message boundary", func(t *testing.T) {
		c, server := newPipeConn(t, WireLimits{})
		server.Close()
		if _, err := c.ReadMessage(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatalf("ReadMessage() = %v, want io.EOF", err)
		}
		if _, err := c.ReadMessage(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("ReadMessage() after EOF = %v, want ErrClosed", err)
		}
	})
	t.Run("inside a frame", func(t *testing.T) {
		c, server := newPipeConn(t, WireLimits{})
		go func() {
			server.Write([]byte("Event: X\r\n"))
			server.Close()
		}()
		if _, err := c.ReadMessage(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("ReadMessage() = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

func TestConnClose(t *testing.T) {
	client, server := net.Pipe()
	sc := newSignalConn(client)
	c, err := NewConn(sc, WireLimits{})
	if err != nil {
		t.Fatalf("NewConn() error = %v", err)
	}
	t.Cleanup(func() { server.Close() })
	go func() {
		<-sc.readEntered
		c.Close()
	}()
	if _, err := c.ReadMessage(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadMessage() interrupted by Close = %v, want ErrClosed", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
	ping := mustAction(t, "Ping")
	if err := c.WriteAction(context.Background(), ping, ""); !errors.Is(err, ErrClosed) {
		t.Fatalf("WriteAction() after Close = %v, want ErrClosed", err)
	}
}

func TestConnConcurrentReadWrite(t *testing.T) {
	c, server := newPipeConn(t, WireLimits{})
	const n = 25
	go func() {
		for i := range n {
			if _, err := server.Write(fmt.Appendf(nil, "Event: E%d\r\n\r\n", i)); err != nil {
				return
			}
		}
	}()
	go io.Copy(io.Discard, server)
	writeDone := make(chan error, 1)
	go func() {
		ping, err := NewAction("Ping")
		if err != nil {
			writeDone <- err
			return
		}
		for range n {
			if err := c.WriteAction(context.Background(), ping, "id"); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- nil
	}()
	for i := range n {
		msg, err := c.ReadMessage(context.Background())
		if err != nil {
			t.Fatalf("ReadMessage() %d error = %v", i, err)
		}
		if want := fmt.Sprintf("E%d", i); msg.Get("Event") != want {
			t.Fatalf("message %d = %q, want %q", i, msg.Get("Event"), want)
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("concurrent writer: %v", err)
	}
}

func TestMessageFromWire(t *testing.T) {
	if m := messageFromWire(nil); m.Get("any") != "" {
		t.Fatal("empty wire fields produced a non-empty message")
	}
	fields := []wire.Field{
		{Key: "Variable", Value: "a=1"},
		{Key: "variable", Value: "b=2"},
	}
	m := messageFromWire(fields)
	if got := m.Values("VARIABLE"); !equalStrings(got, []string{"a=1", "b=2"}) {
		t.Fatalf("Values() = %q, want wire order across case variants", got)
	}
	fields[0].Value = "mutated"
	if m.Get("Variable") != "a=1" {
		t.Fatal("mutating the wire slice changed the message")
	}
}
