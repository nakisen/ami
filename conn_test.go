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

func newPipeFramer(t *testing.T, limits WireLimits) (*framer, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	f, err := newFramer(client, limits)
	if err != nil {
		t.Fatalf("newFramer() error = %v", err)
	}
	t.Cleanup(func() {
		f.close()
		server.Close()
	})
	return f, server
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

// writeResultConn reports one caller-selected write result without
// changing its cause. It models transports whose error identity collides
// with the clean and pre-wire meanings on the framing layer's error
// surface.
type writeResultConn struct {
	net.Conn
	n   int
	err error
}

func (c *writeResultConn) Write(p []byte) (int, error) {
	return min(c.n, len(p)), c.err
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

func TestNewFramerValidation(t *testing.T) {
	if _, err := newFramer(nil, WireLimits{}); err == nil {
		t.Fatal("newFramer(nil) succeeded")
	}
	client, server := net.Pipe()
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	if _, err := newFramer(client, WireLimits{MaxLineBytes: -1}); err == nil || !strings.Contains(err.Error(), "MaxLineBytes") {
		t.Fatalf("newFramer with negative limit: err = %v, want error naming MaxLineBytes", err)
	}
	if _, err := newFramer(client, WireLimits{MaxPartialFrameAge: -time.Second}); err == nil || !strings.Contains(err.Error(), "MaxPartialFrameAge") {
		t.Fatalf("newFramer with negative age: err = %v, want error naming MaxPartialFrameAge", err)
	}
	// Constructor failure leaves ownership with the caller: the same
	// connection must still be fully usable.
	f, err := newFramer(client, WireLimits{})
	if err != nil {
		t.Fatalf("newFramer() after failed construction: %v", err)
	}
	go server.Write([]byte("Event: Reused\r\n\r\n"))
	msg, err := f.readMessage(context.Background())
	if err != nil || msg.Get("Event") != "Reused" {
		t.Fatalf("readMessage() = (%v, %v)", msg, err)
	}
}

func TestFramerReadBannerAndMessage(t *testing.T) {
	f, server := newPipeFramer(t, WireLimits{})
	go server.Write([]byte("Asterisk Call Manager/5.0.2\r\nEvent: FullyBooted\r\nUptime: 1\r\n\r\n"))
	banner, err := f.readBanner(context.Background())
	if err != nil || banner != "Asterisk Call Manager/5.0.2" {
		t.Fatalf("readBanner() = (%q, %v)", banner, err)
	}
	msg, err := f.readMessage(context.Background())
	if err != nil {
		t.Fatalf("readMessage() error = %v", err)
	}
	if msg.Get("Event") != "FullyBooted" || msg.Get("Uptime") != "1" {
		t.Fatalf("unexpected message: %v", msg)
	}
}

func TestFramerReadMessageLegacyCommand(t *testing.T) {
	f, server := newPipeFramer(t, WireLimits{})
	go server.Write([]byte("Response: Follows\r\nPrivilege: Command\r\nActionID: 7\r\nrow one\nrow two\n--END COMMAND--\r\n\r\n"))
	msg, err := f.readMessage(context.Background())
	if err != nil {
		t.Fatalf("readMessage() error = %v", err)
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

func TestFramerWriteActionWire(t *testing.T) {
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
			f, server := newPipeFramer(t, WireLimits{})
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
			if d, err := f.writeAction(context.Background(), act, tt.actionID); d != writeComplete || err != nil {
				t.Fatalf("writeAction() = (%v, %v), want writeComplete", d, err)
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

func TestFramerWriteActionValidationLeavesUsable(t *testing.T) {
	// "Action: Ping\r\n\r\n" is exactly 16 bytes, so the Ping without an
	// ActionID fits and anything more is rejected before I/O.
	f, server := newPipeFramer(t, WireLimits{MaxActionBytes: 16})
	ping := mustAction(t, "Ping")

	var pe *ProtocolError
	d, err := f.writeAction(context.Background(), Action{}, "")
	if d != writeRejected || !errors.As(err, &pe) || pe.Category != "envelope" || pe.Dimension != "empty action name" {
		t.Fatalf("zero action: (%v, %v), want writeRejected and envelope/empty action name", d, err)
	}
	d, err = f.writeAction(context.Background(), ping, "a\r\nb")
	if d != writeRejected || !errors.As(err, &pe) || pe.Category != "envelope" || pe.Dimension != "action id" {
		t.Fatalf("bad action id: (%v, %v), want writeRejected and envelope/action id", d, err)
	}
	d, err = f.writeAction(context.Background(), ping, "0123456789")
	if d != writeRejected || !errors.As(err, &pe) || pe.Category != "limit" || pe.Dimension != "MaxActionBytes" {
		t.Fatalf("oversized action: (%v, %v), want writeRejected and limit/MaxActionBytes", d, err)
	}

	// Every rejection above happened before I/O; the connection works.
	go io.Copy(io.Discard, server)
	if d, err := f.writeAction(context.Background(), ping, ""); d != writeComplete || err != nil {
		t.Fatalf("writeAction() after rejections: (%v, %v)", d, err)
	}
}

func TestFramerPreCanceledContext(t *testing.T) {
	f, server := newPipeFramer(t, WireLimits{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.readMessage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("readMessage(canceled) = %v, want context.Canceled", err)
	}
	ping := mustAction(t, "Ping")
	if d, err := f.writeAction(ctx, ping, ""); d != writeCanceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("writeAction(canceled) = (%v, %v), want writeCanceled and context.Canceled", d, err)
	}
	// No I/O happened; the connection is untouched and usable.
	go server.Write([]byte("Event: Alive\r\n\r\n"))
	msg, err := f.readMessage(context.Background())
	if err != nil || msg.Get("Event") != "Alive" {
		t.Fatalf("readMessage() after pre-canceled ops = (%v, %v)", msg, err)
	}
}

func TestFramerReadCancelCleanLeavesUsable(t *testing.T) {
	client, server := net.Pipe()
	sc := newSignalConn(client)
	f, err := newFramer(sc, WireLimits{})
	if err != nil {
		t.Fatalf("newFramer() error = %v", err)
	}
	t.Cleanup(func() {
		f.close()
		server.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sc.readEntered
		cancel()
	}()
	if _, err := f.readMessage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("readMessage() = %v, want context.Canceled", err)
	}
	// No frame byte was consumed, so the connection must remain usable
	// and the poked deadline must have been cleared.
	go server.Write([]byte("Event: Later\r\n\r\n"))
	msg, err := f.readMessage(context.Background())
	if err != nil || msg.Get("Event") != "Later" {
		t.Fatalf("readMessage() after clean cancel = (%v, %v)", msg, err)
	}
}

func TestFramerReadCancelMidFrameCloses(t *testing.T) {
	f, server := newPipeFramer(t, WireLimits{})
	ctx, cancel := context.WithCancel(context.Background())
	resCh := make(chan error, 1)
	go func() {
		_, err := f.readMessage(ctx)
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
		t.Fatalf("readMessage() = %v, want the transport deadline error", err)
	}
	if _, err := f.readMessage(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("readMessage() after poisoning = %v, want ErrClosed", err)
	}
}

func TestFramerWriteCancelZeroBytesLeavesUsable(t *testing.T) {
	client, server := net.Pipe()
	sc := newSignalConn(client)
	f, err := newFramer(sc, WireLimits{})
	if err != nil {
		t.Fatalf("newFramer() error = %v", err)
	}
	t.Cleanup(func() {
		f.close()
		server.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sc.writeEntered
		cancel()
	}()
	ping := mustAction(t, "Ping")
	// Nobody reads the server end, so the write cannot transfer a byte.
	if d, err := f.writeAction(ctx, ping, ""); d != writeCanceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("writeAction() = (%v, %v), want writeCanceled and context.Canceled", d, err)
	}
	go io.Copy(io.Discard, server)
	if d, err := f.writeAction(context.Background(), ping, ""); d != writeComplete || err != nil {
		t.Fatalf("writeAction() after zero-byte cancel = (%v, %v)", d, err)
	}
}

func TestFramerWriteCancelPartialCloses(t *testing.T) {
	f, server := newPipeFramer(t, WireLimits{})
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
	d, err := f.writeAction(ctx, ping, "12345")
	if d != writeOutcomeUnknown {
		t.Fatalf("writeAction() disposition = %v, want writeOutcomeUnknown", d)
	}
	// The interrupted write transferred bytes, so its outcome is unknown
	// even though the interruption came from a canceled context: the
	// disposition reports that, and the error stays the transport's own.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writeAction() error = %v, want the transport error, not a clean context error", err)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("writeAction() error = %v, want the transport deadline error", err)
	}
	if d, err := f.writeAction(context.Background(), ping, ""); d != writeClosed || !errors.Is(err, ErrClosed) {
		t.Fatalf("writeAction() after poisoning = (%v, %v), want writeClosed and ErrClosed", d, err)
	}
}

// TestFramerWriteDispositionIgnoresCauseIdentity pins the rule the session
// layer's outcome taxonomy rests on: only the byte disposition says
// whether an action may have reached the server. A transport may return
// any error value alongside any byte count, including one that matches a
// clean cancellation or a pre-wire validation rejection.
func TestFramerWriteDispositionIgnoresCauseIdentity(t *testing.T) {
	protocolCause := &ProtocolError{Category: "synthetic", Dimension: "transport"}
	tests := []struct {
		name  string
		n     int
		cause error
		want  writeDisposition
	}{
		{name: "zero-byte context-like cause", cause: context.Canceled, want: writeNotSent},
		{name: "zero-byte protocol-like cause", cause: protocolCause, want: writeNotSent},
		{name: "partial context-like cause", n: 1, cause: context.Canceled, want: writeOutcomeUnknown},
		{name: "partial protocol-like cause", n: 1, cause: protocolCause, want: writeOutcomeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			transport := &writeResultConn{Conn: client, n: tt.n, err: tt.cause}
			f, err := newFramer(transport, WireLimits{})
			if err != nil {
				t.Fatalf("newFramer() error = %v", err)
			}
			t.Cleanup(func() {
				f.close()
				server.Close()
			})

			ping := mustAction(t, "Ping")
			d, err := f.writeAction(context.Background(), ping, "synthetic-id")
			if d != tt.want {
				t.Fatalf("writeAction() disposition = %v, want %v", d, tt.want)
			}
			if err != tt.cause {
				t.Fatalf("writeAction() error = %v, want the original cause %v", err, tt.cause)
			}
			// A failed transport write closes the connection, and a
			// connection found already closed reports that instead of a
			// byte-transfer disposition.
			d, closedErr := f.writeAction(context.Background(), ping, "")
			if d != writeClosed || !errors.Is(closedErr, ErrClosed) {
				t.Fatalf("writeAction() after transport failure = (%v, %v), want writeClosed and ErrClosed", d, closedErr)
			}
		})
	}
}

func TestFramerPartialFrameAgeExpires(t *testing.T) {
	f, server := newPipeFramer(t, WireLimits{MaxPartialFrameAge: time.Nanosecond})
	resCh := make(chan error, 1)
	go func() {
		_, err := f.readMessage(context.Background())
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
		t.Fatalf("readMessage() = %v, want limit/MaxPartialFrameAge", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("partial-frame expiry surfaced as a context error")
	}
	if _, err := f.readMessage(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("readMessage() after expiry = %v, want ErrClosed", err)
	}
}

// TestFramerPartialFrameAgeArmsAtFirstByte pins the first-byte contract
// end to end: a single byte of a never-completing first line — the
// banner included — must start the frame clock, so the read fails on
// the age instead of hanging forever on a stalled peer.
func TestFramerPartialFrameAgeArmsAtFirstByte(t *testing.T) {
	tests := []struct {
		name string
		read func(*framer) error
	}{
		{"banner", func(f *framer) error {
			_, err := f.readBanner(context.Background())
			return err
		}},
		{"message", func(f *framer) error {
			_, err := f.readMessage(context.Background())
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, server := newPipeFramer(t, WireLimits{MaxPartialFrameAge: 50 * time.Millisecond})
			resCh := make(chan error, 1)
			go func() { resCh <- tt.read(f) }()
			if _, err := server.Write([]byte("A")); err != nil {
				t.Fatalf("priming the first byte: %v", err)
			}
			err := <-resCh
			var pe *ProtocolError
			if !errors.As(err, &pe) || pe.Category != "limit" || pe.Dimension != "MaxPartialFrameAge" {
				t.Fatalf("read = %v, want limit/MaxPartialFrameAge", err)
			}
		})
	}
}

func TestFramerPartialFrameAgeIdleUnaffected(t *testing.T) {
	client, server := net.Pipe()
	sc := newSignalConn(client)
	f, err := newFramer(sc, WireLimits{MaxPartialFrameAge: time.Nanosecond})
	if err != nil {
		t.Fatalf("newFramer() error = %v", err)
	}
	t.Cleanup(func() {
		f.close()
		server.Close()
	})
	// An idle wait holds no pending frame, so even the tightest possible
	// age must not run; the provably-in-flight read abandons cleanly.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sc.readEntered
		cancel()
	}()
	if _, err := f.readMessage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("idle readMessage() = %v, want context.Canceled", err)
	}
	// A frame delivered in one chunk parses from the buffer without a
	// further stream read, so even a 1ns age admits it.
	go server.Write([]byte("Event: Quick\r\n\r\n"))
	msg, err := f.readMessage(context.Background())
	if err != nil || msg.Get("Event") != "Quick" {
		t.Fatalf("readMessage() = (%v, %v)", msg, err)
	}
	// The success disarmed the long-expired deadline: the next read must
	// block on the idle stream instead of failing instantly.
	go server.Write([]byte("Event: Again\r\n\r\n"))
	msg, err = f.readMessage(context.Background())
	if err != nil || msg.Get("Event") != "Again" {
		t.Fatalf("readMessage() after disarm = (%v, %v)", msg, err)
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

func TestFramerFrameStartYieldsToCancelPoke(t *testing.T) {
	rec := &deadlineRecorder{}
	f := &framer{conn: rec, age: time.Minute}
	f.frameStarted() // no poke in flight: arms the age deadline
	f.readPoke()     // the cancellation poke takes ownership
	f.frameStarted() // must not extend the poked deadline
	f.readClear()    // release re-enables arming
	f.frameStarted()
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

func TestFramerInboundViolationCloses(t *testing.T) {
	f, server := newPipeFramer(t, WireLimits{MaxLineBytes: 8})
	go server.Write([]byte("A: 123456789\r\n\r\n"))
	_, err := f.readMessage(context.Background())
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Category != "limit" || pe.Dimension != "MaxLineBytes" {
		t.Fatalf("readMessage() = %v, want limit/MaxLineBytes", err)
	}
	if _, err := f.readMessage(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("readMessage() after violation = %v, want ErrClosed", err)
	}
}

func TestFramerRemoteClose(t *testing.T) {
	t.Run("at message boundary", func(t *testing.T) {
		f, server := newPipeFramer(t, WireLimits{})
		server.Close()
		if _, err := f.readMessage(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatalf("readMessage() = %v, want io.EOF", err)
		}
		if _, err := f.readMessage(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("readMessage() after EOF = %v, want ErrClosed", err)
		}
	})
	t.Run("inside a frame", func(t *testing.T) {
		f, server := newPipeFramer(t, WireLimits{})
		go func() {
			server.Write([]byte("Event: X\r\n"))
			server.Close()
		}()
		if _, err := f.readMessage(context.Background()); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("readMessage() = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

func TestFramerClose(t *testing.T) {
	client, server := net.Pipe()
	sc := newSignalConn(client)
	f, err := newFramer(sc, WireLimits{})
	if err != nil {
		t.Fatalf("newFramer() error = %v", err)
	}
	t.Cleanup(func() { server.Close() })
	go func() {
		<-sc.readEntered
		f.close()
	}()
	if _, err := f.readMessage(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("readMessage() interrupted by close = %v, want ErrClosed", err)
	}
	if err := f.close(); err != nil {
		t.Fatalf("second close() = %v, want nil", err)
	}
	ping := mustAction(t, "Ping")
	if d, err := f.writeAction(context.Background(), ping, ""); d != writeClosed || !errors.Is(err, ErrClosed) {
		t.Fatalf("writeAction() after close = (%v, %v), want writeClosed and ErrClosed", d, err)
	}
}

func TestFramerConcurrentReadWrite(t *testing.T) {
	f, server := newPipeFramer(t, WireLimits{})
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
			if _, err := f.writeAction(context.Background(), ping, "id"); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- nil
	}()
	for i := range n {
		msg, err := f.readMessage(context.Background())
		if err != nil {
			t.Fatalf("readMessage() %d error = %v", i, err)
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
