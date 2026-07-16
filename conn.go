package ami

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nakisen/ami/internal/wire"
)

// aLongTimeAgo is a non-zero past instant used to interrupt blocked
// connection I/O by poking a deadline.
var aLongTimeAgo = time.Unix(1, 0)

// A Conn is the low-level AMI framing layer over one established network
// connection: banner read, message read, and action write, each bounded
// by a context and by the connection's WireLimits.
//
// Conn is synchronous and single-owner: at most one goroutine may call
// read methods and at most one goroutine may call WriteAction at any
// time. Close may be called concurrently with both. Conn starts no
// background goroutines and performs no login, correlation,
// subscription, or keepalive work — that is the session layer's job.
//
// # Error contract
//
// A method returns ctx.Err() — an error matching context.Canceled or
// context.DeadlineExceeded — only when the operation was abandoned
// cleanly: no byte of the pending inbound frame had been consumed, or no
// action byte had been written. The connection remains usable.
//
// Any other error means the connection has been closed. Transport errors
// are returned verbatim; inbound protocol and limit violations are
// reported as *ProtocolError; a clean remote close surfaces as io.EOF at
// a message boundary and as io.ErrUnexpectedEOF inside a frame; and a
// cancellation that interrupted a partially transferred frame surfaces
// as the transport's deadline error, deliberately not as a context
// error, because the frame — and therefore the connection — is
// unrecoverable.
//
// A frame that stays incomplete past WireLimits.MaxPartialFrameAge is an
// inbound violation: the read fails with a *ProtocolError and the
// connection closes. The age clock starts when the frame's first byte is
// consumed and stops when the frame completes, so an idle connection
// with no pending frame never trips it.
//
// The one exception is outbound validation: a *ProtocolError from
// WriteAction is reported before any byte is written and leaves the
// connection usable.
//
// Operations on a closed connection return ErrClosed.
type Conn struct {
	conn net.Conn
	r    *wire.Reader
	lim  wire.Limits
	age  time.Duration // partial-frame age, armed at each frame's first byte

	wbuf []byte // encode buffer, reused by the single writer

	mu      sync.Mutex
	closed  bool
	rdPoked bool // a cancellation poke owns the read deadline
}

// NewConn wraps an established network connection — plain TCP, TLS, or
// any other net.Conn — in the AMI framing layer. A successful NewConn
// takes ownership of conn: the caller must no longer use or close it
// directly. On error, NewConn has performed no I/O and ownership stays
// with the caller.
func NewConn(conn net.Conn, limits WireLimits) (*Conn, error) {
	lim, age, err := limits.resolve()
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("ami: NewConn: nil connection")
	}
	c := &Conn{conn: conn, r: wire.NewReader(conn, lim), lim: lim, age: age}
	c.r.SetFrameStartHook(c.frameStarted)
	return c, nil
}

// ReadBanner reads the protocol banner line the server sends before its
// first message. The banner is diagnostic data: the library derives no
// behavior from it.
func (c *Conn) ReadBanner(ctx context.Context) (string, error) {
	var banner string
	err := c.read(ctx, func() error {
		var err error
		banner, err = c.r.ReadBanner()
		return err
	})
	if err != nil {
		return "", err
	}
	return banner, nil
}

// ReadMessage reads one complete AMI message. Fields arrive in wire
// order with duplicate keys preserved; both Command output framings are
// handled by the parser and presented uniformly through Output fields.
func (c *Conn) ReadMessage(ctx context.Context) (Message, error) {
	var fields []wire.Field
	err := c.read(ctx, func() error {
		var err error
		fields, err = c.r.ReadMessage()
		return err
	})
	if err != nil {
		return Message{}, err
	}
	return messageFromWire(fields), nil
}

// read runs one wire read under context interruption and classifies the
// outcome according to the connection error contract.
func (c *Conn) read(ctx context.Context, op func() error) error {
	if err := c.enter(ctx); err != nil {
		return err
	}
	release := c.interrupt(ctx, c.readPoke, c.readClear)
	err := op()
	interrupted := release()
	if err == nil {
		// Every successful read consumed a frame, so a partial-frame
		// deadline is armed; disarm it before the next idle wait.
		c.readClear()
		return nil
	}
	if c.isClosed() {
		return ErrClosed
	}
	if interrupted && !c.r.Dirty() && errors.Is(err, os.ErrDeadlineExceeded) {
		return ctx.Err()
	}
	c.poison()
	if !interrupted && c.r.Dirty() && errors.Is(err, os.ErrDeadlineExceeded) {
		// The only uninterrupted deadline that can expire mid-frame is
		// the armed partial-frame age.
		return &ProtocolError{Category: "limit", Dimension: "MaxPartialFrameAge"}
	}
	return wireError(err)
}

// WriteAction encodes and writes one action frame: an Action field
// carrying the action name, an ActionID field when actionID is
// non-empty, then the action's extra fields in order. An empty actionID
// omits the ActionID field entirely; this low-level escape hatch lets an
// advanced session layer own its correlation scheme.
//
// Validation and encoding complete before any byte is written, so a
// *ProtocolError leaves the connection usable, as does a cancellation
// with zero bytes written. Once any byte may have been written, an error
// closes the connection and the action's outcome is unknown.
func (c *Conn) WriteAction(ctx context.Context, action Action, actionID string) error {
	if err := c.enter(ctx); err != nil {
		return err
	}
	if action.name == "" {
		return &ProtocolError{Category: "envelope", Dimension: "empty action name"}
	}
	if strings.ContainsAny(actionID, "\r\n") {
		return &ProtocolError{Category: "envelope", Dimension: "action id"}
	}
	fields := make([]wire.Field, 0, len(action.fields)+2)
	fields = append(fields, wire.Field{Key: "Action", Value: action.name})
	if actionID != "" {
		fields = append(fields, wire.Field{Key: "ActionID", Value: actionID})
	}
	for _, f := range action.fields {
		fields = append(fields, wire.Field(f))
	}
	buf, err := wire.AppendMessage(c.wbuf[:0], fields, c.lim)
	if err != nil {
		return wireError(err)
	}
	c.wbuf = buf

	release := c.interrupt(ctx, c.writePoke, c.writeClear)
	n, err := c.conn.Write(buf)
	interrupted := release()
	if err == nil {
		return nil
	}
	if c.isClosed() {
		return ErrClosed
	}
	if interrupted && n == 0 && errors.Is(err, os.ErrDeadlineExceeded) {
		return ctx.Err()
	}
	c.poison()
	return err
}

// Close closes the connection. It is idempotent, immediate, and safe to
// call concurrently with pending operations, which fail with ErrClosed.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// enter performs the common pre-I/O checks. A context already done
// before any I/O leaves the connection usable.
func (c *Conn) enter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.isClosed() {
		return ErrClosed
	}
	return nil
}

func (c *Conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// poison closes the connection after a terminal framing incident.
func (c *Conn) poison() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.conn.Close()
	}
}

// interrupt arms a watcher that pokes a past deadline into the
// connection when ctx is canceled, unblocking the pending operation. The
// returned release function reports whether the watcher fired; before
// reporting true it waits for the poke to finish and runs clear, so an
// operation that completed despite the cancellation leaves the
// connection usable.
func (c *Conn) interrupt(ctx context.Context, poke, clear func()) (release func() bool) {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		poke()
	})
	return func() bool {
		if stop() {
			return false
		}
		<-done
		clear()
		return true
	}
}

// readPoke interrupts a blocked read by poking a past read deadline. The
// flag it takes under the lock stops a later frame-start arming from
// overwriting the poke and stalling the cancellation.
func (c *Conn) readPoke() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdPoked = true
	c.conn.SetReadDeadline(aLongTimeAgo)
}

// readClear clears the poked or armed read deadline and re-enables
// frame-start arming.
func (c *Conn) readClear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdPoked = false
	c.conn.SetReadDeadline(time.Time{})
}

// frameStarted arms the partial-frame deadline as the reader consumes
// the first byte of a new frame. A cancellation poke in flight wins: the
// poked deadline must not be extended.
func (c *Conn) frameStarted() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rdPoked || c.closed {
		return
	}
	c.conn.SetReadDeadline(time.Now().Add(c.age))
}

// writePoke and writeClear interrupt and restore the write deadline;
// nothing else touches it, so the write side needs no flag.
func (c *Conn) writePoke()  { c.conn.SetWriteDeadline(aLongTimeAgo) }
func (c *Conn) writeClear() { c.conn.SetWriteDeadline(time.Time{}) }

// wireError maps internal wire errors onto the public error surface;
// every other error passes through verbatim.
func wireError(err error) error {
	for _, m := range []struct {
		is        error
		category  string
		dimension string
	}{
		{wire.ErrBannerTooLong, "limit", "MaxBannerBytes"},
		{wire.ErrLineTooLong, "limit", "MaxLineBytes"},
		{wire.ErrTooManyFields, "limit", "MaxFields"},
		{wire.ErrMessageTooLarge, "limit", "MaxMessageBytes"},
		{wire.ErrTooManyOutputLines, "limit", "MaxCommandOutputLines"},
		{wire.ErrOutputTooLarge, "limit", "MaxCommandOutputBytes"},
		{wire.ErrTooManyActionFields, "limit", "MaxActionFields"},
		{wire.ErrActionLineTooLong, "limit", "MaxActionLineBytes"},
		{wire.ErrActionTooLarge, "limit", "MaxActionBytes"},
		{wire.ErrMalformedLine, "framing", "malformed line"},
		{wire.ErrEmptyMessage, "framing", "empty message"},
		{wire.ErrCommandFraming, "framing", "command output framing"},
		{wire.ErrInvalidKey, "envelope", "field key"},
		{wire.ErrInvalidValue, "envelope", "field value"},
	} {
		if errors.Is(err, m.is) {
			return &ProtocolError{Category: m.category, Dimension: m.dimension, cause: err}
		}
	}
	return err
}
