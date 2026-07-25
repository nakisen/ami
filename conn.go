package ami

import (
	"context"
	"errors"
	"io"
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

// A framer is the AMI framing layer over one established network
// connection: banner read, message read, and action write, each bounded
// by a context and by the connection's WireLimits. It is internal to the
// package; Client owns the only instance a consumer can reach.
//
// A framer is synchronous and single-owner: at most one goroutine may
// call read methods and at most one goroutine may call writeAction at any
// time. close may be called concurrently with both. A framer starts no
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
// Any other error means the connection has been closed. Inbound transport
// errors are returned verbatim; inbound protocol and limit violations are
// reported as *ProtocolError; and a clean remote close surfaces as io.EOF
// at a message boundary and as io.ErrUnexpectedEOF inside a frame.
//
// writeAction returns a writeDisposition alongside its error, and the
// disposition alone establishes whether bytes reached the transport: a
// transport may return a context-like or *ProtocolError value after
// transferring data, so the session layer classifies write outcomes from
// the disposition and never from the returned error's identity.
//
// A frame that stays incomplete past WireLimits.MaxPartialFrameAge is an
// inbound violation: the read fails with a *ProtocolError and the
// connection closes. The age clock starts when the frame's first byte is
// consumed and stops when the frame completes, so an idle connection
// with no pending frame never trips it.
//
// The one exception is outbound validation: a *ProtocolError from
// writeAction is reported before any byte is written and leaves the
// connection usable.
//
// Operations on a closed connection return ErrClosed.
type framer struct {
	conn net.Conn
	r    *wire.Reader
	lim  wire.Limits
	age  time.Duration // partial-frame age, armed at each frame's first byte

	wbuf []byte // encode buffer, reused by the single writer

	mu      sync.Mutex
	closed  bool
	rdPoked bool // a cancellation poke owns the read deadline
}

// newFramer wraps an established network connection — plain TCP, TLS, or
// any other net.Conn — in the AMI framing layer. A successful newFramer
// takes ownership of conn: the caller must no longer use or close it
// directly. On error, newFramer has performed no I/O and ownership stays
// with the caller.
func newFramer(conn net.Conn, limits WireLimits) (*framer, error) {
	lim, age, err := limits.resolve()
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("ami: nil connection")
	}
	f := &framer{conn: conn, r: wire.NewReader(conn, lim), lim: lim, age: age}
	f.r.SetFrameStartHook(f.frameStarted)
	return f, nil
}

// readBanner reads the protocol banner line the server sends before its
// first message. The banner is diagnostic data: the library derives no
// behavior from it.
func (f *framer) readBanner(ctx context.Context) (string, error) {
	var banner string
	err := f.read(ctx, func() error {
		var err error
		banner, err = f.r.ReadBanner()
		return err
	})
	if err != nil {
		return "", err
	}
	return banner, nil
}

// readMessage reads one complete AMI message. Fields arrive in wire
// order with duplicate keys preserved; both Command output framings are
// handled by the parser and presented uniformly through Output fields.
func (f *framer) readMessage(ctx context.Context) (Message, error) {
	var fields []wire.Field
	err := f.read(ctx, func() error {
		var err error
		fields, err = f.r.ReadMessage()
		return err
	})
	if err != nil {
		return Message{}, err
	}
	return messageFromWire(fields), nil
}

// read runs one wire read under context interruption and classifies the
// outcome according to the connection error contract.
func (f *framer) read(ctx context.Context, op func() error) error {
	if err := f.enter(ctx); err != nil {
		return err
	}
	release := f.interrupt(ctx, f.readPoke, f.readClear)
	err := op()
	interrupted := release()
	if err == nil {
		// Every successful read consumed a frame, so a partial-frame
		// deadline is armed; disarm it before the next idle wait.
		f.readClear()
		return nil
	}
	if f.isClosed() {
		return ErrClosed
	}
	if interrupted && !f.r.Dirty() && errors.Is(err, os.ErrDeadlineExceeded) {
		return ctx.Err()
	}
	f.poison()
	if !interrupted && f.r.Dirty() && errors.Is(err, os.ErrDeadlineExceeded) {
		// The only uninterrupted deadline that can expire mid-frame is
		// the armed partial-frame age.
		return &ProtocolError{Category: "limit", Dimension: "MaxPartialFrameAge"}
	}
	return wireError(err)
}

// writeDisposition tells the session why writeAction returned. It is
// deliberately independent of the error chain: a transport is allowed
// to return any error value, including one that happens to match a
// context or protocol error after transferring bytes.
type writeDisposition uint8

const (
	writeComplete       writeDisposition = 1 + iota
	writeRejected                        // local validation, connection usable
	writeCanceled                        // clean zero-byte cancellation, connection usable
	writeClosed                          // connection was already closed, no transport failure
	writeNotSent                         // zero-byte transport failure, connection closed
	writeOutcomeUnknown                  // one or more bytes transferred, connection closed
)

// writeAction encodes and writes one action frame: an Action field
// carrying the action name, an ActionID field when actionID is
// non-empty, then the action's extra fields in order. An empty actionID
// omits the ActionID field entirely, which the session layer never does;
// the framing layer leaves the correlation scheme to its owner.
//
// Validation and encoding complete before any byte is written, so a
// writeRejected disposition leaves the connection usable, as does the
// clean zero-byte writeCanceled. The remaining dispositions report
// whether any byte may have reached the transport, which the returned
// error's identity never establishes on its own.
func (f *framer) writeAction(ctx context.Context, action Action, actionID string) (writeDisposition, error) {
	if err := f.enter(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return writeCanceled, err
		}
		return writeClosed, err
	}
	if action.name == "" {
		return writeRejected, &ProtocolError{Category: "envelope", Dimension: "empty action name"}
	}
	if strings.ContainsAny(actionID, "\x00\r\n") {
		return writeRejected, &ProtocolError{Category: "envelope", Dimension: "action id"}
	}
	fields := make([]wire.Field, 0, len(action.fields)+2)
	fields = append(fields, wire.Field{Key: "Action", Value: action.name})
	if actionID != "" {
		fields = append(fields, wire.Field{Key: "ActionID", Value: actionID})
	}
	for _, fl := range action.fields {
		fields = append(fields, wire.Field(fl))
	}
	buf, err := wire.AppendMessage(f.wbuf[:0], fields, f.lim)
	if err != nil {
		return writeRejected, wireError(err)
	}
	f.wbuf = buf

	release := f.interrupt(ctx, f.writePoke, f.writeClear)
	n, err := f.conn.Write(buf)
	interrupted := release()
	if err == nil && n == len(buf) {
		return writeComplete, nil
	}
	if err == nil {
		err = io.ErrShortWrite
	}
	if f.isClosed() {
		if n > 0 {
			return writeOutcomeUnknown, ErrClosed
		}
		return writeClosed, ErrClosed
	}
	if interrupted && n == 0 && errors.Is(err, os.ErrDeadlineExceeded) {
		return writeCanceled, ctx.Err()
	}
	f.poison()
	if n > 0 {
		return writeOutcomeUnknown, err
	}
	return writeNotSent, err
}

// clearWriteBuffer zeroes the reused encode buffer's full capacity. The
// session layer calls it after the login exchange so a credential-
// bearing frame does not outlive its write in long-lived memory. The
// caller must hold write ownership, like writeAction itself.
func (f *framer) clearWriteBuffer() {
	clear(f.wbuf[:cap(f.wbuf)])
}

// close closes the connection. It is idempotent, immediate, and safe to
// call concurrently with pending operations, which fail with ErrClosed.
func (f *framer) close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	return f.conn.Close()
}

// enter performs the common pre-I/O checks. A context already done
// before any I/O leaves the connection usable.
func (f *framer) enter(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.isClosed() {
		return ErrClosed
	}
	return nil
}

func (f *framer) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// poison closes the connection after a terminal framing incident.
func (f *framer) poison() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		f.conn.Close()
	}
}

// interrupt arms a watcher that pokes a past deadline into the
// connection when ctx is canceled, unblocking the pending operation. The
// returned release function reports whether the watcher fired; before
// reporting true it waits for the poke to finish and runs clear, so an
// operation that completed despite the cancellation leaves the
// connection usable.
func (f *framer) interrupt(ctx context.Context, poke, clear func()) (release func() bool) {
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
func (f *framer) readPoke() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rdPoked = true
	f.conn.SetReadDeadline(aLongTimeAgo)
}

// readClear clears the poked or armed read deadline and re-enables
// frame-start arming.
func (f *framer) readClear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rdPoked = false
	f.conn.SetReadDeadline(time.Time{})
}

// frameStarted arms the partial-frame deadline as the reader consumes
// the first byte of a new frame. A cancellation poke in flight wins: the
// poked deadline must not be extended.
func (f *framer) frameStarted() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rdPoked || f.closed {
		return
	}
	f.conn.SetReadDeadline(time.Now().Add(f.age))
}

// writePoke and writeClear interrupt and restore the write deadline;
// nothing else touches it, so the write side needs no flag.
func (f *framer) writePoke()  { f.conn.SetWriteDeadline(aLongTimeAgo) }
func (f *framer) writeClear() { f.conn.SetWriteDeadline(time.Time{}) }

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
