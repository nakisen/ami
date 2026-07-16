package ami

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
)

// diagnostics is the bounded internal queue between the library and an
// optional caller-supplied slog handler. Emission is non-blocking:
// overflow drops the diagnostic and counts the drop, because dropping
// diagnostics is acceptable — unlike events. The handler runs only on
// the queue's own worker goroutine, never on the read loop, and
// receives explicitly allowlisted metadata: names, counts, durations,
// and reason codes — never message contents, field values,
// credentials, or endpoints.
type diagnostics struct {
	logger *slog.Logger
	ch     chan diagEntry
	drops  atomic.Uint64
}

type diagEntry struct {
	msg  string
	args []any
}

// newDiagnostics returns nil for a nil logger: the silent default
// costs nothing.
func newDiagnostics(logger *slog.Logger) *diagnostics {
	if logger == nil {
		return nil
	}
	return &diagnostics{logger: logger, ch: make(chan diagEntry, 64)}
}

// info enqueues one diagnostic; safe on the nil (silent) instance.
func (d *diagnostics) info(msg string, args ...any) {
	if d == nil {
		return
	}
	select {
	case d.ch <- diagEntry{msg: msg, args: args}:
	default:
		d.drops.Add(1)
	}
}

// run drains the queue until the client context ends, then flushes
// what is buffered and reports the drop count once.
func (d *diagnostics) run(ctx context.Context) {
	for {
		select {
		case e := <-d.ch:
			d.logger.Info(e.msg, e.args...)
		case <-ctx.Done():
			for {
				select {
				case e := <-d.ch:
					d.logger.Info(e.msg, e.args...)
				default:
					if n := d.drops.Load(); n > 0 {
						d.logger.Info("diagnostics dropped", "count", n)
					}
					return
				}
			}
		}
	}
}

// diagErrClass reduces a terminal cause to a stable, allowlisted class
// for diagnostics. This package's own error types have sanitized Error
// text by construction; everything else — transport, TLS, OS — may
// carry endpoints and is reported only by class.
func diagErrClass(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ErrClosed):
		return "closed"
	case errors.Is(err, ErrPingTimeout), errors.Is(err, ErrPingWriteTimeout):
		return err.Error()
	case errors.Is(err, ErrRetirementExpired):
		return "retirement expired"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection ended"
	}
	if pe, ok := errors.AsType[*ProtocolError](err); ok {
		return pe.Error()
	}
	if ke, ok := errors.AsType[*KeepaliveError](err); ok {
		return ke.Error()
	}
	return "transport error"
}
