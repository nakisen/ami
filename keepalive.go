package ami

import (
	"context"
	"errors"
	"time"

	"github.com/nakisen/ami/internal/demux"
)

// keepaliveLoop is the application-level Ping worker. The first Ping
// becomes due one interval after readiness; a valid matching response
// schedules the next full interval, ticks never accumulate, ordinary
// traffic does not reset the schedule, and a Ping is never overlapped
// with another.
func (c *Client) keepaliveLoop() {
	timer := time.NewTimer(c.ka.Interval)
	defer timer.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
		}
		if err := c.ping(); err != nil {
			c.diag.info("keepalive terminated the client", "cause", diagErrClass(err))
			c.die(err)
			return
		}
		timer.Reset(c.ka.Interval)
	}
}

// ping runs one Ping exchange through the reserved internal slot and
// the normal correlation machinery. A non-nil error is the client
// death cause; nil either means a valid response arrived or the client
// is already terminating.
func (c *Client) ping() error {
	action, err := NewAction("Ping")
	if err != nil {
		return err
	}
	id := c.newActionID(demux.KindRequest)

	// Admission and the complete write share the write-attempt bound:
	// once due, a Ping that cannot be on the wire in time is the
	// failure this worker exists to detect.
	wctx, cancel := context.WithTimeout(c.ctx, c.ka.WriteTimeout)
	defer cancel()
	select {
	case c.writeSem <- struct{}{}:
	case <-wctx.Done():
		if c.ctx.Err() != nil {
			return nil
		}
		return &KeepaliveError{Phase: "write", cause: ErrPingWriteTimeout}
	}

	c.mu.Lock()
	if c.terminated {
		c.mu.Unlock()
		c.releaseWriter()
		return nil
	}
	tkt, err := c.machine.Admit(id, demux.KindRequest, demux.AdmitOptions[Message]{Internal: true})
	if err != nil {
		c.mu.Unlock()
		c.releaseWriter()
		if errors.Is(err, demux.ErrDead) {
			return nil
		}
		// The serial worker never overlaps Pings, so the internal slot
		// is free by construction; anything else is a session bug
		// surfaced as a keepalive failure rather than ignored.
		return &KeepaliveError{Phase: "write", cause: ErrPingWriteTimeout}
	}
	w := make(chan demux.Completion[Message], 1)
	c.waiters[tkt] = w
	c.mu.Unlock()

	n, err := c.conn.writeAction(wctx, action, id)
	c.releaseWriter()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if !c.resolveNotSentLocked(tkt, w) {
				// A death completion won the race; the client is
				// already terminating with its own cause.
				return nil
			}
			if c.ctx.Err() != nil {
				return nil
			}
			return &KeepaliveError{Phase: "write", cause: ErrPingWriteTimeout}
		}
		// The connection is closed: the transport cause wins over a
		// keepalive classification.
		if n == 0 {
			c.resolveNotSentLocked(tkt, w)
			return err
		}
		c.mu.Lock()
		delete(c.waiters, tkt)
		c.resolveDeadLocked(func() { c.machine.CommitWrite(tkt) })
		c.mu.Unlock()
		return err
	}
	c.mu.Lock()
	c.machine.CommitWrite(tkt)
	c.mu.Unlock()

	// A fully written Ping must receive its valid response in time.
	respTimer := time.NewTimer(c.ka.Timeout)
	defer respTimer.Stop()
	select {
	case cpl := <-w:
		if !cpl.Delivered {
			return nil // client death owns the cause
		}
		if !responseSuccess(cpl.Response) {
			return &KeepaliveError{Phase: "rejected"}
		}
		return nil
	case <-respTimer.C:
	}

	// The timeout and a late response race through the session lock
	// with one linearized winner.
	c.mu.Lock()
	select {
	case cpl := <-w:
		c.mu.Unlock()
		if !cpl.Delivered {
			return nil
		}
		if !responseSuccess(cpl.Response) {
			return &KeepaliveError{Phase: "rejected"}
		}
		return nil
	default:
	}
	delete(c.waiters, tkt)
	if !c.terminated {
		c.machine.Abandon(tkt, c.now())
		c.mu.Unlock()
		c.pokeExpiry()
	} else {
		c.mu.Unlock()
		return nil
	}
	return &KeepaliveError{Phase: "response", cause: ErrPingTimeout}
}
