package ami

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nakisen/ami/internal/demux"
)

// A Client is one authenticated AMI session: correlation, explicit
// subscriptions, list state, keepalive, and terminal lifecycle over one
// connection. Construct it with Dial. All methods are safe for
// concurrent use.
//
// The client is generation-scoped: it never reconnects. When Done
// closes, Err returns the stable root cause; the application creates a
// new client under its own backoff policy and starts a fresh
// snapshot/reconciliation generation.
type Client struct {
	conn   *Conn
	banner string

	sess  sessionLimits
	ka    KeepaliveConfig
	diag  *diagnostics
	epoch time.Time // base of the session's monotonic logical clock

	idPrefix string // random per-session ActionID prefix, separator included
	seq      atomic.Uint64

	ctx    context.Context
	cancel context.CancelCauseFunc
	done   chan struct{}
	wg     sync.WaitGroup

	// inflight counts dispatches between admission and the end of their
	// machine bookkeeping — write resolution, adoption or release. Done
	// closes only after it drains, so no correlation state changes after
	// Done; holds are taken under the session lock atomically with the
	// liveness check, so none can start after termination.
	inflight sync.WaitGroup

	// writeSem serializes action writes. Its wait queue is FIFO, so
	// once a due keepalive Ping is waiting, later public writes cannot
	// pass it.
	writeSem chan struct{}

	// retirePoke nudges the expiry loop after any call that may have
	// created a record or advanced the earliest deadline.
	retirePoke chan struct{}

	// mu is the session lock: the machine and every field below are
	// accessed only under it. Effects are applied while holding it —
	// waiter sends are buffered and branch notifications non-blocking,
	// so no lock-held operation can block on a consumer.
	mu         sync.Mutex
	machine    *demux.Machine[Message]
	waiters    map[demux.Ticket]chan demux.Completion[Message]
	branches   map[demux.BranchID]*branchState
	terminated bool
	cause      error
}

// branchState is the session-side state of one consumer-facing branch,
// shared by Subscription and List handles.
type branchState struct {
	id     demux.BranchID
	notify chan struct{} // capacity 1: wake coalescing
	done   chan struct{}

	// Guarded by the client's session lock.
	terminal bool
	err      error
}

// Dial connects, performs the optional TLS handshake, reads the
// banner, authenticates, and returns a ready client, all bounded by
// ctx. On error nothing is retained: the connection is closed and no
// goroutine survives.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Address == "" {
		return nil, errors.New("ami: Dial: empty address")
	}
	if cfg.Username == "" {
		return nil, errors.New("ami: Dial: empty username")
	}
	if cfg.Auth > AuthMD5 {
		return nil, errors.New("ami: Dial: unknown auth method")
	}
	sess, err := cfg.Limits.resolve()
	if err != nil {
		return nil, err
	}
	ka, err := cfg.Keepalive.resolve()
	if err != nil {
		return nil, err
	}

	dial := cfg.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	raw, err := dial(ctx, "tcp", cfg.Address)
	if err != nil {
		return nil, &DialError{Phase: "dial", cause: err}
	}
	if cfg.TLS != nil {
		tcfg := cfg.TLS.Clone()
		if !tcfg.InsecureSkipVerify && tcfg.ServerName == "" {
			host, _, err := net.SplitHostPort(cfg.Address)
			if err != nil {
				host = cfg.Address
			}
			tcfg.ServerName = host
		}
		tconn := tls.Client(raw, tcfg)
		if err := tconn.HandshakeContext(ctx); err != nil {
			raw.Close()
			return nil, &DialError{Phase: "tls", cause: err}
		}
		raw = tconn
	}
	conn, err := NewConn(raw, cfg.Limits.Wire)
	if err != nil {
		raw.Close()
		return nil, err
	}

	c := &Client{
		conn:       conn,
		sess:       sess,
		ka:         ka,
		diag:       newDiagnostics(cfg.Logger),
		epoch:      time.Now(),
		idPrefix:   rand.Text() + "-",
		done:       make(chan struct{}),
		writeSem:   make(chan struct{}, 1),
		retirePoke: make(chan struct{}, 1),
		machine:    demux.New[Message](sess.machine),
		waiters:    make(map[demux.Ticket]chan demux.Completion[Message]),
		branches:   make(map[demux.BranchID]*branchState),
	}

	banner, err := conn.ReadBanner(ctx)
	if err != nil {
		conn.Close()
		return nil, &DialError{Phase: "banner", cause: err}
	}
	c.banner = banner
	if err := c.login(ctx, cfg); err != nil {
		conn.Close()
		return nil, &DialError{Phase: "login", cause: err}
	}

	c.ctx, c.cancel = context.WithCancelCause(context.Background())
	c.wg.Go(c.readLoop)
	c.wg.Go(c.expiryLoop)
	if !ka.Disabled {
		c.wg.Go(c.keepaliveLoop)
	}
	if c.diag != nil {
		// Outside the waitgroup: Done must not wait on the caller's
		// slog handler.
		go c.diag.run(c.ctx)
	}
	go func() {
		c.wg.Wait()
		// The workers only stop after termination committed, so no new
		// in-flight hold can start once this wait begins.
		c.inflight.Wait()
		close(c.done)
	}()
	c.diag.info("session ready", "keepalive", !ka.Disabled)
	return c, nil
}

// login authenticates over the still-synchronous connection: no other
// reader exists yet, and an unauthenticated session receives nothing
// unsolicited, so each action's response is the next message.
func (c *Client) login(ctx context.Context, cfg Config) error {
	// The plain-auth Login frame embeds the secret. The client retains
	// no copy of the configuration, so scrubbing the connection's reused
	// encode buffer on every exit path leaves no library-owned copy of
	// the credential in long-lived memory.
	defer c.conn.clearWriteBuffer()
	events := cfg.EventMask
	if events == "" {
		events = "off"
	}
	var act Action
	var err error
	if cfg.Auth == AuthMD5 {
		challenge, err := NewAction("Challenge", Field{Key: "AuthType", Value: "MD5"})
		if err != nil {
			return err
		}
		resp, err := c.loginExchange(ctx, challenge)
		if err != nil {
			return err
		}
		if !loginSuccess(resp) {
			return ErrLoginFailed
		}
		nonce := resp.Get("Challenge")
		if nonce == "" {
			return &ProtocolError{Category: "envelope", Dimension: "empty challenge"}
		}
		sum := md5.Sum([]byte(nonce + cfg.Secret))
		act, err = NewAction("Login",
			Field{Key: "AuthType", Value: "MD5"},
			Field{Key: "Username", Value: cfg.Username},
			Field{Key: "Key", Value: hex.EncodeToString(sum[:])},
			Field{Key: "Events", Value: events},
		)
		if err != nil {
			return err
		}
	} else {
		act, err = NewAction("Login",
			Field{Key: "Username", Value: cfg.Username},
			Field{Key: "Secret", Value: cfg.Secret},
			Field{Key: "Events", Value: events},
		)
		if err != nil {
			return err
		}
	}
	resp, err := c.loginExchange(ctx, act)
	if err != nil {
		return err
	}
	if !loginSuccess(resp) {
		return ErrLoginFailed
	}
	return nil
}

// loginSuccess reports an exact Response: Success disposition. The
// pre-session exchanges accept nothing weaker: Follows acknowledges
// command output and is meaningless for Challenge and Login, so it
// must not authenticate.
func loginSuccess(m Message) bool {
	return equalFoldASCII(m.Get("Response"), "Success")
}

// loginExchange writes one pre-session action and reads its response,
// classified under the same envelope rules as session traffic and
// strictly matched by ActionID: an event-class reply, a conflicting
// duplicate envelope, or a foreign or missing ActionID fails the login
// instead of passing as its response.
func (c *Client) loginExchange(ctx context.Context, act Action) (Message, error) {
	id := c.newActionID(demux.KindRequest)
	if err := c.conn.WriteAction(ctx, act, id); err != nil {
		return Message{}, err
	}
	msg, err := c.conn.ReadMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	if env := c.classify(msg); env.Class != demux.ClassResponse || env.ActionID != id {
		return Message{}, &ProtocolError{Category: "correlation", Dimension: "login response"}
	}
	return msg, nil
}

// newActionID issues the next opaque ActionID: the session prefix, the
// kind discriminator, and a monotonic suffix. Consumers must not parse
// the internal form.
func (c *Client) newActionID(kind demux.Kind) string {
	k := byte('r')
	if kind == demux.KindList {
		k = 'l'
	}
	return c.idPrefix + string(k) + strconv.FormatUint(c.seq.Add(1), 10)
}

// now is the session's monotonic logical clock in nanoseconds,
// supplied to the machine with every timestamped call.
func (c *Client) now() int64 {
	return int64(time.Since(c.epoch))
}

// Banner returns the raw protocol banner line the server sent. It is
// diagnostic, untrusted remote data; the library derives no behavior
// from it.
func (c *Client) Banner() string {
	return c.banner
}

// Done returns a channel that closes when the client has terminated:
// the reader, keepalive, and expiry workers have stopped, every
// in-flight Do and StartList has finished its correlation bookkeeping,
// no correlation or terminal-result state can change, and every
// admitted waiter has been made runnable with its committed result.
// Done does not wait for caller code to observe those returns. An
// already-terminal handle may still drain its own bounded queue after
// Done.
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// Err returns the client's stable root cause once terminal, and nil
// while the client is running. It is guaranteed stable after Done.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminated {
		return c.cause
	}
	return nil
}

// Close terminates the client: immediate, idempotent, and abortive. It
// stops admission, fails still-active operations, and closes the
// connection without waiting for consumer queues to drain or a remote
// logoff. When no earlier terminal cause exists, ErrClosed wins as the
// client's root cause.
func (c *Client) Close() error {
	c.die(ErrClosed)
	return nil
}

// die commits cause as the client's root cause — first winner only —
// runs the machine's death cascade, releases every waiter, and tears
// the connection down.
func (c *Client) die(cause error) {
	c.mu.Lock()
	if !c.terminated {
		c.terminated = true
		c.cause = cause
		var fx demux.Effects[Message]
		func() {
			// A machine wrecked by a mid-mutation panic must not take
			// the teardown with it: the sweep below releases whatever
			// the cascade could not.
			defer func() { recover() }()
			fx = c.machine.Kill(demux.ReasonKilled)
		}()
		c.deliverLocked(fx)
		c.sweepLocked()
		c.diag.info("client terminated", "cause", diagErrClass(cause))
	}
	c.mu.Unlock()
	c.finishTeardown()
}

// finishTeardown cancels the session context with the committed cause
// and closes the connection. Both are idempotent; every death path
// funnels through here.
func (c *Client) finishTeardown() {
	c.mu.Lock()
	cause := c.cause
	c.mu.Unlock()
	c.cancel(cause)
	c.conn.Close()
}

// applyLockedFx applies one machine call's effects under the session
// lock: a fatality commits the mapped root cause first, completions
// release their waiters, wakes signal branch consumers and close Done
// on newly terminal branches. The caller finishes a fatality's
// teardown after releasing the lock.
func (c *Client) applyLockedFx(fx demux.Effects[Message]) {
	if fx.Fatal != nil && !c.terminated {
		c.terminated = true
		c.cause = c.fatalError(fx.Fatal)
		c.diag.info("client terminated", "cause", diagErrClass(c.cause))
	}
	c.deliverLocked(fx)
	if c.terminated {
		c.sweepLocked()
	}
}

// deliverLocked releases completed waiters and wakes branch consumers.
// Waiter channels have capacity one and receive exactly one
// completion, and notifications coalesce into a capacity-one channel,
// so nothing here blocks while the lock is held.
func (c *Client) deliverLocked(fx demux.Effects[Message]) {
	for _, cpl := range fx.Complete {
		if w := c.waiters[cpl.Ticket]; w != nil {
			delete(c.waiters, cpl.Ticket)
			w <- cpl
		}
	}
	for _, id := range fx.Wake {
		b := c.branches[id]
		if b == nil {
			continue
		}
		select {
		case b.notify <- struct{}{}:
		default:
		}
		if reason, terminal := c.machine.Terminal(id); terminal {
			c.commitTerminalLocked(b, reason)
		}
	}
}

// sweepLocked releases anything the death cascade could not: it is the
// machine-independent guarantee that after termination every waiter is
// runnable and every branch Done closes.
func (c *Client) sweepLocked() {
	for tkt, w := range c.waiters {
		delete(c.waiters, tkt)
		w <- demux.Completion[Message]{Ticket: tkt, Delivered: false}
	}
	for _, b := range c.branches {
		if !b.terminal {
			c.commitTerminalLocked(b, demux.ReasonClientDead)
		}
	}
}

// registerBranchLocked creates the session-side state for an adopted
// or subscribed branch. The branch may already be terminal — lagged or
// completed while provisional — and its Done then closes immediately.
func (c *Client) registerBranchLocked(id demux.BranchID) *branchState {
	b := &branchState{
		id:     id,
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	c.branches[id] = b
	if reason, terminal := c.machine.Terminal(id); terminal {
		c.commitTerminalLocked(b, reason)
	}
	return b
}

// commitTerminalLocked commits a branch's first terminal result: the
// mapped error is stable from here on, Done closes, and any parked
// consumer wakes. A result that already won is preserved.
func (c *Client) commitTerminalLocked(b *branchState, reason demux.Reason) {
	if b.terminal {
		return
	}
	b.terminal = true
	b.err = c.errForReasonLocked(reason)
	close(b.done)
	// Terminal paths outside routing — a local Close, the death sweep —
	// carry no machine Wake effect, so the commit itself must unpark a
	// consumer blocked in takeBranch; without this, Next on a background
	// context would wait forever on a branch nothing will signal again.
	select {
	case b.notify <- struct{}{}:
	default:
	}
	if reason != demux.ReasonNone && reason != demux.ReasonClosed {
		c.diag.info("branch terminated", "reason", reason.String())
	}
}

// errForReasonLocked maps a machine terminal reason onto the public
// error surface. ReasonNone is a clean terminal: no error, io.EOF at
// the consumption end.
func (c *Client) errForReasonLocked(reason demux.Reason) error {
	switch reason {
	case demux.ReasonNone:
		return nil
	case demux.ReasonLagged:
		return ErrLagged
	case demux.ReasonClosed:
		return ErrClosed
	case demux.ReasonClientDead:
		if c.cause != nil {
			return c.cause
		}
		return ErrClosed
	case demux.ReasonOverflow:
		return &ListError{Failure: ListOverflowed}
	case demux.ReasonCancelled:
		return &ListError{Failure: ListCancelled}
	case demux.ReasonCountMismatch:
		return &ListError{Failure: ListCountMismatch}
	case demux.ReasonCountMalformed:
		return &ListError{Failure: ListCountMalformed}
	}
	return ErrClosed
}

// fatalError maps an internally detected fatality onto the public
// error surface.
func (c *Client) fatalError(f *demux.Fatality) error {
	switch f.Reason {
	case demux.ReasonRetirementExpired:
		kind := "request"
		if f.Kind == demux.KindList {
			kind = "list"
		}
		return &RetirementError{Kind: kind}
	case demux.ReasonEnvelopeInvalid:
		return &ProtocolError{Category: "envelope", Dimension: "message classification"}
	case demux.ReasonResponseNoID, demux.ReasonResponseForeign, demux.ReasonResponseUnmatched:
		return &ProtocolError{Category: "correlation", Dimension: f.Reason.String()}
	}
	return ErrClosed
}

// readLoop is the connection's only reader and the machine's only
// message router. Any panic below it — the machine's invariant checks
// included — converts into client death with the cause preserved.
func (c *Client) readLoop() {
	defer func() {
		if p := recover(); p != nil {
			c.die(panicError(p))
		}
	}()
	for {
		msg, err := c.conn.ReadMessage(c.ctx)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			if errors.Is(err, ErrClosed) {
				// A concurrent writer poisoned the connection and owns
				// the real transport cause; every such path commits it
				// through die. Racing it with a generic ErrClosed here
				// would steal the root cause, so wait for that commit.
				<-c.ctx.Done()
				return
			}
			c.die(err)
			return
		}
		fatal := c.routeOne(msg)
		c.pokeExpiry()
		if fatal {
			c.finishTeardown()
			return
		}
	}
}

// routeOne routes one message through the machine and applies its
// effects. The deferred unlock keeps the session lock panic-safe: a
// machine invariant panic unwinds into readLoop's recover with the
// lock released.
func (c *Client) routeOne(msg Message) (fatal bool) {
	env := c.classify(msg)
	c.mu.Lock()
	defer c.mu.Unlock()
	fx := c.machine.Route(env, msg)
	c.applyLockedFx(fx)
	return fx.Fatal != nil
}

// expiryLoop fires retirement/drain expiry at the machine's earliest
// deadline. Any call that may create a record or advance a deadline
// pokes it awake; with no record it sleeps until poked.
func (c *Client) expiryLoop() {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		c.mu.Lock()
		deadline, ok := c.machine.NextDeadline()
		terminated := c.terminated
		c.mu.Unlock()
		if terminated {
			return
		}
		if !ok {
			select {
			case <-c.ctx.Done():
				return
			case <-c.retirePoke:
				continue
			}
		}
		wait := time.Duration(deadline - c.now())
		if wait > 0 {
			timer.Reset(wait)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-c.retirePoke:
				timer.Stop()
				continue
			case <-timer.C:
			}
		}
		c.mu.Lock()
		fx := c.machine.Expire(c.now())
		c.applyLockedFx(fx)
		fatal := fx.Fatal != nil
		c.mu.Unlock()
		if fatal {
			c.finishTeardown()
			return
		}
	}
}

// pokeExpiry nudges the expiry loop to re-read the earliest deadline.
func (c *Client) pokeExpiry() {
	select {
	case c.retirePoke <- struct{}{}:
	default:
	}
}

// panicError converts a read-loop panic into the client root cause.
// Machine invariant panics carry a stable message with no remote data.
func panicError(p any) error {
	if err, ok := p.(error); ok {
		return err
	}
	if s, ok := p.(string); ok {
		return errors.New(s)
	}
	return fmt.Errorf("ami: read loop panic: %v", p)
}
