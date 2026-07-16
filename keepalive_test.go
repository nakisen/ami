package ami

import (
	"context"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

// dialBubble dials a piped client inside the current synctest bubble.
// The fake clock makes every configured duration fire deterministically
// with zero wall time.
func dialBubble(t *testing.T, mutate func(*Config)) (*Client, *script) {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()
	cfg := testConfig(clientEnd)
	cfg.Keepalive = KeepaliveConfig{} // enabled defaults unless mutated
	if mutate != nil {
		mutate(&cfg)
	}
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
	return c, s
}

func TestKeepalivePingCycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s := dialBubble(t, nil)

		// The first Ping becomes due one interval after readiness; a
		// valid response schedules the next full interval.
		for cycle := range 2 {
			ping := s.readAction()
			if ping.name != "Ping" {
				t.Fatalf("cycle %d: keepalive sent %q", cycle, ping.name)
			}
			s.respond(ping.id, "Success", "Ping", "Pong")
		}

		// Ordinary traffic flows unaffected between pings.
		s.event("FullyBooted")

		// The third Ping's response never arrives: the client dies with
		// ErrPingTimeout as its root cause.
		s.readAction()
		<-c.Done()
		var ke *KeepaliveError
		if err := c.Err(); !errors.As(err, &ke) || !errors.Is(err, ErrPingTimeout) {
			t.Fatalf("Err() = %v, want KeepaliveError wrapping ErrPingTimeout", err)
		}
		s.conn.Close()
	})
}

func TestKeepaliveWriteTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s := dialBubble(t, nil)

		// Nothing reads the server end, so the due Ping's write can
		// never complete; the write-attempt deadline terminates the
		// client with ErrPingWriteTimeout.
		<-c.Done()
		var ke *KeepaliveError
		if err := c.Err(); !errors.As(err, &ke) || !errors.Is(err, ErrPingWriteTimeout) {
			t.Fatalf("Err() = %v, want KeepaliveError wrapping ErrPingWriteTimeout", err)
		}
		s.conn.Close()
	})
}

func TestKeepaliveRejectedPing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s := dialBubble(t, nil)
		ping := s.readAction()
		s.respond(ping.id, "Error", "Message", "no pong for you")
		<-c.Done()
		err := c.Err()
		var ke *KeepaliveError
		if !errors.As(err, &ke) || ke.Phase != "rejected" {
			t.Fatalf("Err() = %v, want KeepaliveError{rejected}", err)
		}
		if errors.Is(err, ErrPingTimeout) || errors.Is(err, ErrPingWriteTimeout) {
			t.Fatalf("Err() = %v: a rejection must stay distinct from the timeouts", err)
		}
		s.conn.Close()
	})
}

func TestKeepaliveDisabledSendsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s := dialBubble(t, func(cfg *Config) {
			cfg.Keepalive = KeepaliveConfig{Disabled: true}
		})
		// Let more than an interval of fake time pass with the server
		// reading nothing: with keepalive disabled no Ping is due and
		// nothing terminates.
		time.Sleep(2 * defaultKeepaliveInterval)
		if err := c.Err(); err != nil {
			t.Fatalf("Err() = %v, want a quiet living client", err)
		}
		c.Close()
		<-c.Done()
		s.conn.Close()
	})
}

func TestRetirementExpiryTerminatesClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s := dialBubble(t, func(cfg *Config) {
			cfg.Keepalive = KeepaliveConfig{Disabled: true}
		})

		// Abandon one fully written request: its reserved slot becomes
		// a live retirement record.
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			act, _ := NewAction("Originate")
			_, err := c.Do(ctx, act)
			done <- err
		}()
		s.readAction()
		cancel()
		if err := <-done; !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatalf("Do() = %v, want outcome unknown", err)
		}

		// No terminal evidence ever arrives: at the retirement lifetime
		// the expiry closes the client rather than risk misclassifying
		// late traffic.
		<-c.Done()
		var re *RetirementError
		if err := c.Err(); !errors.As(err, &re) || !errors.Is(err, ErrRetirementExpired) {
			t.Fatalf("Err() = %v, want RetirementError", err)
		}
		if re.Kind != "request" {
			t.Fatalf("RetirementError.Kind = %q, want request", re.Kind)
		}
		s.conn.Close()
	})
}

func TestRetirementReleaseByLateResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, s := dialBubble(t, func(cfg *Config) {
			cfg.Keepalive = KeepaliveConfig{Disabled: true}
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			act, _ := NewAction("Originate")
			_, err := c.Do(ctx, act)
			done <- err
		}()
		act := s.readAction()
		cancel()
		<-done

		// The late response arrives well before expiry: the record
		// releases and the client outlives the original lifetime.
		time.Sleep(defaultRetirementLifetime / 2)
		s.respond(act.id, "Success")
		s.sync(c)
		time.Sleep(2 * defaultRetirementLifetime)
		if err := c.Err(); err != nil {
			t.Fatalf("Err() = %v, want the record released by its evidence", err)
		}
		c.Close()
		<-c.Done()
		s.conn.Close()
	})
}
