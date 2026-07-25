// Command router is hook-style event handling built entirely in
// application space on the public surface: a tiny name-to-handler
// registry driven by Subscription.Consume. The goroutine running the
// handlers stays visible and application-owned, and the stream's
// terminal error arrives as an ordinary function return instead of
// vanishing inside a hidden callback dispatcher — which is why the
// library ships no OnEvent registry of its own.
//
// With no flags it runs a self-contained offline demo against an
// amitest fake server; -addr points it at a real AMI endpoint.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"time"

	"github.com/nakisen/ami"
	"github.com/nakisen/ami/amitest"
)

// A Router dispatches events to per-name handlers. It is deliberately
// tiny: registration completes before Run starts, so there is no
// concurrent mutation to coordinate.
type Router struct {
	names    []string
	handlers []func(ami.Event)
}

func NewRouter() *Router {
	return &Router{}
}

// Handle registers fn for the named event.
func (r *Router) Handle(name string, fn func(ami.Event)) {
	r.names = append(r.names, name)
	r.handlers = append(r.handlers, fn)
}

// Names returns the registered event names, ready to become the
// subscription's declarative SubSpec.Events filter: events without a
// handler are then never queued at all.
func (r *Router) Names() []string {
	return slices.Clone(r.names)
}

// Run consumes sub until ctx ends or the stream terminates, invoking
// each event's handler on the calling goroutine — never on the
// client's read loop — and returns the stream's terminal error.
//
// Matching goes through Event.Is, so the router agrees with the
// subscription about what an event name means: both use the protocol's
// ASCII case folding, which strings.EqualFold and strings.ToLower do not
// reproduce. A registry of this size scans; a large table would fold its
// own keys once instead.
func (r *Router) Run(ctx context.Context, sub *ami.Subscription) error {
	return sub.Consume(ctx, func(e ami.Event) error {
		for i, name := range r.names {
			if e.Is(name) {
				r.handlers[i](e)
				return nil
			}
		}
		return nil
	})
}

func main() {
	addr := flag.String("addr", "", "AMI address (host:port); empty runs the offline demo")
	username := flag.String("username", amitest.DefaultUsername, "manager username")
	secret := flag.String("secret", amitest.DefaultSecret, "manager secret")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *addr == "" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		srv := startDemoServer(ctx)
		defer srv.Close()
		*addr = srv.Addr()
	}

	client, err := ami.Dial(ctx, ami.Config{Address: *addr, Username: *username, Secret: *secret})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer client.Close()

	router := NewRouter()
	router.Handle("QueueMemberStatus", func(e ami.Event) {
		fmt.Printf("member %s -> status %s\n", e.Get("Interface"), e.Get("Status"))
	})
	router.Handle("PeerStatus", func(e ami.Event) {
		fmt.Printf("peer %s -> %s\n", e.Get("Peer"), e.Get("PeerStatus"))
	})

	sub, err := client.Subscribe(ami.SubSpec{Events: router.Names()})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	events, err := ami.NewAction("Events", ami.Field{Key: "EventMask", Value: "on"})
	if err != nil {
		log.Fatalf("compose Events action: %v", err)
	}
	if _, err := client.Do(ctx, events); err != nil {
		log.Fatalf("enable events: %v", err)
	}

	// The "hook dispatcher" is one visible, application-owned goroutine.
	done := make(chan error, 1)
	go func() { done <- router.Run(ctx, sub) }()

	// The main goroutine stays free for foreground work; handlers may
	// also call Do themselves, since they never run on the read loop.
	ping, err := ami.NewAction("Ping")
	if err != nil {
		log.Fatalf("compose Ping action: %v", err)
	}
	if _, err := client.Do(ctx, ping); err != nil {
		log.Fatalf("ping: %v", err)
	}
	fmt.Println("ping round-trip completed while the router keeps consuming")

	err = <-done
	switch {
	case err == nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		fmt.Println("done")
	default:
		log.Fatalf("router: %v", err)
	}
}

// startDemoServer runs a local fake AMI server that broadcasts the two
// routed event names plus one that no handler matches — the
// subscription's filter drops that one before it costs any queue
// space.
func startDemoServer(ctx context.Context) *amitest.Server {
	srv := amitest.NewServer(amitest.Config{})
	go func() {
		tick := time.NewTicker(400 * time.Millisecond)
		defer tick.Stop()
		status := []string{"1", "2", "6"}
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				srv.Event("QueueMemberStatus", "Queue", "support",
					"Interface", "PJSIP/1001", "Status", status[i%len(status)])
				srv.Event("PeerStatus", "Peer", "PJSIP/demo-2", "PeerStatus", "Reachable")
				srv.Event("Registry", "Domain", "demo.invalid", "Status", "Registered")
			}
		}
	}()
	return srv
}
