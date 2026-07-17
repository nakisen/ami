// Command reconnect is the application-owned reconnect pattern: the
// library never redials on its own, so a bounded outer loop watches
// each client generation end, reads its root cause, and climbs a
// backoff ladder that resets after a healthy session. Every new
// generation starts from scratch — a state-deriving application takes
// a fresh snapshot here (see examples/wallboard).
//
// With no flags it runs a self-contained offline demo against an
// amitest fake server, compressing every wait 100× so the generations
// and the ladder are visible in a few seconds; -addr points it at a
// real AMI endpoint with real timings.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nakisen/ami"
	"github.com/nakisen/ami/amitest"
)

// The ladder holds each wait before the next dial attempt; past its
// end the last rung repeats. A session healthy for at least
// healthyReset starts the ladder over, and giveUpAfter consecutive
// failures ends the process instead of hammering a dead server
// forever.
var ladder = []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 60 * time.Second}

const (
	healthyReset = time.Minute
	giveUpAfter  = 100
)

// timeScale compresses every wait for the offline demo. A real
// deployment keeps it at 1.
var timeScale time.Duration = 1

func main() {
	addr := flag.String("addr", "", "AMI address (host:port); empty runs the offline demo")
	username := flag.String("username", amitest.DefaultUsername, "manager username")
	secret := flag.String("secret", amitest.DefaultSecret, "manager secret")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *addr == "" {
		timeScale = 100
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		srv := startDemoServer(ctx)
		defer srv.Close()
		*addr = srv.Addr()
	}
	cfg := ami.Config{Address: *addr, Username: *username, Secret: *secret}

	attempt := 0
	for generation := 1; ; generation++ {
		client, err := ami.Dial(ctx, cfg)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("stopping")
				return
			}
			attempt++
			if attempt >= giveUpAfter {
				log.Fatalf("giving up after %d consecutive failed attempts: %v", attempt, err)
			}
			wait := ladder[min(attempt, len(ladder))-1]
			log.Printf("generation %d: dial failed (%v); retrying in %s", generation, err, wait)
			if !sleep(ctx, wait) {
				fmt.Println("stopping")
				return
			}
			continue
		}

		start := time.Now()
		log.Printf("generation %d: connected (%s)", generation, client.Banner())
		serveErr := serve(ctx, client)
		cause := client.Err() // the generation's committed root cause, once terminal
		client.Close()
		if ctx.Err() != nil {
			fmt.Println("stopping")
			return
		}
		if cause == nil {
			cause = serveErr
		}

		if time.Since(start) >= healthyReset/timeScale {
			// The session held long enough to count as healthy: the next
			// failure starts the ladder from the bottom again.
			attempt = 0
		}
		attempt++
		wait := ladder[min(attempt, len(ladder))-1]
		log.Printf("generation %d ended (%v); redialing in %s", generation, cause, wait)
		if !sleep(ctx, wait) {
			fmt.Println("stopping")
			return
		}
	}
}

// serve runs one client generation until it ends: subscribe, open the
// event mask, and consume until the stream terminates. The returned
// error is the consume terminal; the caller reads Client.Err for the
// committed root cause.
func serve(ctx context.Context, client *ami.Client) error {
	sub, err := client.Subscribe()
	if err != nil {
		return err
	}
	events, err := ami.NewAction("Events", ami.Field{Key: "EventMask", Value: "on"})
	if err != nil {
		return err
	}
	if _, err := client.Do(ctx, events); err != nil {
		return err
	}
	return sub.Consume(ctx, func(e ami.Event) error {
		fmt.Println("event:", e.Name())
		return nil
	})
}

// sleep waits for d scaled by timeScale, reporting false when ctx ends
// first.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d / timeScale):
		return true
	case <-ctx.Done():
		return false
	}
}

// startDemoServer scripts a fake Asterisk with a rough day: it chatters
// events, hangs up on its sessions twice, and finally goes away for
// good so the dial-failure rungs of the ladder become visible too.
func startDemoServer(ctx context.Context) *amitest.Server {
	srv := amitest.NewServer(amitest.Config{})
	go func() {
		tick := time.NewTicker(300 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				srv.Event("PeerStatus", "Peer", "PJSIP/demo-1", "PeerStatus", "Reachable")
			}
		}
	}()
	go func() {
		for range 2 {
			if !waitDemo(ctx, 1500*time.Millisecond) {
				return
			}
			srv.Hangup()
		}
		if waitDemo(ctx, 1500*time.Millisecond) {
			srv.Close()
		}
	}()
	return srv
}

// waitDemo pauses scripted misbehavior, reporting false when the demo
// ends first.
func waitDemo(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
