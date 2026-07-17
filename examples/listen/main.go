// Command listen is the minimal first contact with the library:
// connect, subscribe, enable the server-side event mask, and print
// every event name as it arrives.
//
// With no flags it runs a self-contained offline demo against an
// amitest fake server. Point it at a real AMI endpoint instead with
//
//	go run . -addr pbx.example.com:5038 -username manager -secret ...
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nakisen/ami"
	"github.com/nakisen/ami/amitest"
)

func main() {
	addr := flag.String("addr", "", "AMI address (host:port); empty runs the offline demo")
	username := flag.String("username", amitest.DefaultUsername, "manager username")
	secret := flag.String("secret", amitest.DefaultSecret, "manager secret")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *addr == "" {
		// The demo bounds itself; a real run ends on Ctrl-C.
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
	fmt.Println("connected:", client.Banner())

	// The gap-minimizing startup order: the zero Config logs in with
	// events off, so register the subscription first and only then open
	// the server-side mask with an Events action.
	sub, err := client.Subscribe()
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

	// Consume runs the handler on this goroutine — never on the read
	// loop — and closes the subscription on every exit path.
	err = sub.Consume(ctx, func(e ami.Event) error {
		fmt.Println("event:", e.Name())
		return nil
	})
	switch {
	case err == nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		fmt.Println("done")
	default:
		log.Fatalf("consume: %v", err)
	}
}

// startDemoServer runs a local fake AMI server that broadcasts a
// synthetic event a few times a second, the way a live Asterisk
// chatters.
func startDemoServer(ctx context.Context) *amitest.Server {
	srv := amitest.NewServer(amitest.Config{})
	go func() {
		tick := time.NewTicker(400 * time.Millisecond)
		defer tick.Stop()
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				srv.Event("PeerStatus",
					"Peer", fmt.Sprintf("PJSIP/demo-%d", i%3+1),
					"PeerStatus", "Reachable")
			}
		}
	}()
	return srv
}
