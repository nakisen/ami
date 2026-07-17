package ami_test

import (
	"context"
	"log"

	"github.com/nakisen/ami"
)

// Example is the minimal session: connect, subscribe, enable the
// server-side event mask, and consume events off the read loop. The
// address is a placeholder, so the example is compile-checked but not
// run; self-contained runnable variants live in the repository's
// examples directory.
func Example() {
	ctx := context.Background()

	client, err := ami.Dial(ctx, ami.Config{
		Address:  "asterisk.example.com:5038",
		Username: "manager",
		Secret:   "secret",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// The zero Config logs in with events off: register the
	// subscription first, then open the server-side mask.
	sub, err := client.Subscribe(ami.MatchEvents("PeerStatus"))
	if err != nil {
		log.Fatal(err)
	}
	events, err := ami.NewAction("Events", ami.Field{Key: "EventMask", Value: "on"})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := client.Do(ctx, events); err != nil {
		log.Fatal(err)
	}

	err = sub.Consume(ctx, func(e ami.Event) error {
		log.Println(e.Name(), e.Get("Peer"), e.Get("PeerStatus"))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
