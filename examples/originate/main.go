// Command originate is the async-completion pattern: it places a call
// with an asynchronous Originate and receives the OriginateResponse
// through a follow subscription that the client registers atomically
// with the dispatch — before the first action byte is written. The
// application never manufactures an ActionID and never races a
// separate subscription against the send.
//
// With no flags it runs a self-contained offline demo against an
// amitest fake server; -addr points it at a real AMI endpoint (the
// demo numbers are synthetic — expect the real action to be rejected
// unless they exist in your dialplan).
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
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
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

	// Asterisk delivers OriginateResponse only to sessions whose event
	// mask includes the call class; a session left at the login default
	// of Events: off would wait on the follow forever.
	events, err := ami.NewAction("Events", ami.Field{Key: "EventMask", Value: "call"})
	if err != nil {
		log.Fatalf("compose Events action: %v", err)
	}
	if _, err := client.Do(ctx, events); err != nil {
		log.Fatalf("enable events: %v", err)
	}

	originate, err := ami.NewAction("Originate",
		ami.Field{Key: "Channel", Value: "PJSIP/demo-1001"},
		ami.Field{Key: "Context", Value: "demo"},
		ami.Field{Key: "Exten", Value: "600"},
		ami.Field{Key: "Priority", Value: "1"},
		ami.Field{Key: "CallerID", Value: "Demo <600>"},
		ami.Field{Key: "Async", Value: "true"},
	)
	if err != nil {
		log.Fatalf("compose Originate action: %v", err)
	}

	res, err := client.Do(ctx, originate, ami.WithFollow(ami.FollowSpec{
		CompletionEvents: []string{"OriginateResponse"},
	}))
	if err != nil {
		if respErr, ok := errors.AsType[*ami.ResponseError](err); ok {
			// The Message field is untrusted remote text — acceptable in a
			// demo, classify and redact before logging in production.
			log.Fatalf("originate rejected: %s", respErr.Response().Get("Message"))
		}
		log.Fatalf("originate: %v", err)
	}
	fmt.Println("queued:", res.Response.Get("Message"))

	// The follow delivers only events correlated to this action's ID and
	// completes cleanly once the declared OriginateResponse arrives, so
	// a clean Consume return means the outcome below is final.
	err = res.Follow.Consume(ctx, func(e ami.Event) error {
		fmt.Printf("%s: Response=%s Reason=%s Channel=%s\n",
			e.Name(), e.Get("Response"), e.Get("Reason"), e.Get("Channel"))
		return nil
	})
	if err != nil {
		log.Fatalf("follow: %v", err)
	}
	fmt.Println("originate completed")
}

// startDemoServer scripts a fake Asterisk that queues the originate,
// lets it "ring" briefly, and reports the answer through a correlated
// OriginateResponse.
func startDemoServer(ctx context.Context) *amitest.Server {
	srv := amitest.NewServer(amitest.Config{})
	srv.HandleAction("Originate", func(call *amitest.Call) {
		call.Respond("Success", "Message", "Originate successfully queued")
		channel := call.Get("Channel")
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(600 * time.Millisecond):
			}
			call.Event("OriginateResponse",
				"Response", "Success",
				"Channel", channel,
				"Context", "demo",
				"Exten", "600",
				"Reason", "4", // 4: the call was answered
				"Uniqueid", "1752700000.42",
				"CallerIDNum", "600",
				"CallerIDName", "Demo")
		}()
	})
	return srv
}
