// Command wallboard is the snapshot-plus-live pattern: an explicit
// subscription registered before the server-side event mask opens, a
// QueueStatus list staged through clean completion, and live queue
// events reconciled onto the snapshot afterwards. On ErrLagged it
// performs the only honest recovery — a replacement subscription and a
// fresh snapshot on the same connection.
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
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
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

	err = run(ctx, client)
	switch {
	case err == nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		fmt.Println("done")
	default:
		log.Fatalf("wallboard: %v", err)
	}
}

// board is the derived wallboard state. A real application would key it
// by queue; the demo aggregates one queue for brevity.
type board struct {
	members map[string]string // member interface -> status label
	waiting int
}

func newBoard() *board {
	return &board{members: map[string]string{}}
}

func run(ctx context.Context, client *ami.Client) error {
	// Asterisk 12 still names the caller events Join and Leave; both
	// generations are matched so the example covers the supported floor.
	names := []string{"QueueMemberStatus", "QueueCallerJoin", "QueueCallerLeave", "Join", "Leave"}
	enabled := false
	for {
		// Subscribe before opening the mask (first pass) and before the
		// fresh snapshot (recovery pass), so no live event falls between
		// snapshot and stream.
		sub, err := client.Subscribe(ami.MatchEvents(names...))
		if err != nil {
			return err
		}
		if !enabled {
			events, err := ami.NewAction("Events", ami.Field{Key: "EventMask", Value: "call,agent"})
			if err != nil {
				return err
			}
			if _, err := client.Do(ctx, events); err != nil {
				return err
			}
			enabled = true
		}

		b, err := snapshot(ctx, client)
		if err != nil {
			return err
		}
		render(b)

		err = sub.Consume(ctx, func(e ami.Event) error {
			apply(b, e)
			render(b)
			return nil
		})
		if errors.Is(err, ami.ErrLagged) {
			// The consumer lost synchronization: events were discarded, so
			// the derived state can no longer be trusted. Replace the
			// subscription, then resnapshot.
			log.Println("subscription lagged; resubscribing and resnapshotting")
			continue
		}
		return err
	}
}

// snapshot stages one QueueStatus list through clean completion and
// only then publishes the staged state, per the list contract:
// provisional items are not a snapshot.
func snapshot(ctx context.Context, client *ami.Client) (*board, error) {
	action, err := ami.NewAction("QueueStatus")
	if err != nil {
		return nil, err
	}
	list, err := client.StartList(ctx, action, ami.ListSpec{
		CompletionEvents: []string{"QueueStatusComplete"},
		CountFields:      []string{"ListItems"},
	})
	if err != nil {
		return nil, err
	}
	staged := newBoard()
	for e, err := range list.All(ctx) {
		if err != nil {
			return nil, err
		}
		switch {
		case strings.EqualFold(e.Name(), "QueueMember"):
			staged.members[memberKey(e)] = statusLabel(e.Get("Status"))
		case strings.EqualFold(e.Name(), "QueueEntry"):
			staged.waiting++
		}
	}
	// All closed the list on exit; the completion event remains
	// available on the handle after a clean completion.
	if cpl, ok := list.Completion(); ok {
		log.Printf("snapshot complete: %s items", cpl.Get("ListItems"))
	}
	return staged, nil
}

// apply folds one live event into the board.
func apply(b *board, e ami.Event) {
	switch {
	case strings.EqualFold(e.Name(), "QueueMemberStatus"):
		b.members[memberKey(e)] = statusLabel(e.Get("Status"))
	default:
		// Join/Leave and QueueCallerJoin/QueueCallerLeave carry the
		// queue's absolute depth, so replaying events that overlap the
		// snapshot converges instead of double counting.
		if n, err := strconv.Atoi(e.Get("Count")); err == nil {
			b.waiting = n
		}
	}
}

// memberKey identifies a queue member across Asterisk versions, whose
// QueueMember events have drifted between Interface, Location, and
// Name for the same fact.
func memberKey(e ami.Event) string {
	for _, k := range []string{"Interface", "Location", "Name"} {
		if v := e.Get(k); v != "" {
			return v
		}
	}
	return "unknown"
}

// statusLabel maps the numeric device state of QueueMember and
// QueueMemberStatus events to a display label.
func statusLabel(code string) string {
	labels := map[string]string{
		"0": "Unknown", "1": "NotInUse", "2": "InUse", "3": "Busy",
		"4": "Invalid", "5": "Unavailable", "6": "Ringing",
		"7": "RingInUse", "8": "OnHold",
	}
	if l, ok := labels[code]; ok {
		return l
	}
	return "Status" + code
}

func render(b *board) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "waiting=%d |", b.waiting)
	for _, m := range slices.Sorted(maps.Keys(b.members)) {
		fmt.Fprintf(&sb, " %s=%s", m, b.members[m])
	}
	fmt.Println(sb.String())
}

// startDemoServer scripts a fake Asterisk: a three-member QueueStatus
// snapshot and a stream of live status changes.
func startDemoServer(ctx context.Context) *amitest.Server {
	srv := amitest.NewServer(amitest.Config{})
	srv.HandleAction("QueueStatus", func(call *amitest.Call) {
		call.Respond("Success", "EventList", "start", "Message", "Queue status will follow")
		call.Event("QueueParams", "Queue", "support", "Strategy", "ringall", "Calls", "1")
		call.Event("QueueMember", "Queue", "support",
			"Interface", "PJSIP/1001", "MemberName", "Demo Agent One", "Status", "1", "Paused", "0")
		call.Event("QueueMember", "Queue", "support",
			"Interface", "PJSIP/1002", "MemberName", "Demo Agent Two", "Status", "2", "Paused", "0")
		call.Event("QueueEntry", "Queue", "support",
			"Position", "1", "Channel", "PJSIP/demo-caller-1", "CallerIDNum", "5550001", "Wait", "12")
		call.Event("QueueStatusComplete", "EventList", "Complete", "ListItems", "4")
	})
	go func() {
		tick := time.NewTicker(700 * time.Millisecond)
		defer tick.Stop()
		status := []string{"2", "6", "1"}
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				srv.Event("QueueMemberStatus", "Queue", "support",
					"Interface", "PJSIP/1001", "MemberName", "Demo Agent One",
					"Status", status[i%len(status)], "Paused", "0")
				if i == 2 {
					srv.Event("QueueCallerLeave", "Queue", "support", "Count", "0", "Position", "1")
				}
			}
		}
	}()
	return srv
}
