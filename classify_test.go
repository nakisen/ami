package ami

import (
	"testing"
	"time"

	"github.com/nakisen/ami/internal/demux"
)

func TestFoldASCII(t *testing.T) {
	tests := []struct{ in, want string }{
		{"", ""},
		{"newchannel", "newchannel"},
		{"NewChannel", "newchannel"},
		{"QUEUE-member_Status.9", "queue-member_status.9"},
		{"già", "già"}, // non-ASCII bytes pass through untouched
	}
	for _, tt := range tests {
		if got := foldASCII(tt.in); got != tt.want {
			t.Errorf("foldASCII(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
	if !equalFoldASCII("EventList", "eventlist") || equalFoldASCII("a", "ab") || equalFoldASCII("a", "b") {
		t.Error("equalFoldASCII misbehaves")
	}
}

func TestWireSize(t *testing.T) {
	// "Event: A\r\n" (10) + "K: vv\r\n" (7) + "\r\n" (2) = 19.
	m := newMessage([]Field{{Key: "Event", Value: "A"}, {Key: "K", Value: "vv"}})
	if got := wireSize(m); got != 19 {
		t.Fatalf("wireSize = %d, want 19", got)
	}
}

func TestClassify(t *testing.T) {
	c := &Client{idPrefix: "SESSIONPREFIX-", epoch: time.Now()}
	msg := func(fields ...Field) Message { return newMessage(fields) }

	tests := []struct {
		name string
		in   Message
		want demux.Envelope
	}{
		{
			"plain event",
			msg(Field{"Event", "NewChannel"}, Field{"Channel", "x"}),
			demux.Envelope{Class: demux.ClassEvent, Name: "newchannel"},
		},
		{
			"event beats response",
			msg(Field{"Response", "Success"}, Field{"Event", "OriginateResponse"}),
			demux.Envelope{Class: demux.ClassEvent, Name: "originateresponse"},
		},
		{
			"own request response",
			msg(Field{"Response", "Success"}, Field{"ActionID", "SESSIONPREFIX-r7"}),
			demux.Envelope{Class: demux.ClassResponse, Success: true, ActionID: "SESSIONPREFIX-r7", Own: true, Kind: demux.KindRequest},
		},
		{
			"follows is success",
			msg(Field{"Response", "Follows"}, Field{"ActionID", "SESSIONPREFIX-r8"}),
			demux.Envelope{Class: demux.ClassResponse, Success: true, ActionID: "SESSIONPREFIX-r8", Own: true, Kind: demux.KindRequest},
		},
		{
			"error response",
			msg(Field{"Response", "Error"}, Field{"ActionID", "SESSIONPREFIX-l2"}),
			demux.Envelope{Class: demux.ClassResponse, ActionID: "SESSIONPREFIX-l2", Own: true, Kind: demux.KindList},
		},
		{
			"foreign id",
			msg(Field{"Response", "Success"}, Field{"ActionID", "other-r1"}),
			demux.Envelope{Class: demux.ClassResponse, Success: true, ActionID: "other-r1"},
		},
		{
			"mangled own discriminator is foreign",
			msg(Field{"Response", "Success"}, Field{"ActionID", "SESSIONPREFIX-x1"}),
			demux.Envelope{Class: demux.ClassResponse, Success: true, ActionID: "SESSIONPREFIX-x1"},
		},
		{
			"list completion mark",
			msg(Field{"Event", "PeerlistComplete"}, Field{"EventList", "Complete"}, Field{"ActionID", "SESSIONPREFIX-l3"}),
			demux.Envelope{Class: demux.ClassEvent, Name: "peerlistcomplete", Mark: demux.MarkComplete, ActionID: "SESSIONPREFIX-l3", Own: true, Kind: demux.KindList},
		},
		{
			"cancelled mark",
			msg(Field{"Event", "X"}, Field{"EventList", "cancelled"}),
			demux.Envelope{Class: demux.ClassEvent, Name: "x", Mark: demux.MarkCancelled},
		},
		{
			"start mark",
			msg(Field{"Event", "X"}, Field{"EventList", "start"}),
			demux.Envelope{Class: demux.ClassEvent, Name: "x", Mark: demux.MarkStart},
		},
		{
			"unknown mark is lenient",
			msg(Field{"Event", "X"}, Field{"EventList", "perhaps"}),
			demux.Envelope{Class: demux.ClassEvent, Name: "x"},
		},
		{
			"repeated identical envelope fields tolerated",
			msg(Field{"Event", "X"}, Field{"event", "x"}),
			demux.Envelope{Class: demux.ClassEvent, Name: "x"},
		},
		{"neither field", msg(Field{"Foo", "bar"}), demux.Envelope{}},
		{"conflicting events", msg(Field{"Event", "A"}, Field{"Event", "B"}), demux.Envelope{}},
		{"conflicting responses", msg(Field{"Response", "Success"}, Field{"Response", "Error"}), demux.Envelope{}},
		{"empty event name", msg(Field{"Event", ""}), demux.Envelope{}},
		{
			"conflicting action ids",
			msg(Field{"Event", "A"}, Field{"ActionID", "one"}, Field{"ActionID", "two"}),
			demux.Envelope{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.classify(tt.in)
			got.Size, got.Now = 0, 0 // asserted separately
			if got != tt.want {
				t.Fatalf("classify() = %+v, want %+v", got, tt.want)
			}
		})
	}

	env := c.classify(msg(Field{"Event", "A"}))
	if env.Size != wireSize(msg(Field{"Event", "A"})) {
		t.Fatalf("classify Size = %d", env.Size)
	}
}
