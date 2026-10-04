package wire

import (
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// testLimits returns limits generous enough that ordinary test messages
// never brush against them; limit tests override single dimensions.
func testLimits() Limits {
	return Limits{
		MaxBannerBytes:        128,
		MaxLineBytes:          256,
		MaxFields:             32,
		MaxMessageBytes:       4096,
		MaxCommandOutputLines: 64,
		MaxCommandOutputBytes: 8192,
		MaxActionFields:       32,
		MaxActionLineBytes:    256,
		MaxActionBytes:        4096,
	}
}

func newTestReader(in string, lim Limits) *Reader {
	return NewReader(strings.NewReader(in), lim)
}

func TestReadBanner(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr error
	}{
		{"crlf terminator", "Asterisk Call Manager/2.10.6\r\nEvent: X\r\n\r\n", "Asterisk Call Manager/2.10.6", nil},
		{"bare lf terminator", "Asterisk Call Manager/9.0.0\n", "Asterisk Call Manager/9.0.0", nil},
		{"not an ami banner is still returned", "HTTP/1.1 400 Bad Request\r\n", "HTTP/1.1 400 Bad Request", nil},
		{"empty stream", "", "", io.EOF},
		{"eof mid line", "Asterisk Call", "", io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newTestReader(tt.in, testLimits()).ReadBanner()
			if !errors.Is(err, tt.wantErr) || got != tt.want {
				t.Fatalf("ReadBanner() = (%q, %v), want (%q, %v)", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestReadBannerLimit(t *testing.T) {
	lim := testLimits()
	lim.MaxBannerBytes = 5
	if got, err := newTestReader("12345\r\n", lim).ReadBanner(); err != nil || got != "12345" {
		t.Fatalf("banner at exact limit = (%q, %v), want (\"12345\", nil)", got, err)
	}
	if _, err := newTestReader("123456\r\n", lim).ReadBanner(); !errors.Is(err, ErrBannerTooLong) {
		t.Fatalf("banner one past limit: err = %v, want ErrBannerTooLong", err)
	}
}

func TestReadMessage(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []Field
	}{
		{
			"single field",
			"Event: FullyBooted\r\n\r\n",
			[]Field{{Key: "Event", Value: "FullyBooted"}},
		},
		{
			"duplicate keys keep wire order",
			"Event: Newchannel\r\nVariable: a=1\r\nChanVariable: x=9\r\nVariable: b=2\r\n\r\n",
			[]Field{
				{Key: "Event", Value: "Newchannel"},
				{Key: "Variable", Value: "a=1"},
				{Key: "ChanVariable", Value: "x=9"},
				{Key: "Variable", Value: "b=2"},
			},
		},
		{
			"empty value with trailing space",
			"Event: X\r\nEmpty: \r\n\r\n",
			[]Field{{Key: "Event", Value: "X"}, {Key: "Empty", Value: ""}},
		},
		{
			"empty value without space",
			"Event: X\r\nEmpty:\r\n\r\n",
			[]Field{{Key: "Event", Value: "X"}, {Key: "Empty", Value: ""}},
		},
		{
			"no space after colon",
			"Key:value\r\n\r\n",
			[]Field{{Key: "Key", Value: "value"}},
		},
		{
			"second space is part of the value",
			"Key:  padded\r\n\r\n",
			[]Field{{Key: "Key", Value: " padded"}},
		},
		{
			"colon inside value",
			"AppData: Dial(PJSIP/100,30,tT):1\r\n\r\n",
			[]Field{{Key: "AppData", Value: "Dial(PJSIP/100,30,tT):1"}},
		},
		{
			"bare lf terminators",
			"Event: X\nHeader2: digits\n\n",
			[]Field{{Key: "Event", Value: "X"}, {Key: "Header2", Value: "digits"}},
		},
		{
			"mixed terminators",
			"A: 1\nB: 2\r\n\n",
			[]Field{{Key: "A", Value: "1"}, {Key: "B", Value: "2"}},
		},
		{
			"key case preserved verbatim",
			"eVENT: x\r\n\r\n",
			[]Field{{Key: "eVENT", Value: "x"}},
		},
		{
			"non-utf8 value bytes preserved",
			"Bin: \xff\xfe\x01\r\n\r\n",
			[]Field{{Key: "Bin", Value: "\xff\xfe\x01"}},
		},
		{
			"interior carriage return preserved",
			"K: a\rb\r\n\r\n",
			[]Field{{Key: "K", Value: "a\rb"}},
		},
		{
			"modern command output keeps original keys",
			"Response: Success\r\nActionID: 7\r\nOutput: line one\r\noutput: line two\r\nOutput: \r\n\r\n",
			[]Field{
				{Key: "Response", Value: "Success"},
				{Key: "ActionID", Value: "7"},
				{Key: "Output", Value: "line one"},
				{Key: "output", Value: "line two"},
				{Key: "Output", Value: ""},
			},
		},
		{
			"response follows beyond the first field is not legacy framing",
			"Event: E\r\nResponse: Follows\r\n\r\n",
			[]Field{{Key: "Event", Value: "E"}, {Key: "Response", Value: "Follows"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newTestReader(tt.in, testLimits()).ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage() error = %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ReadMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadMessageErrors(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr error
	}{
		{"clean eof at boundary", "", io.EOF},
		{"blank line before the first message", "\r\nEvent: X\r\n\r\n", ErrEmptyMessage},
		{"first line without colon", "garbage\r\n\r\n", ErrMalformedLine},
		{"empty key", ": value\r\n\r\n", ErrMalformedLine},
		{"later line without colon", "Event: X\r\ngarbage\r\n\r\n", ErrMalformedLine},
		{"eof mid line", "Event: X", io.ErrUnexpectedEOF},
		{"eof before blank terminator", "Event: X\r\n", io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newTestReader(tt.in, testLimits()).ReadMessage(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReadMessage() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestReadMessageSequence(t *testing.T) {
	r := newTestReader("Event: One\r\n\r\nEvent: Two\r\nExtra: y\r\n\r\n", testLimits())
	first, err := r.ReadMessage()
	if err != nil || !slices.Equal(first, []Field{{Key: "Event", Value: "One"}}) {
		t.Fatalf("first message = (%v, %v)", first, err)
	}
	second, err := r.ReadMessage()
	want := []Field{{Key: "Event", Value: "Two"}, {Key: "Extra", Value: "y"}}
	if err != nil || !slices.Equal(second, want) {
		t.Fatalf("second message = (%v, %v), want (%v, nil)", second, err, want)
	}
	if _, err := r.ReadMessage(); err != io.EOF {
		t.Fatalf("read past final message: err = %v, want io.EOF", err)
	}
}

// strayTerminator is the framing quirk inter-message padding exists
// for: the Message value already ends in a bare line feed, so the
// server's own message terminator leaves one blank line behind the
// response it just ended.
const strayTerminator = "Response: Error\r\nActionID: 7\r\nMessage: No endpoints found\n\r\n\r\n"

func TestReadMessagePadding(t *testing.T) {
	one := []Field{{Key: "Event", Value: "One"}}
	two := []Field{{Key: "Event", Value: "Two"}}
	legacy := []Field{{Key: "Response", Value: "Follows"}, {Key: "Output", Value: "row"}}
	tests := []struct {
		name string
		in   string
		want [][]Field
	}{
		{
			"stray terminator behind an error response",
			strayTerminator + "Response: Success\r\nActionID: 8\r\nPing: Pong\r\n\r\n",
			[][]Field{
				{
					{Key: "Response", Value: "Error"},
					{Key: "ActionID", Value: "7"},
					{Key: "Message", Value: "No endpoints found"},
				},
				{
					{Key: "Response", Value: "Success"},
					{Key: "ActionID", Value: "8"},
					{Key: "Ping", Value: "Pong"},
				},
			},
		},
		{"bare lf padding", "Event: One\n\n\nEvent: Two\n\n", [][]Field{one, two}},
		{"mixed run of padding", "Event: One\r\n\r\n\r\n\n\r\nEvent: Two\r\n\r\n", [][]Field{one, two}},
		{"padding between every message", "Event: One\r\n\r\n\r\nEvent: Two\r\n\r\n\r\nEvent: One\r\n\r\n", [][]Field{one, two, one}},
		{
			"padding behind a legacy command frame",
			"Response: Follows\r\nrow\n--END COMMAND--\r\n\r\n\r\nEvent: Two\r\n\r\n",
			[][]Field{legacy, two},
		},
		{
			"padding ahead of a legacy command frame",
			"Event: One\r\n\r\n\r\nResponse: Follows\r\nrow\n--END COMMAND--\r\n\r\n",
			[][]Field{one, legacy},
		},
		{"trailing padding at end of stream", "Event: One\r\n\r\n\r\n\n", [][]Field{one}},
	}
	transports := []struct {
		name string
		wrap func(io.Reader) io.Reader
	}{
		{"coalesced", func(r io.Reader) io.Reader { return r }},
		{"one byte per read", iotest.OneByteReader},
	}
	for _, tt := range tests {
		for _, tr := range transports {
			t.Run(tt.name+"/"+tr.name, func(t *testing.T) {
				r := NewReader(tr.wrap(strings.NewReader(tt.in)), testLimits())
				for i, want := range tt.want {
					got, err := r.ReadMessage()
					if err != nil || !slices.Equal(got, want) {
						t.Fatalf("message %d = (%v, %v), want (%v, nil)", i, got, err, want)
					}
				}
				// Padding belongs to no frame: the stream ends cleanly at a
				// message boundary however many blank lines trail it.
				if _, err := r.ReadMessage(); err != io.EOF {
					t.Fatalf("read past final message: err = %v, want io.EOF", err)
				}
				if r.Dirty() {
					t.Fatal("discarded padding left the reader dirty")
				}
			})
		}
	}
}

// TestReadMessagePaddingKeepsFramingErrors pins what padding does not
// excuse: after the first message parses, each stream must still fail
// exactly as the same bytes would without the padding ahead of them.
func TestReadMessagePaddingKeepsFramingErrors(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr error
	}{
		{"line without colon after padding", "Event: One\r\n\r\n\r\ngarbage\r\n\r\n", ErrMalformedLine},
		{"empty key after padding", "Event: One\r\n\r\n\r\n: value\r\n\r\n", ErrMalformedLine},
		{"eof inside a padding line", "Event: One\r\n\r\n\r", io.ErrUnexpectedEOF},
		{"eof inside the message after padding", "Event: One\r\n\r\n\r\nEvent: Two\r\n", io.ErrUnexpectedEOF},
		{
			"legacy terminator without its blank line after padding",
			"Event: One\r\n\r\n\r\nResponse: Follows\r\n--END COMMAND--\r\nX: y\r\n\r\n",
			ErrCommandFraming,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestReader(tt.in, testLimits())
			if _, err := r.ReadMessage(); err != nil {
				t.Fatalf("first message: %v", err)
			}
			if _, err := r.ReadMessage(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReadMessage() error = %v, want %v", err, tt.wantErr)
			}
			if !r.Dirty() {
				t.Fatal("clean after a frame failed partway")
			}
		})
	}
}

// TestReadMessageBlankLineBeforeFirstMessage pins the case that stays
// an empty message: nothing has been terminated yet, so a blank line
// where the stream's first message must start is not padding.
func TestReadMessageBlankLineBeforeFirstMessage(t *testing.T) {
	r := newTestReader("Asterisk Call Manager/9.0.0\r\n\r\nEvent: X\r\n\r\n", testLimits())
	if _, err := r.ReadBanner(); err != nil {
		t.Fatalf("ReadBanner() error = %v", err)
	}
	if _, err := r.ReadMessage(); !errors.Is(err, ErrEmptyMessage) {
		t.Fatalf("blank line right after the banner: err = %v, want ErrEmptyMessage", err)
	}
}

func TestReadMessagePaddingLimit(t *testing.T) {
	lim := testLimits()
	lim.MaxMessageBytes = 8
	const msg = "A: 1\r\n\r\n" // exactly 8 raw bytes: itself at the limit
	want := []Field{{Key: "A", Value: "1"}}
	tests := []struct {
		name    string
		padding string
		wantErr error // nil means the run must be discarded
	}{
		{"crlf run at exact limit", strings.Repeat("\r\n", 4), nil},
		{"crlf run one past limit", strings.Repeat("\r\n", 4) + "\n", ErrMessageTooLarge},
		{"bare lf run at exact limit", strings.Repeat("\n", 8), nil},
		{"bare lf run one past limit", strings.Repeat("\n", 9), ErrMessageTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Two runs prove the budget is per run, and the exact-limit
			// messages around them prove a run is never charged to the
			// message it precedes.
			r := newTestReader(msg+tt.padding+msg+tt.padding+msg, lim)
			if got, err := r.ReadMessage(); err != nil || !slices.Equal(got, want) {
				t.Fatalf("first message = (%v, %v)", got, err)
			}
			if tt.wantErr != nil {
				if _, err := r.ReadMessage(); !errors.Is(err, tt.wantErr) {
					t.Fatalf("ReadMessage() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			for i := 1; i <= 2; i++ {
				if got, err := r.ReadMessage(); err != nil || !slices.Equal(got, want) {
					t.Fatalf("message %d after padding = (%v, %v), want (%v, nil)", i, got, err, want)
				}
			}
		})
	}
}

func TestReadMessageFragmented(t *testing.T) {
	in := "Asterisk Call Manager/5.0.2\r\n" +
		"Response: Follows\r\nPrivilege: Command\r\nActionID: 42\r\nrow one\nrow two\n--END COMMAND--\r\n\r\n" +
		"Event: Later\r\n\r\n"
	r := NewReader(iotest.OneByteReader(strings.NewReader(in)), testLimits())
	if banner, err := r.ReadBanner(); err != nil || banner != "Asterisk Call Manager/5.0.2" {
		t.Fatalf("banner = (%q, %v)", banner, err)
	}
	legacy, err := r.ReadMessage()
	wantLegacy := []Field{
		{Key: "Response", Value: "Follows"},
		{Key: "Privilege", Value: "Command"},
		{Key: "ActionID", Value: "42"},
		{Key: "Output", Value: "row one"},
		{Key: "Output", Value: "row two"},
	}
	if err != nil || !slices.Equal(legacy, wantLegacy) {
		t.Fatalf("legacy message = (%v, %v), want (%v, nil)", legacy, err, wantLegacy)
	}
	event, err := r.ReadMessage()
	if err != nil || !slices.Equal(event, []Field{{Key: "Event", Value: "Later"}}) {
		t.Fatalf("trailing message = (%v, %v)", event, err)
	}
}

func TestReadMessageLegacyCommand(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []Field
	}{
		{
			"full frame",
			"Response: Follows\r\nPrivilege: Command\r\nActionID: 42\r\nline one\nline two\n--END COMMAND--\r\n\r\n",
			[]Field{
				{Key: "Response", Value: "Follows"},
				{Key: "Privilege", Value: "Command"},
				{Key: "ActionID", Value: "42"},
				{Key: "Output", Value: "line one"},
				{Key: "Output", Value: "line two"},
			},
		},
		{
			"payload may be empty and contain colons",
			"Response: Follows\r\nPrivilege: Command\r\nName/Context: default\n\nTotal: 2\n--END COMMAND--\r\n\r\n",
			[]Field{
				{Key: "Response", Value: "Follows"},
				{Key: "Privilege", Value: "Command"},
				{Key: "Output", Value: "Name/Context: default"},
				{Key: "Output", Value: ""},
				{Key: "Output", Value: "Total: 2"},
			},
		},
		{
			"no output at all",
			"Response: Follows\r\nPrivilege: Command\r\nActionID: 1\r\n--END COMMAND--\r\n\r\n",
			[]Field{
				{Key: "Response", Value: "Follows"},
				{Key: "Privilege", Value: "Command"},
				{Key: "ActionID", Value: "1"},
			},
		},
		{
			"no trailer headers",
			"Response: Follows\r\nraw\n--END COMMAND--\r\n\r\n",
			[]Field{
				{Key: "Response", Value: "Follows"},
				{Key: "Output", Value: "raw"},
			},
		},
		{
			"terminator glued to unterminated output",
			"Response: Follows\r\nPrivilege: Command\r\npartial--END COMMAND--\r\n\r\n",
			[]Field{
				{Key: "Response", Value: "Follows"},
				{Key: "Privilege", Value: "Command"},
				{Key: "Output", Value: "partial"},
			},
		},
		{
			"header-looking payload after payload starts",
			"Response: Follows\r\nplain\nPrivilege: fake\nActionID: fake\n--END COMMAND--\r\n\r\n",
			[]Field{
				{Key: "Response", Value: "Follows"},
				{Key: "Output", Value: "plain"},
				{Key: "Output", Value: "Privilege: fake"},
				{Key: "Output", Value: "ActionID: fake"},
			},
		},
		{
			"trailer headers match case-insensitively",
			"response: follows\r\nactionid: 7\r\nout\n--END COMMAND--\r\n\r\n",
			[]Field{
				{Key: "response", Value: "follows"},
				{Key: "actionid", Value: "7"},
				{Key: "Output", Value: "out"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newTestReader(tt.in, testLimits()).ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage() error = %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ReadMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadMessageLegacyCommandErrors(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr error
	}{
		{
			"terminator not followed by blank line",
			"Response: Follows\r\n--END COMMAND--\r\nX: y\r\n\r\n",
			ErrCommandFraming,
		},
		{"eof mid payload", "Response: Follows\r\nrow one\n", io.ErrUnexpectedEOF},
		{"eof after terminator", "Response: Follows\r\n--END COMMAND--\r\n", io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newTestReader(tt.in, testLimits()).ReadMessage(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReadMessage() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestReadMessageLimits(t *testing.T) {
	tests := []struct {
		name    string
		adjust  func(*Limits)
		in      string
		wantErr error // nil means the message must parse
	}{
		{
			"line at exact limit",
			func(l *Limits) { l.MaxLineBytes = 8 },
			"A: 12345\r\n\r\n", // content is exactly 8 bytes
			nil,
		},
		{
			"line one past limit",
			func(l *Limits) { l.MaxLineBytes = 8 },
			"A: 123456\r\n\r\n",
			ErrLineTooLong,
		},
		{
			"fields at exact limit",
			func(l *Limits) { l.MaxFields = 2 },
			"A: 1\r\nB: 2\r\n\r\n",
			nil,
		},
		{
			"fields one past limit",
			func(l *Limits) { l.MaxFields = 2 },
			"A: 1\r\nB: 2\r\nC: 3\r\n\r\n",
			ErrTooManyFields,
		},
		{
			"message bytes at exact limit",
			func(l *Limits) { l.MaxMessageBytes = 8 },
			"A: 1\r\n\r\n", // 6 field bytes + 2 terminator bytes
			nil,
		},
		{
			"message bytes one past limit",
			func(l *Limits) { l.MaxMessageBytes = 8 },
			"A: 12\r\n\r\n",
			ErrMessageTooLarge,
		},
		{
			"output fields do not consume the field limit",
			func(l *Limits) { l.MaxFields = 1 },
			"Response: Success\r\nOutput: a\r\nOutput: b\r\nOutput: c\r\n\r\n",
			nil,
		},
		{
			"output lines at exact limit",
			func(l *Limits) { l.MaxCommandOutputLines = 2 },
			"Response: Success\r\nOutput: a\r\nOutput: b\r\n\r\n",
			nil,
		},
		{
			"output lines one past limit",
			func(l *Limits) { l.MaxCommandOutputLines = 2 },
			"Response: Success\r\nOutput: a\r\nOutput: b\r\nOutput: c\r\n\r\n",
			ErrTooManyOutputLines,
		},
		{
			"output bytes at exact limit",
			func(l *Limits) { l.MaxCommandOutputBytes = 22 },
			"Response: Success\r\nOutput: a\r\nOutput: b\r\n\r\n", // two 11-byte raw output lines
			nil,
		},
		{
			"output bytes one past limit",
			func(l *Limits) { l.MaxCommandOutputBytes = 21 },
			"Response: Success\r\nOutput: a\r\nOutput: b\r\n\r\n",
			ErrOutputTooLarge,
		},
		{
			"legacy payload lines charge the output line limit",
			func(l *Limits) { l.MaxCommandOutputLines = 2 },
			"Response: Follows\r\none\ntwo\nthree\n--END COMMAND--\r\n\r\n",
			ErrTooManyOutputLines,
		},
		{
			"legacy payload bytes charge the output byte limit",
			func(l *Limits) { l.MaxCommandOutputBytes = 8 },
			"Response: Follows\r\n123456789\n--END COMMAND--\r\n\r\n",
			ErrOutputTooLarge,
		},
		{
			"legacy payload within output budgets",
			func(l *Limits) { l.MaxCommandOutputLines = 2; l.MaxCommandOutputBytes = 8 },
			"Response: Follows\r\none\ntwo\n--END COMMAND--\r\n\r\n", // two 4-byte raw payload lines
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lim := testLimits()
			tt.adjust(&lim)
			_, err := newTestReader(tt.in, lim).ReadMessage()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ReadMessage() error = %v, want success", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReadMessage() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestReaderDirty(t *testing.T) {
	r := newTestReader("Event: X\r\n\r\nEvent: Y\r\nPartial", testLimits())
	if r.Dirty() {
		t.Fatal("fresh reader reports dirty")
	}
	if _, err := r.ReadMessage(); err != nil {
		t.Fatalf("first message: %v", err)
	}
	if r.Dirty() {
		t.Fatal("dirty after a completely returned message")
	}
	if _, err := r.ReadMessage(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated message: err = %v, want io.ErrUnexpectedEOF", err)
	}
	if !r.Dirty() {
		t.Fatal("clean after consuming part of an unreturned frame")
	}

	empty := newTestReader("", testLimits())
	if _, err := empty.ReadMessage(); err != io.EOF || empty.Dirty() {
		t.Fatalf("empty stream: err = %v, dirty = %v, want io.EOF and clean", err, empty.Dirty())
	}

	partialBanner := newTestReader("Asterisk Call", testLimits())
	if _, err := partialBanner.ReadBanner(); !errors.Is(err, io.ErrUnexpectedEOF) || !partialBanner.Dirty() {
		t.Fatalf("partial banner: err = %v, dirty = %v, want io.ErrUnexpectedEOF and dirty", err, partialBanner.Dirty())
	}
	fullBanner := newTestReader("Asterisk Call Manager/2.10.6\r\n", testLimits())
	if _, err := fullBanner.ReadBanner(); err != nil || fullBanner.Dirty() {
		t.Fatalf("complete banner: err = %v, dirty = %v, want nil and clean", err, fullBanner.Dirty())
	}
}

func TestLimitsValidate(t *testing.T) {
	if err := testLimits().Validate(); err != nil {
		t.Fatalf("Validate() on positive limits = %v", err)
	}
	tests := []struct {
		name string
		zero func(*Limits)
	}{
		{"MaxBannerBytes", func(l *Limits) { l.MaxBannerBytes = 0 }},
		{"MaxLineBytes", func(l *Limits) { l.MaxLineBytes = 0 }},
		{"MaxFields", func(l *Limits) { l.MaxFields = -1 }},
		{"MaxMessageBytes", func(l *Limits) { l.MaxMessageBytes = 0 }},
		{"MaxCommandOutputLines", func(l *Limits) { l.MaxCommandOutputLines = 0 }},
		{"MaxCommandOutputBytes", func(l *Limits) { l.MaxCommandOutputBytes = -5 }},
		{"MaxActionFields", func(l *Limits) { l.MaxActionFields = 0 }},
		{"MaxActionLineBytes", func(l *Limits) { l.MaxActionLineBytes = 0 }},
		{"MaxActionBytes", func(l *Limits) { l.MaxActionBytes = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lim := testLimits()
			tt.zero(&lim)
			err := lim.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.name) {
				t.Fatalf("Validate() = %v, want error naming %s", err, tt.name)
			}
		})
	}
}

// TestReaderFrameStartHookFiresMidLine pins the first-byte contract:
// the hook fires when the frame's first byte is consumed from the
// stream, not when its first line completes. After writing a single
// byte of a still-incomplete line, the hook must fire without any
// further input.
func TestReaderFrameStartHookFiresMidLine(t *testing.T) {
	pr, pw := io.Pipe()
	r := NewReader(pr, testLimits())
	fired := make(chan struct{}, 1)
	r.SetFrameStartHook(func() {
		select {
		case fired <- struct{}{}:
		default:
			t.Error("frame-start hook fired more than once for one frame")
		}
	})
	msgCh := make(chan error, 1)
	go func() {
		_, err := r.ReadMessage()
		msgCh <- err
	}()
	if _, err := pw.Write([]byte("E")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("frame-start hook did not fire on the first byte of an incomplete line")
	}
	if _, err := pw.Write([]byte("vent: X\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := <-msgCh; err != nil {
		t.Fatalf("ReadMessage() = %v", err)
	}
}

func TestReaderFrameStartHook(t *testing.T) {
	r := newTestReader("banner\r\nEvent: A\r\nCount: 1\r\n\r\nEvent: B\r\n\r\n", testLimits())
	var fires int
	r.SetFrameStartHook(func() { fires++ })
	if _, err := r.ReadBanner(); err != nil || fires != 1 {
		t.Fatalf("after banner: fires = %d, err = %v, want exactly 1", fires, err)
	}
	// The second frame's first byte comes from the buffer, not the
	// stream; the hook must still fire, and only once per frame no
	// matter how many lines the frame has.
	if _, err := r.ReadMessage(); err != nil || fires != 2 {
		t.Fatalf("after first message: fires = %d, err = %v, want exactly 2", fires, err)
	}
	if _, err := r.ReadMessage(); err != nil || fires != 3 {
		t.Fatalf("after second message: fires = %d, err = %v, want exactly 3", fires, err)
	}
}

// TestReaderPaddingHook pins the pairing the connection layer's
// deadline relies on: every frame start is closed either by a returned
// message or by a padding report, and the padding report finds the
// reader clean.
func TestReaderPaddingHook(t *testing.T) {
	r := newTestReader("Event: A\r\n\r\n\r\n\nEvent: B\r\n\r\n", testLimits())
	var trace []string
	r.SetFrameStartHook(func() { trace = append(trace, "start") })
	r.SetPaddingHook(func() {
		if r.Dirty() {
			t.Error("padding hook fired on a dirty reader")
		}
		trace = append(trace, "padding")
	})
	if _, err := r.ReadMessage(); err != nil {
		t.Fatalf("first message: %v", err)
	}
	if want := []string{"start"}; !slices.Equal(trace, want) {
		t.Fatalf("after first message: trace = %v, want %v", trace, want)
	}
	// Both blank lines are already buffered; each one starts a frame
	// that turns out to be padding before the real frame starts.
	if _, err := r.ReadMessage(); err != nil {
		t.Fatalf("second message: %v", err)
	}
	want := []string{"start", "start", "padding", "start", "padding", "start"}
	if !slices.Equal(trace, want) {
		t.Fatalf("after second message: trace = %v, want %v", trace, want)
	}
}
