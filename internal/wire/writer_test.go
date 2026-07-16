package wire

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestAppendMessageExactWire(t *testing.T) {
	fields := []Field{
		{Key: "Action", Value: "Ping"},
		{Key: "ActionID", Value: "7"},
	}
	got, err := AppendMessage(nil, fields, testLimits())
	if err != nil {
		t.Fatalf("AppendMessage() error = %v", err)
	}
	if want := "Action: Ping\r\nActionID: 7\r\n\r\n"; string(got) != want {
		t.Fatalf("AppendMessage() = %q, want %q", got, want)
	}
}

func TestAppendMessageAppends(t *testing.T) {
	prefix := []byte("existing")
	got, err := AppendMessage(prefix, []Field{{Key: "Action", Value: "Ping"}}, testLimits())
	if err != nil {
		t.Fatalf("AppendMessage() error = %v", err)
	}
	if want := "existingAction: Ping\r\n\r\n"; string(got) != want {
		t.Fatalf("AppendMessage() = %q, want %q", got, want)
	}
}

func TestAppendMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		fields []Field
	}{
		{"login action", []Field{
			{Key: "Action", Value: "Login"},
			{Key: "Username", Value: "synthetic"},
			{Key: "Secret", Value: "synthetic"},
			{Key: "ActionID", Value: "prefix-1"},
		}},
		{"empty value", []Field{
			{Key: "Action", Value: "Events"},
			{Key: "EventMask", Value: ""},
		}},
		{"value with leading space", []Field{
			{Key: "K", Value: " leading"},
		}},
		{"value with colon and unicode", []Field{
			{Key: "AppData", Value: "Dial(PJSIP/köprü,30):x"},
		}},
		{"duplicate keys in order", []Field{
			{Key: "Action", Value: "Originate"},
			{Key: "Variable", Value: "a=1"},
			{Key: "Variable", Value: "b=2"},
		}},
		{"digit-bearing key", []Field{
			{Key: "Header2", Value: "v"},
		}},
		{"output-keyed fields survive", []Field{
			{Key: "Response", Value: "Success"},
			{Key: "Output", Value: "row"},
			{Key: "output", Value: ""},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc, err := AppendMessage(nil, tt.fields, testLimits())
			if err != nil {
				t.Fatalf("AppendMessage() error = %v", err)
			}
			got, err := NewReader(bytes.NewReader(enc), testLimits()).ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage() error = %v on %q", err, enc)
			}
			if !slices.Equal(got, tt.fields) {
				t.Fatalf("round trip = %v, want %v (wire %q)", got, tt.fields, enc)
			}
		})
	}
}

func TestAppendMessageValidation(t *testing.T) {
	tests := []struct {
		name    string
		fields  []Field
		wantErr error
	}{
		{"no fields", nil, ErrEmptyMessage},
		{"empty key", []Field{{Key: "", Value: "v"}}, ErrInvalidKey},
		{"colon in key", []Field{{Key: "A:B", Value: "v"}}, ErrInvalidKey},
		{"carriage return in key", []Field{{Key: "A\rB", Value: "v"}}, ErrInvalidKey},
		{"line feed in key", []Field{{Key: "A\nB", Value: "v"}}, ErrInvalidKey},
		{"carriage return in value", []Field{{Key: "K", Value: "a\rb"}}, ErrInvalidValue},
		{"line feed in value", []Field{{Key: "K", Value: "a\nb"}}, ErrInvalidValue},
		{
			"header injection in value",
			[]Field{{Key: "Action", Value: "Login\r\nInjected: x"}},
			ErrInvalidValue,
		},
		{
			"later field invalid",
			[]Field{{Key: "Action", Value: "Ping"}, {Key: "Bad\n", Value: "v"}},
			ErrInvalidKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := []byte("prefix")
			got, err := AppendMessage(prefix, tt.fields, testLimits())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("AppendMessage() error = %v, want %v", err, tt.wantErr)
			}
			if string(got) != "prefix" {
				t.Fatalf("dst modified on error: %q", got)
			}
		})
	}
}

func TestAppendMessageLimits(t *testing.T) {
	tests := []struct {
		name    string
		adjust  func(*Limits)
		fields  []Field
		wantErr error // nil means encoding must succeed
	}{
		{
			"fields at exact limit",
			func(l *Limits) { l.MaxActionFields = 2 },
			[]Field{{Key: "A", Value: "1"}, {Key: "B", Value: "2"}},
			nil,
		},
		{
			"fields one past limit",
			func(l *Limits) { l.MaxActionFields = 2 },
			[]Field{{Key: "A", Value: "1"}, {Key: "B", Value: "2"}, {Key: "C", Value: "3"}},
			ErrTooManyActionFields,
		},
		{
			"line at exact limit",
			func(l *Limits) { l.MaxActionLineBytes = 7 },
			[]Field{{Key: "AB", Value: "CDE"}}, // "AB: CDE" is exactly 7 bytes
			nil,
		},
		{
			"line one past limit",
			func(l *Limits) { l.MaxActionLineBytes = 6 },
			[]Field{{Key: "AB", Value: "CDE"}},
			ErrActionLineTooLong,
		},
		{
			"total at exact limit",
			func(l *Limits) { l.MaxActionBytes = 8 },
			[]Field{{Key: "A", Value: "1"}}, // "A: 1\r\n\r\n" is exactly 8 bytes
			nil,
		},
		{
			"total one past limit",
			func(l *Limits) { l.MaxActionBytes = 7 },
			[]Field{{Key: "A", Value: "1"}},
			ErrActionTooLarge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lim := testLimits()
			tt.adjust(&lim)
			got, err := AppendMessage(nil, tt.fields, lim)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("AppendMessage() error = %v, want success", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("AppendMessage() error = %v, want %v", err, tt.wantErr)
			}
			if len(got) != 0 {
				t.Fatalf("dst modified on error: %q", got)
			}
		})
	}
}

// TestAppendMessageFollowsCaveat pins the documented wire ambiguity: an
// encoded message whose first field is "Response: Follows" re-parses
// under the legacy Command framing, not as the encoded field sequence.
func TestAppendMessageFollowsCaveat(t *testing.T) {
	fields := []Field{
		{Key: "Response", Value: "Follows"},
		{Key: "Note", Value: "not a header on re-parse"},
	}
	enc, err := AppendMessage(nil, fields, testLimits())
	if err != nil {
		t.Fatalf("AppendMessage() error = %v", err)
	}
	if _, err := NewReader(bytes.NewReader(enc), testLimits()).ReadMessage(); err == nil {
		t.Fatal("re-parse succeeded; the legacy frame lacks --END COMMAND--, so it must fail")
	}
	raw := string(enc)
	raw = strings.TrimSuffix(raw, "\r\n") + "--END COMMAND--\r\n\r\n"
	got, err := NewReader(strings.NewReader(raw), testLimits()).ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() on completed frame error = %v", err)
	}
	want := []Field{
		{Key: "Response", Value: "Follows"},
		{Key: "Output", Value: "Note: not a header on re-parse"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("re-parse = %v, want %v", got, want)
	}
}
