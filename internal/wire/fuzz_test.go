package wire

import (
	"bytes"
	"slices"
	"strings"
	"testing"
)

// FuzzReadMessage feeds the parser arbitrary streams. Whatever the input,
// the parser must terminate without panicking, and every successfully
// parsed message must uphold the field invariants: at least one field,
// non-empty keys, no colon or line feed inside a key, and no line feed
// inside a value.
func FuzzReadMessage(f *testing.F) {
	seeds := []string{
		"Event: Newchannel\r\nChannel: PJSIP/synthetic-0001\r\nVariable: a=1\r\nVariable: b=2\r\n\r\n",
		"Response: Success\r\nActionID: 7\r\nOutput: one\r\nOutput: two\r\n\r\n",
		"Response: Follows\r\nPrivilege: Command\r\nActionID: 1\r\nrow one\nName/Context: default\n\n--END COMMAND--\r\n\r\n",
		"Response: Follows\r\nglued--END COMMAND--\r\n\r\n",
		"Response: Follows\r\n--END COMMAND--\r\nnot blank\r\n\r\n",
		"A: 1\nB: 2\n\nEvent: Second\n\n",
		"K:no-space\r\n\r\n",
		"K:  double-space\r\n\r\n",
		"Empty:\r\n\r\n",
		": empty key\r\n\r\n",
		"no colon at all\r\n\r\n",
		"\r\n",
		"Event: truncated",
		"Bin: \xff\xfe\x00\x1b\r\n\r\n",
		strings.Repeat("A", 300) + ": long line\r\n\r\n",
		"Response: Follows\r\n" + strings.Repeat("x\n", 80) + "--END COMMAND--\r\n\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data), testLimits())
		for {
			fields, err := r.ReadMessage()
			if err != nil {
				return // any terminating error is acceptable; panics and hangs are not
			}
			if len(fields) == 0 {
				t.Fatal("successful read returned no fields")
			}
			for _, fd := range fields {
				if fd.Key == "" {
					t.Fatalf("empty key in %v", fields)
				}
				if strings.ContainsAny(fd.Key, ":\n") {
					t.Fatalf("colon or line feed leaked into key %q", fd.Key)
				}
				if strings.Contains(fd.Value, "\n") {
					t.Fatalf("line feed leaked into value %q", fd.Value)
				}
			}
		}
	})
}

// FuzzRoundTrip pins the encoder/parser contract: every field sequence
// the encoder accepts must re-parse to exactly the same sequence. The
// single documented exception is a first field of "Response: Follows",
// which re-parses under the legacy Command framing by design.
func FuzzRoundTrip(f *testing.F) {
	f.Add("Action", "Ping", "ActionID", "7")
	f.Add("Action", "Login", "Secret", "")
	f.Add("K", " leading space", "Variable", "a=b=c")
	f.Add("Variable", "a=1", "Variable", "b=2")
	f.Add("Output", "row", "output", "")
	f.Add("AppData", "a:b:c", "Header2", "\xff\xfe")
	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 string) {
		fields := []Field{
			{Key: k1, Value: v1},
			{Key: k2, Value: v2},
		}
		enc, err := AppendMessage(nil, fields, testLimits())
		if err != nil {
			t.Skip() // the encoder rejected the input; nothing to round-trip
		}
		if strings.EqualFold(k1, "Response") && strings.EqualFold(v1, "Follows") {
			t.Skip() // documented ambiguity: re-parses as a legacy command frame
		}
		got, err := NewReader(bytes.NewReader(enc), testLimits()).ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage() error = %v on encoder output %q", err, enc)
		}
		if !slices.Equal(got, fields) {
			t.Fatalf("round trip = %v, want %v (wire %q)", got, fields, enc)
		}
	})
}
