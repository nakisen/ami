package ami

import (
	"slices"
	"testing"
)

// sampleFields covers the shapes the design pins down: repeated keys,
// meaningful empty values, and header names containing digits.
func sampleFields() []Field {
	return []Field{
		{Key: "Event", Value: "Newchannel"},
		{Key: "Variable", Value: "a=1"},
		{Key: "ChanVariable", Value: "x=9"},
		{Key: "Variable", Value: "b=2"},
		{Key: "Empty", Value: ""},
		{Key: "Header2", Value: "digit-key"},
	}
}

func TestMessageLookupAndGet(t *testing.T) {
	m := newMessage(sampleFields())
	tests := []struct {
		name   string
		key    string
		want   string
		wantOK bool
	}{
		{"first value of repeated key", "Variable", "a=1", true},
		{"case-insensitive match", "vArIaBlE", "a=1", true},
		{"present but empty value", "Empty", "", true},
		{"digit-bearing header name", "header2", "digit-key", true},
		{"absent key", "Missing", "", false},
		{"no partial key match", "Var", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := m.Lookup(tt.key)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("Lookup(%q) = (%q, %v), want (%q, %v)", tt.key, got, ok, tt.want, tt.wantOK)
			}
			if got := m.Get(tt.key); got != tt.want {
				t.Fatalf("Get(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestMessageValues(t *testing.T) {
	m := newMessage(sampleFields())
	tests := []struct {
		name string
		key  string
		want []string
	}{
		{"repeated key in wire order", "variable", []string{"a=1", "b=2"}},
		{"single occurrence", "Event", []string{"Newchannel"}},
		{"present but empty value", "Empty", []string{""}},
		{"absent key is nil", "Missing", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := m.Values(tt.key); !slices.Equal(got, tt.want) {
				t.Fatalf("Values(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestMessageValuesReturnsCopy(t *testing.T) {
	m := newMessage(sampleFields())
	got := m.Values("Variable")
	got[0] = "mutated"
	if again := m.Values("Variable"); again[0] != "a=1" {
		t.Fatalf("mutating the returned slice changed the message: Values = %q", again)
	}
}

func TestMessageFieldsOrder(t *testing.T) {
	in := sampleFields()
	m := newMessage(in)
	var got []Field
	for k, v := range m.Fields() {
		got = append(got, Field{Key: k, Value: v})
	}
	if !slices.Equal(got, in) {
		t.Fatalf("Fields() yielded %v, want wire order %v", got, in)
	}
}

func TestMessageFieldsEarlyBreak(t *testing.T) {
	m := newMessage(sampleFields())
	n := 0
	for range m.Fields() {
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		t.Fatalf("iterated %d fields after break at 2", n)
	}
}

func TestMessageImmutableAfterConstruction(t *testing.T) {
	in := sampleFields()
	m := newMessage(in)
	in[0] = Field{Key: "Event", Value: "changed"}
	if got := m.Get("Event"); got != "Newchannel" {
		t.Fatalf("mutating the input slice changed the message: Get(Event) = %q", got)
	}
}

func TestZeroAndEmptyMessage(t *testing.T) {
	for _, m := range []Message{{}, newMessage(nil), newMessage([]Field{})} {
		if v, ok := m.Lookup("any"); v != "" || ok {
			t.Fatalf("Lookup on empty message = (%q, %v), want (\"\", false)", v, ok)
		}
		if got := m.Get("any"); got != "" {
			t.Fatalf("Get on empty message = %q, want \"\"", got)
		}
		if got := m.Values("any"); got != nil {
			t.Fatalf("Values on empty message = %v, want nil", got)
		}
		for range m.Fields() {
			t.Fatal("empty message yielded a field")
		}
	}
}
