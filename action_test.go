package ami

import (
	"slices"
	"strings"
	"testing"
)

func TestNewAction(t *testing.T) {
	fields := []Field{
		{Key: "Channel", Value: "PJSIP/synthetic-0001"},
		{Key: "Variable", Value: "a=1"},
		{Key: "Variable", Value: "b=2"},
		{Key: "Empty", Value: ""},
	}
	a, err := NewAction("Originate", fields...)
	if err != nil {
		t.Fatalf("NewAction() error = %v", err)
	}
	if a.Name() != "Originate" {
		t.Fatalf("Name() = %q, want %q", a.Name(), "Originate")
	}
	var got []Field
	for k, v := range a.Fields() {
		got = append(got, Field{Key: k, Value: v})
	}
	if !slices.Equal(got, fields) {
		t.Fatalf("Fields() = %v, want %v", got, fields)
	}
}

func TestNewActionRejects(t *testing.T) {
	tests := []struct {
		name    string
		action  string
		fields  []Field
		wantHas string
	}{
		{"empty name", "", nil, "empty name"},
		{"name with cr", "Ping\r", nil, "NUL, CR, or LF"},
		{"name with lf", "Ping\nInjected: x", nil, "NUL, CR, or LF"},
		{"name with nul", "Originate\x00Redirect", nil, "NUL, CR, or LF"},
		{"empty key", "Ping", []Field{{Key: "", Value: "v"}}, "empty key"},
		{"colon in key", "Ping", []Field{{Key: "A:B", Value: "v"}}, "colon"},
		{"cr in key", "Ping", []Field{{Key: "A\rB", Value: "v"}}, "colon, NUL, CR, or LF"},
		{"lf in key", "Ping", []Field{{Key: "A\nB", Value: "v"}}, "colon, NUL, CR, or LF"},
		{"nul in key", "Ping", []Field{{Key: "A\x00B", Value: "v"}}, "colon, NUL, CR, or LF"},
		{"reserved action key", "Ping", []Field{{Key: "action", Value: "x"}}, "reserved"},
		{"reserved actionid key", "Ping", []Field{{Key: "ACTIONID", Value: "x"}}, "reserved"},
		{"cr in value", "Ping", []Field{{Key: "K", Value: "a\rb"}}, "NUL, CR, or LF"},
		{"lf injection in value", "Ping", []Field{{Key: "K", Value: "v\r\nEvents: on"}}, "NUL, CR, or LF"},
		{"nul truncation in value", "Ping", []Field{{Key: "Channel", Value: "PJSIP/a\x00, junk"}}, "NUL, CR, or LF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewAction(tt.action, tt.fields...)
			if err == nil || !strings.Contains(err.Error(), tt.wantHas) {
				t.Fatalf("NewAction() error = %v, want containing %q", err, tt.wantHas)
			}
		})
	}
}

func TestNewActionCopiesFields(t *testing.T) {
	in := []Field{{Key: "Channel", Value: "original"}}
	a, err := NewAction("Status", in...)
	if err != nil {
		t.Fatalf("NewAction() error = %v", err)
	}
	in[0] = Field{Key: "Channel", Value: "mutated"}
	for _, v := range a.Fields() {
		if v != "original" {
			t.Fatalf("mutating the input slice changed the action: value %q", v)
		}
	}
}

func TestActionZeroValue(t *testing.T) {
	var a Action
	if a.Name() != "" {
		t.Fatalf("zero Action Name() = %q, want empty", a.Name())
	}
	for range a.Fields() {
		t.Fatal("zero Action yielded a field")
	}
}
