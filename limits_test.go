package ami

import (
	"strings"
	"testing"
	"time"
)

func TestWireLimitsZeroSelectsDefaults(t *testing.T) {
	got, age, err := WireLimits{}.resolve()
	if err != nil {
		t.Fatalf("resolve() error = %v", err)
	}
	if age != 30*time.Second {
		t.Errorf("default MaxPartialFrameAge = %v, want 30s", age)
	}
	tests := []struct {
		name string
		got  int
		want int
	}{
		{"MaxBannerBytes", got.MaxBannerBytes, 1024},
		{"MaxLineBytes", got.MaxLineBytes, 32768},
		{"MaxFields", got.MaxFields, 1024},
		{"MaxMessageBytes", got.MaxMessageBytes, 131072},
		{"MaxCommandOutputLines", got.MaxCommandOutputLines, 65536},
		{"MaxCommandOutputBytes", got.MaxCommandOutputBytes, 8388608},
		{"MaxActionFields", got.MaxActionFields, 128},
		{"MaxActionLineBytes", got.MaxActionLineBytes, 1022},
		{"MaxActionBytes", got.MaxActionBytes, 131072},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("default %s = %d, want %d", tt.name, tt.got, tt.want)
		}
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("resolved defaults fail wire validation: %v", err)
	}
}

func TestWireLimitsExplicitAndPartial(t *testing.T) {
	got, age, err := WireLimits{MaxLineBytes: 512, MaxActionFields: 16, MaxPartialFrameAge: 5 * time.Second}.resolve()
	if err != nil {
		t.Fatalf("resolve() error = %v", err)
	}
	if got.MaxLineBytes != 512 || got.MaxActionFields != 16 {
		t.Fatalf("explicit fields not honored: line %d, action fields %d", got.MaxLineBytes, got.MaxActionFields)
	}
	if age != 5*time.Second {
		t.Fatalf("explicit MaxPartialFrameAge not honored: %v", age)
	}
	if got.MaxBannerBytes != 1024 || got.MaxMessageBytes != 131072 {
		t.Fatalf("unset fields lost their defaults: banner %d, message %d", got.MaxBannerBytes, got.MaxMessageBytes)
	}
}

func TestWireLimitsNegativeRejected(t *testing.T) {
	tests := []struct {
		name string
		lim  WireLimits
	}{
		{"MaxBannerBytes", WireLimits{MaxBannerBytes: -1}},
		{"MaxLineBytes", WireLimits{MaxLineBytes: -1}},
		{"MaxFields", WireLimits{MaxFields: -1}},
		{"MaxMessageBytes", WireLimits{MaxMessageBytes: -1}},
		{"MaxCommandOutputLines", WireLimits{MaxCommandOutputLines: -1}},
		{"MaxCommandOutputBytes", WireLimits{MaxCommandOutputBytes: -1}},
		{"MaxActionFields", WireLimits{MaxActionFields: -1}},
		{"MaxActionLineBytes", WireLimits{MaxActionLineBytes: -1}},
		{"MaxActionBytes", WireLimits{MaxActionBytes: -1}},
		{"MaxPartialFrameAge", WireLimits{MaxPartialFrameAge: -time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := tt.lim.resolve()
			if err == nil || !strings.Contains(err.Error(), tt.name) {
				t.Fatalf("resolve() = %v, want error naming %s", err, tt.name)
			}
		})
	}
}
