package keys

import (
	"encoding/hex"
	"strings"
	"testing"
)

// FuzzHash asserts the key hash is always a stable 16-char lowercase hex
// string for any input — Redis key segments must never contain control
// characters or vary across calls.
func FuzzHash(f *testing.F) {
	f.Add("")
	f.Add("limit-123")
	f.Add("scope:user/42")
	f.Add(strings.Repeat("x", 4096))
	f.Fuzz(func(t *testing.T, value string) {
		got := Hash(value)
		if len(got) != 16 {
			t.Fatalf("Hash(%q) length = %d, want 16", value, len(got))
		}
		if _, err := hex.DecodeString(got); err != nil {
			t.Fatalf("Hash(%q) = %q is not valid hex: %v", value, got, err)
		}
		if got != Hash(value) {
			t.Fatalf("Hash(%q) is not deterministic", value)
		}
	})
}

// FuzzPrefix asserts the namespace prefix is well-formed for any operator
// supplied environment/product, including empty values that fall back to
// defaults.
func FuzzPrefix(f *testing.F) {
	f.Add("prod", "workspace")
	f.Add("", "")
	f.Add("staging", "assistant")
	f.Fuzz(func(t *testing.T, environment, product string) {
		got := Prefix(environment, product)
		if !strings.HasPrefix(got, "quota:v1:") {
			t.Fatalf("Prefix(%q,%q) = %q missing namespace prefix", environment, product, got)
		}
		if !strings.HasSuffix(got, ":") {
			t.Fatalf("Prefix(%q,%q) = %q must end with ':'", environment, product, got)
		}
	})
}
