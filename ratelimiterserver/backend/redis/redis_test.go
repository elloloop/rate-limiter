package redis

import (
	"context"
	"strings"
	"testing"
)

func TestRequiredScriptsAreEmbedded(t *testing.T) {
	required := []string{
		"consume.lua",
		"reserve.lua",
		"increment_reservation.lua",
		"finalize_reservation.lua",
		"release_reservation.lua",
		"expire_reservations.lua",
		"acquire_lease.lua",
		"renew_lease.lua",
		"release_lease.lua",
	}

	for _, name := range required {
		body, err := scriptFS.ReadFile("scripts/" + name)
		if err != nil {
			t.Fatalf("missing script %s: %v", name, err)
		}
		if !strings.Contains(string(body), "redis.call") {
			t.Fatalf("script %s does not appear to call Redis", name)
		}
	}
}

func TestBoolArg(t *testing.T) {
	if got := boolArg(true); got != "1" {
		t.Fatalf("true encoded as %q", got)
	}
	if got := boolArg(false); got != "0" {
		t.Fatalf("false encoded as %q", got)
	}
}

func TestNewRejectsInvalidRedisURL(t *testing.T) {
	_, err := New(context.Background(), "://not-a-url")
	if err == nil {
		t.Fatal("expected invalid redis URL error")
	}
}

func TestNewRejectsUnreachableRedis(t *testing.T) {
	// Port 1 is reserved (tcpmux) and effectively never accepts
	// connections, so this exercises the synchronous ping at
	// construction without depending on test orchestration.
	_, err := New(context.Background(), "redis://127.0.0.1:1/0")
	if err == nil {
		t.Fatal("expected unreachable Redis to fail New")
	}
	if !strings.Contains(err.Error(), "ping redis") {
		t.Fatalf("expected ping error, got: %v", err)
	}
}
