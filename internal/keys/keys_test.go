package keys

import (
	"strings"
	"testing"
	"time"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

func TestPrefixDefaultsAndExplicitValues(t *testing.T) {
	if got := Prefix("", ""); got != "quota:v1:local:default:" {
		t.Fatalf("unexpected default prefix: %q", got)
	}
	if got := Prefix("prod", "assistant"); got != "quota:v1:prod:assistant:" {
		t.Fatalf("unexpected explicit prefix: %q", got)
	}
}

func TestFixedCalendarDayWindowUsesUTCBoundary(t *testing.T) {
	limit := calendarLimit(quotav1.CalendarUnit_CALENDAR_UNIT_DAY)
	now := time.Date(2026, 5, 17, 23, 59, 30, 0, time.UTC)
	key, reset, ttl := FixedWindow(Prefix("prod", "workspace"), limit, now)

	if !strings.Contains(key, "quota:v1:prod:workspace:fw:") {
		t.Fatalf("unexpected key prefix: %q", key)
	}
	if !strings.HasSuffix(key, ":20260517") {
		t.Fatalf("expected day window id, got %q", key)
	}
	wantReset := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)
	if !reset.Equal(wantReset) {
		t.Fatalf("reset mismatch: got %s want %s", reset, wantReset)
	}
	if ttl <= time.Hour {
		t.Fatalf("ttl should extend past reset, got %s", ttl)
	}
}

func TestFixedCalendarWeekUsesISOWeek(t *testing.T) {
	limit := calendarLimit(quotav1.CalendarUnit_CALENDAR_UNIT_WEEK)
	now := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	key, reset, _ := FixedWindow(Prefix("prod", "workspace"), limit, now)

	if !strings.HasSuffix(key, ":2026W20") {
		t.Fatalf("expected ISO week id, got %q", key)
	}
	wantReset := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)
	if !reset.Equal(wantReset) {
		t.Fatalf("reset mismatch: got %s want %s", reset, wantReset)
	}
}

func TestDurationWindowUsesEpochBucket(t *testing.T) {
	limit := &quotav1.Limit{
		LimitId:  "per_minute",
		ScopeKey: "provider:openai",
		Window: &quotav1.Window{
			Type:       quotav1.WindowType_WINDOW_TYPE_DURATION,
			DurationMs: 60000,
		},
	}
	now := time.UnixMilli(180000)
	key, reset, ttl := DurationWindow(Prefix("prod", "assistant"), limit, now)

	if !strings.HasSuffix(key, ":3") {
		t.Fatalf("expected bucket 3, got %q", key)
	}
	if got := reset.UnixMilli(); got != 240000 {
		t.Fatalf("reset mismatch: got %d want 240000", got)
	}
	if ttl != time.Minute+time.Hour {
		t.Fatalf("ttl mismatch: got %s", ttl)
	}
}

func TestSlidingBucketsIncludeActiveWindowAndCurrentWriteKey(t *testing.T) {
	limit := &quotav1.Limit{
		LimitId:  "rolling",
		ScopeKey: "user:user_123",
		Window: &quotav1.Window{
			Type:        quotav1.WindowType_WINDOW_TYPE_SLIDING,
			DurationMs:  10000,
			BucketCount: 5,
		},
	}
	readKeys, writeKey, reset, ttl := SlidingBuckets(Prefix("local", "education"), limit, time.UnixMilli(22000))

	if len(readKeys) != 5 {
		t.Fatalf("expected 5 active buckets, got %d: %v", len(readKeys), readKeys)
	}
	if writeKey != readKeys[len(readKeys)-1] {
		t.Fatalf("current bucket should be write key")
	}
	if reset.UnixMilli() != 24000 {
		t.Fatalf("reset mismatch: got %d want 24000", reset.UnixMilli())
	}
	if ttl <= 10*time.Second {
		t.Fatalf("ttl should exceed sliding duration, got %s", ttl)
	}
}

func TestStableHashIsShortAndDeterministic(t *testing.T) {
	a := Hash("user:user_123")
	b := Hash("user:user_123")
	c := Hash("user:user_456")
	if a != b {
		t.Fatalf("hash must be deterministic")
	}
	if a == c {
		t.Fatalf("different inputs should not collide in this test")
	}
	if len(a) != 16 {
		t.Fatalf("expected 16 hex chars, got %q", a)
	}
}

func calendarLimit(unit quotav1.CalendarUnit) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:  "daily",
		ScopeKey: "user:user_123",
		Window: &quotav1.Window{
			Type:         quotav1.WindowType_WINDOW_TYPE_CALENDAR,
			CalendarUnit: unit,
			Timezone:     "UTC",
		},
	}
}
