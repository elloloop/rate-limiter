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

func TestFixedCalendarMinuteHourAndMonthWindows(t *testing.T) {
	tests := []struct {
		name        string
		unit        quotav1.CalendarUnit
		now         time.Time
		wantSuffix  string
		wantResetMS int64
	}{
		{
			name:        "minute",
			unit:        quotav1.CalendarUnit_CALENDAR_UNIT_MINUTE,
			now:         time.Date(2026, 5, 17, 12, 34, 56, 0, time.UTC),
			wantSuffix:  ":20260517T1234",
			wantResetMS: time.Date(2026, 5, 17, 12, 35, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name:        "hour",
			unit:        quotav1.CalendarUnit_CALENDAR_UNIT_HOUR,
			now:         time.Date(2026, 5, 17, 12, 34, 56, 0, time.UTC),
			wantSuffix:  ":20260517T12",
			wantResetMS: time.Date(2026, 5, 17, 13, 0, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name:        "month",
			unit:        quotav1.CalendarUnit_CALENDAR_UNIT_MONTH,
			now:         time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC),
			wantSuffix:  ":202602",
			wantResetMS: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, reset, ttl := FixedWindow(Prefix("prod", "workspace"), calendarLimit(tt.unit), tt.now)
			if !strings.HasSuffix(key, tt.wantSuffix) {
				t.Fatalf("window key = %q, want suffix %q", key, tt.wantSuffix)
			}
			if reset.UnixMilli() != tt.wantResetMS {
				t.Fatalf("reset = %d, want %d", reset.UnixMilli(), tt.wantResetMS)
			}
			if ttl <= time.Hour {
				t.Fatalf("ttl should extend past reset, got %s", ttl)
			}
		})
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

func TestSlidingBucketsDefaultBucketCountAndMinimumBucketSize(t *testing.T) {
	limit := &quotav1.Limit{
		LimitId:  "tiny",
		ScopeKey: "scope:tiny",
		Window: &quotav1.Window{
			Type:        quotav1.WindowType_WINDOW_TYPE_SLIDING,
			DurationMs:  5,
			BucketCount: 10,
		},
	}
	readKeys, writeKey, reset, ttl := SlidingBuckets(Prefix("local", "tiny"), limit, time.UnixMilli(12))

	if len(readKeys) != 5 {
		t.Fatalf("expected one key per millisecond in active window, got %d: %v", len(readKeys), readKeys)
	}
	if writeKey != readKeys[len(readKeys)-1] {
		t.Fatalf("current bucket should be write key")
	}
	if reset.UnixMilli() != 13 {
		t.Fatalf("reset mismatch: got %d want 13", reset.UnixMilli())
	}
	if ttl != time.Hour+6*time.Millisecond {
		t.Fatalf("ttl mismatch: got %s", ttl)
	}
}

func TestSlidingBucketsUsesDefaultCountWhenUnset(t *testing.T) {
	limit := &quotav1.Limit{
		LimitId:  "default_count",
		ScopeKey: "scope:default_count",
		Window: &quotav1.Window{
			Type:       quotav1.WindowType_WINDOW_TYPE_SLIDING,
			DurationMs: 1000,
		},
	}
	readKeys, _, _, _ := SlidingBuckets(Prefix("local", "defaults"), limit, time.UnixMilli(1000))
	if len(readKeys) != 10 {
		t.Fatalf("expected default 10 active buckets, got %d: %v", len(readKeys), readKeys)
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
