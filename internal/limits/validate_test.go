package limits

import (
	"strings"
	"testing"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

func TestValidateAcceptsFixedCalendarDay(t *testing.T) {
	errs, warnings := Validate("assistant.llm.tokens", []*quotav1.Limit{{
		LimitId:   "user_daily_tokens",
		ScopeKey:  "user:user_123",
		Action:    "assistant.llm.tokens",
		Unit:      "tokens",
		Algorithm: quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR,
		Window: &quotav1.Window{
			Type:         quotav1.WindowType_WINDOW_TYPE_CALENDAR,
			CalendarUnit: quotav1.CalendarUnit_CALENDAR_UNIT_DAY,
			Timezone:     "UTC",
		},
		Limit: 500000,
	}})
	if len(errs) != 0 {
		t.Fatalf("expected valid limit, got errors: %v", errs)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got: %v", warnings)
	}
}

func TestValidateRejectsBusinessMismatchedAction(t *testing.T) {
	errs, _ := Validate("workspace.email.recipients", []*quotav1.Limit{{
		LimitId:   "user_daily_tokens",
		ScopeKey:  "user:user_123",
		Action:    "assistant.llm.tokens",
		Unit:      "tokens",
		Algorithm: quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR,
		Window: &quotav1.Window{
			Type:         quotav1.WindowType_WINDOW_TYPE_CALENDAR,
			CalendarUnit: quotav1.CalendarUnit_CALENDAR_UNIT_DAY,
			Timezone:     "UTC",
		},
		Limit: 500000,
	}})
	assertFieldError(t, errs, "action")
}

func TestValidateRejectsNonUTCCalendarWindow(t *testing.T) {
	errs, _ := Validate("workspace.email.recipients", []*quotav1.Limit{{
		LimitId:   "user_email_daily",
		ScopeKey:  "user:user_123",
		Action:    "workspace.email.recipients",
		Unit:      "recipients",
		Algorithm: quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR,
		Window: &quotav1.Window{
			Type:         quotav1.WindowType_WINDOW_TYPE_CALENDAR,
			CalendarUnit: quotav1.CalendarUnit_CALENDAR_UNIT_DAY,
			Timezone:     "America/Los_Angeles",
		},
		Limit: 500,
	}})
	assertFieldError(t, errs, "window.timezone")
}

func TestValidateSlidingWindowDefaultsBucketCountWarning(t *testing.T) {
	errs, warnings := Validate("education.download.bytes", []*quotav1.Limit{{
		LimitId:   "download_bytes",
		ScopeKey:  "user:user_123",
		Action:    "education.download.bytes",
		Unit:      "bytes",
		Algorithm: quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW,
		Window: &quotav1.Window{
			Type:       quotav1.WindowType_WINDOW_TYPE_SLIDING,
			DurationMs: 60000,
		},
		Limit: 2147483648,
	}})
	if len(errs) != 0 {
		t.Fatalf("expected valid sliding window, got: %v", errs)
	}
	if len(warnings) != 1 || warnings[0].GetField() != "window.bucket_count" {
		t.Fatalf("expected bucket_count warning, got: %v", warnings)
	}
}

func TestValidateContinuousLimitRequiresRefillRate(t *testing.T) {
	errs, _ := Validate("assistant.llm.tokens", []*quotav1.Limit{{
		LimitId:   "openai_tpm",
		ScopeKey:  "provider:openai",
		Action:    "assistant.llm.tokens",
		Unit:      "tokens",
		Algorithm: quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET,
		Window: &quotav1.Window{
			Type:       quotav1.WindowType_WINDOW_TYPE_CONTINUOUS,
			DurationMs: 60000,
		},
		Limit: 500000,
		Burst: 500000,
	}})
	assertFieldError(t, errs, "refill_rate_per_sec")
}

func TestValidateDetectsDuplicateLimitIDs(t *testing.T) {
	limit := &quotav1.Limit{
		LimitId:   "dup",
		ScopeKey:  "user:user_123",
		Action:    "assistant.image.generate",
		Unit:      "requests",
		Algorithm: quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION,
		Window: &quotav1.Window{
			Type:       quotav1.WindowType_WINDOW_TYPE_DURATION,
			DurationMs: 60000,
		},
		Limit: 10,
	}
	errs, _ := Validate("assistant.image.generate", []*quotav1.Limit{limit, limit})
	assertFieldError(t, errs, "limit_id")
}

func assertFieldError(t *testing.T, errs []*quotav1.ValidationError, field string) {
	t.Helper()
	for _, err := range errs {
		if err.GetField() == field || strings.HasPrefix(err.GetField(), field+".") {
			return
		}
	}
	t.Fatalf("expected error for %q, got: %v", field, errs)
}
