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

func TestValidateRejectsEmptyLimitSet(t *testing.T) {
	errs, _ := Validate("assistant.llm.tokens", nil)
	assertFieldError(t, errs, "limits")
}

func TestValidateRejectsMissingRequiredLimitFields(t *testing.T) {
	errs, _ := Validate("assistant.llm.tokens", []*quotav1.Limit{{}})
	for _, field := range []string{"limit_id", "scope_key", "action", "unit", "limit", "algorithm"} {
		assertFieldError(t, errs, field)
	}
	for _, err := range errs {
		if err.GetLimitId() != "limits[0]" {
			t.Fatalf("missing-id errors should use index fallback id, got %q", err.GetLimitId())
		}
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

func TestValidateAlgorithmSpecificRules(t *testing.T) {
	tests := []struct {
		name      string
		limit     *quotav1.Limit
		wantErr   string
		wantWarn  string
		wantValid bool
	}{
		{
			name:      "calendar_requires_window",
			limit:     baseLimit(quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR, nil),
			wantErr:   "window",
			wantValid: false,
		},
		{
			name: "calendar_rejects_wrong_window_type",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR, &quotav1.Window{
				Type:         quotav1.WindowType_WINDOW_TYPE_DURATION,
				CalendarUnit: quotav1.CalendarUnit_CALENDAR_UNIT_DAY,
				Timezone:     "UTC",
			}),
			wantErr:   "window.type",
			wantValid: false,
		},
		{
			name: "calendar_rejects_unsupported_unit",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR, &quotav1.Window{
				Type:         quotav1.WindowType_WINDOW_TYPE_CALENDAR,
				CalendarUnit: quotav1.CalendarUnit_CALENDAR_UNIT_SECOND,
				Timezone:     "UTC",
			}),
			wantErr:   "window.calendar_unit",
			wantValid: false,
		},
		{
			name: "calendar_warns_on_bucket_count",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR, &quotav1.Window{
				Type:         quotav1.WindowType_WINDOW_TYPE_CALENDAR,
				CalendarUnit: quotav1.CalendarUnit_CALENDAR_UNIT_DAY,
				Timezone:     "UTC",
				BucketCount:  4,
			}),
			wantWarn:  "window.bucket_count",
			wantValid: true,
		},
		{
			name:      "duration_requires_window",
			limit:     baseLimit(quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION, nil),
			wantErr:   "window",
			wantValid: false,
		},
		{
			name: "duration_rejects_non_positive_duration",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION, &quotav1.Window{
				Type: quotav1.WindowType_WINDOW_TYPE_DURATION,
			}),
			wantErr:   "window.duration_ms",
			wantValid: false,
		},
		{
			name: "duration_rejects_wrong_window_type",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION, &quotav1.Window{
				Type:       quotav1.WindowType_WINDOW_TYPE_SLIDING,
				DurationMs: 60000,
			}),
			wantErr:   "window.type",
			wantValid: false,
		},
		{
			name:      "sliding_requires_window",
			limit:     baseLimit(quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW, nil),
			wantErr:   "window",
			wantValid: false,
		},
		{
			name: "sliding_rejects_wrong_window_type",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW, &quotav1.Window{
				Type:        quotav1.WindowType_WINDOW_TYPE_DURATION,
				DurationMs:  60000,
				BucketCount: 2,
			}),
			wantErr:   "window.type",
			wantValid: false,
		},
		{
			name: "sliding_rejects_non_positive_duration",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW, &quotav1.Window{
				Type:        quotav1.WindowType_WINDOW_TYPE_SLIDING,
				BucketCount: 2,
			}),
			wantErr:   "window.duration_ms",
			wantValid: false,
		},
		{
			name: "sliding_rejects_single_bucket",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW, &quotav1.Window{
				Type:        quotav1.WindowType_WINDOW_TYPE_SLIDING,
				DurationMs:  60000,
				BucketCount: 1,
			}),
			wantErr:   "window.bucket_count",
			wantValid: false,
		},
		{
			name: "continuous_rejects_wrong_window_type",
			limit: continuousLimit(quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET, &quotav1.Window{
				Type: quotav1.WindowType_WINDOW_TYPE_DURATION,
			}),
			wantErr:   "window.type",
			wantValid: false,
		},
		{
			name:      "continuous_warns_when_burst_defaults",
			limit:     continuousLimit(quotav1.Algorithm_ALGORITHM_GCRA, nil),
			wantWarn:  "burst",
			wantValid: true,
		},
		{
			name: "concurrency_warns_when_window_supplied",
			limit: baseLimit(quotav1.Algorithm_ALGORITHM_CONCURRENCY, &quotav1.Window{
				Type:       quotav1.WindowType_WINDOW_TYPE_DURATION,
				DurationMs: 1000,
			}),
			wantWarn:  "window",
			wantValid: true,
		},
		{
			name:      "unspecified_algorithm_rejected",
			limit:     baseLimit(quotav1.Algorithm_ALGORITHM_UNSPECIFIED, nil),
			wantErr:   "algorithm",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs, warnings := Validate(tt.limit.GetAction(), []*quotav1.Limit{tt.limit})
			if tt.wantValid && len(errs) != 0 {
				t.Fatalf("expected valid limit, got errors: %v", errs)
			}
			if !tt.wantValid && len(errs) == 0 {
				t.Fatalf("expected validation errors")
			}
			if tt.wantErr != "" {
				assertFieldError(t, errs, tt.wantErr)
			}
			if tt.wantWarn != "" {
				assertFieldWarning(t, warnings, tt.wantWarn)
			}
		})
	}
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

func assertFieldWarning(t *testing.T, warnings []*quotav1.ValidationWarning, field string) {
	t.Helper()
	for _, warning := range warnings {
		if warning.GetField() == field || strings.HasPrefix(warning.GetField(), field+".") {
			return
		}
	}
	t.Fatalf("expected warning for %q, got: %v", field, warnings)
}

func baseLimit(algorithm quotav1.Algorithm, window *quotav1.Window) *quotav1.Limit {
	return &quotav1.Limit{
		LimitId:   "limit",
		ScopeKey:  "scope:key",
		Action:    "test.action",
		Unit:      "requests",
		Algorithm: algorithm,
		Window:    window,
		Limit:     10,
	}
}

func continuousLimit(algorithm quotav1.Algorithm, window *quotav1.Window) *quotav1.Limit {
	limit := baseLimit(algorithm, window)
	limit.Unit = "tokens"
	limit.RefillRatePerSec = 10
	return limit
}
