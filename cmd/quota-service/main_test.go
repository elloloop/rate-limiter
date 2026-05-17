package main

import (
	"path/filepath"
	"testing"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

func TestValidateLimitsCommandAcceptsExamples(t *testing.T) {
	examples := []string{
		"workspace-email.yaml",
		"assistant-llm.yaml",
		"assistant-concurrency.yaml",
	}
	for _, example := range examples {
		t.Run(example, func(t *testing.T) {
			path := filepath.Join("..", "..", "examples", "limits", example)
			if err := validateLimits(path); err != nil {
				t.Fatalf("validateLimits(%s): %v", example, err)
			}
		})
	}
}

func TestParsersRejectUnknownValues(t *testing.T) {
	if got := parseAlgorithm("ALGORITHM_DOES_NOT_EXIST"); got != quotav1.Algorithm_ALGORITHM_UNSPECIFIED {
		t.Fatalf("unexpected algorithm parse result: %s", got)
	}
	if got := parseWindowType("WINDOW_TYPE_DOES_NOT_EXIST"); got != quotav1.WindowType_WINDOW_TYPE_UNSPECIFIED {
		t.Fatalf("unexpected window type parse result: %s", got)
	}
	if got := parseCalendarUnit("CALENDAR_UNIT_DOES_NOT_EXIST"); got != quotav1.CalendarUnit_CALENDAR_UNIT_UNSPECIFIED {
		t.Fatalf("unexpected calendar unit parse result: %s", got)
	}
	if got := parseReservationExpiryPolicy("RESERVATION_EXPIRY_POLICY_DOES_NOT_EXIST"); got != quotav1.ReservationExpiryPolicy_RESERVATION_EXPIRY_POLICY_UNSPECIFIED {
		t.Fatalf("unexpected expiry policy parse result: %s", got)
	}
}
