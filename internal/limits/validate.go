package limits

import (
	"fmt"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

func Validate(action string, supplied []*quotav1.Limit) ([]*quotav1.ValidationError, []*quotav1.ValidationWarning) {
	var errs []*quotav1.ValidationError
	var warnings []*quotav1.ValidationWarning
	seen := map[string]struct{}{}

	if len(supplied) == 0 {
		return []*quotav1.ValidationError{{
			Field:   "limits",
			Message: "at least one limit is required",
		}}, warnings
	}

	for i, limit := range supplied {
		id := limit.GetLimitId()
		if id == "" {
			id = fmt.Sprintf("limits[%d]", i)
		}
		addErr := func(field, msg string) {
			errs = append(errs, &quotav1.ValidationError{LimitId: id, Field: field, Message: msg})
		}
		addWarn := func(field, msg string) {
			warnings = append(warnings, &quotav1.ValidationWarning{LimitId: id, Field: field, Message: msg})
		}

		if limit.GetLimitId() == "" {
			addErr("limit_id", "required")
		} else if _, ok := seen[limit.GetLimitId()]; ok {
			addErr("limit_id", "duplicate within request")
		} else {
			seen[limit.GetLimitId()] = struct{}{}
		}
		if limit.GetScopeKey() == "" {
			addErr("scope_key", "required")
		}
		if limit.GetAction() == "" {
			addErr("action", "required")
		} else if action != "" && limit.GetAction() != action {
			addErr("action", "must match request action")
		}
		if limit.GetUnit() == "" {
			addErr("unit", "required")
		}
		if limit.GetLimit() <= 0 {
			addErr("limit", "must be greater than zero")
		}

		switch limit.GetAlgorithm() {
		case quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_CALENDAR:
			validateCalendarWindow(limit, addErr, addWarn)
		case quotav1.Algorithm_ALGORITHM_FIXED_WINDOW_DURATION:
			validateDurationWindow(limit, addErr)
		case quotav1.Algorithm_ALGORITHM_SLIDING_WINDOW:
			validateSlidingWindow(limit, addErr, addWarn)
		case quotav1.Algorithm_ALGORITHM_TOKEN_BUCKET,
			quotav1.Algorithm_ALGORITHM_LEAKY_BUCKET,
			quotav1.Algorithm_ALGORITHM_GCRA:
			validateContinuous(limit, addErr, addWarn)
		case quotav1.Algorithm_ALGORITHM_CONCURRENCY:
			if limit.GetWindow() != nil && limit.GetWindow().GetType() != quotav1.WindowType_WINDOW_TYPE_UNSPECIFIED {
				addWarn("window", "concurrency limits ignore window settings")
			}
		default:
			addErr("algorithm", "unsupported or unspecified")
		}
	}

	return errs, warnings
}

func validateCalendarWindow(limit *quotav1.Limit, addErr func(string, string), addWarn func(string, string)) {
	window := limit.GetWindow()
	if window == nil {
		addErr("window", "required for fixed calendar windows")
		return
	}
	if window.GetType() != quotav1.WindowType_WINDOW_TYPE_CALENDAR {
		addErr("window.type", "must be WINDOW_TYPE_CALENDAR")
	}
	switch window.GetCalendarUnit() {
	case quotav1.CalendarUnit_CALENDAR_UNIT_MINUTE,
		quotav1.CalendarUnit_CALENDAR_UNIT_HOUR,
		quotav1.CalendarUnit_CALENDAR_UNIT_DAY,
		quotav1.CalendarUnit_CALENDAR_UNIT_WEEK,
		quotav1.CalendarUnit_CALENDAR_UNIT_MONTH:
	default:
		addErr("window.calendar_unit", "v1 supports minute, hour, day, week, and month")
	}
	if tz := window.GetTimezone(); tz != "" && tz != "UTC" {
		addErr("window.timezone", "v1 supports UTC calendar windows only")
	}
	if window.GetBucketCount() > 0 {
		addWarn("window.bucket_count", "ignored for fixed calendar windows")
	}
}

func validateDurationWindow(limit *quotav1.Limit, addErr func(string, string)) {
	window := limit.GetWindow()
	if window == nil {
		addErr("window", "required for fixed duration windows")
		return
	}
	if window.GetType() != quotav1.WindowType_WINDOW_TYPE_DURATION {
		addErr("window.type", "must be WINDOW_TYPE_DURATION")
	}
	if window.GetDurationMs() <= 0 {
		addErr("window.duration_ms", "must be greater than zero")
	}
}

func validateSlidingWindow(limit *quotav1.Limit, addErr func(string, string), addWarn func(string, string)) {
	window := limit.GetWindow()
	if window == nil {
		addErr("window", "required for sliding windows")
		return
	}
	if window.GetType() != quotav1.WindowType_WINDOW_TYPE_SLIDING {
		addErr("window.type", "must be WINDOW_TYPE_SLIDING")
	}
	if window.GetDurationMs() <= 0 {
		addErr("window.duration_ms", "must be greater than zero")
	}
	if window.GetBucketCount() == 1 {
		addErr("window.bucket_count", "must be zero or at least two")
	}
	if window.GetBucketCount() == 0 {
		addWarn("window.bucket_count", "defaults to 10")
	}
}

func validateContinuous(limit *quotav1.Limit, addErr func(string, string), addWarn func(string, string)) {
	window := limit.GetWindow()
	if window != nil && window.GetType() != quotav1.WindowType_WINDOW_TYPE_CONTINUOUS {
		addErr("window.type", "must be WINDOW_TYPE_CONTINUOUS when supplied")
	}
	if limit.GetRefillRatePerSec() <= 0 {
		addErr("refill_rate_per_sec", "must be greater than zero")
	}
	if limit.GetBurst() <= 0 {
		addWarn("burst", "defaults to limit when unset")
	}
}
