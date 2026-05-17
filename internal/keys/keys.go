package keys

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

func Prefix(environment, product string) string {
	if environment == "" {
		environment = "local"
	}
	if product == "" {
		product = "default"
	}
	return fmt.Sprintf("quota:v1:%s:%s:", environment, product)
}

func Hash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func Request(prefix, requestID string) string {
	return prefix + "req:" + requestID
}

func Reservation(prefix, reservationID string) string {
	return prefix + "res:" + reservationID
}

func Lease(prefix, leaseID string) string {
	return prefix + "lease:" + leaseID
}

func LeaseSet(prefix string, limit *quotav1.Limit) string {
	return prefix + "lease_set:" + Hash(limit.GetLimitId()) + ":" + Hash(limit.GetScopeKey())
}

func FixedWindow(prefix string, limit *quotav1.Limit, now time.Time) (key string, resetAt time.Time, ttl time.Duration) {
	windowID, reset := calendarWindow(limit.GetWindow().GetCalendarUnit(), now.UTC())
	key = prefix + "fw:" + Hash(limit.GetLimitId()) + ":" + Hash(limit.GetScopeKey()) + ":" + windowID
	return key, reset, time.Until(reset) + time.Hour
}

func DurationWindow(prefix string, limit *quotav1.Limit, now time.Time) (key string, resetAt time.Time, ttl time.Duration) {
	duration := time.Duration(limit.GetWindow().GetDurationMs()) * time.Millisecond
	bucket := now.UnixMilli() / limit.GetWindow().GetDurationMs()
	reset := time.UnixMilli((bucket + 1) * limit.GetWindow().GetDurationMs())
	key = prefix + "fw:" + Hash(limit.GetLimitId()) + ":" + Hash(limit.GetScopeKey()) + ":" + strconv.FormatInt(bucket, 10)
	return key, reset, duration + time.Hour
}

func SlidingBuckets(prefix string, limit *quotav1.Limit, now time.Time) (readKeys []string, writeKey string, resetAt time.Time, ttl time.Duration) {
	window := limit.GetWindow()
	count := int64(window.GetBucketCount())
	if count <= 0 {
		count = 10
	}
	bucketSizeMs := window.GetDurationMs() / count
	if bucketSizeMs <= 0 {
		bucketSizeMs = 1
	}
	nowMs := now.UnixMilli()
	currentBucket := nowMs / bucketSizeMs
	startBucket := (nowMs - window.GetDurationMs()) / bucketSizeMs
	base := prefix + "sw:" + Hash(limit.GetLimitId()) + ":" + Hash(limit.GetScopeKey()) + ":"
	for bucket := startBucket + 1; bucket <= currentBucket; bucket++ {
		readKeys = append(readKeys, base+strconv.FormatInt(bucket, 10))
	}
	writeKey = base + strconv.FormatInt(currentBucket, 10)
	resetAt = time.UnixMilli((currentBucket + 1) * bucketSizeMs)
	ttl = time.Duration(window.GetDurationMs()+bucketSizeMs)*time.Millisecond + time.Hour
	return readKeys, writeKey, resetAt, ttl
}

func TokenBucket(prefix string, limit *quotav1.Limit) string {
	return prefix + "tb:" + Hash(limit.GetLimitId()) + ":" + Hash(limit.GetScopeKey())
}

func LeakyBucket(prefix string, limit *quotav1.Limit) string {
	return prefix + "lb:" + Hash(limit.GetLimitId()) + ":" + Hash(limit.GetScopeKey())
}

func GCRA(prefix string, limit *quotav1.Limit) string {
	return prefix + "gcra:" + Hash(limit.GetLimitId()) + ":" + Hash(limit.GetScopeKey())
}

func calendarWindow(unit quotav1.CalendarUnit, now time.Time) (string, time.Time) {
	switch unit {
	case quotav1.CalendarUnit_CALENDAR_UNIT_MINUTE:
		start := now.Truncate(time.Minute)
		return start.Format("20060102T1504"), start.Add(time.Minute)
	case quotav1.CalendarUnit_CALENDAR_UNIT_HOUR:
		start := now.Truncate(time.Hour)
		return start.Format("20060102T15"), start.Add(time.Hour)
	case quotav1.CalendarUnit_CALENDAR_UNIT_WEEK:
		weekday := int(now.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		start := time.Date(now.Year(), now.Month(), now.Day()-weekday+1, 0, 0, 0, 0, time.UTC)
		year, week := start.ISOWeek()
		return fmt.Sprintf("%04dW%02d", year, week), start.AddDate(0, 0, 7)
	case quotav1.CalendarUnit_CALENDAR_UNIT_MONTH:
		start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start.Format("200601"), start.AddDate(0, 1, 0)
	default:
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return start.Format("20060102"), start.AddDate(0, 0, 1)
	}
}
