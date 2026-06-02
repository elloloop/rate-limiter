package keys

import (
	"testing"
	"time"

	quotav1 "github.com/elloloop/rate-limiter/gen/quota/v1"
)

func BenchmarkHash(b *testing.B) {
	for b.Loop() {
		_ = Hash("tenant:workspace:user:provider:openai:model:gpt-5")
	}
}

func BenchmarkPrefix(b *testing.B) {
	for b.Loop() {
		_ = Prefix("prod", "assistant")
	}
}

func BenchmarkSlidingBuckets(b *testing.B) {
	prefix := Prefix("prod", "assistant")
	limit := &quotav1.Limit{
		LimitId:  "tokens_per_minute",
		ScopeKey: "workspace:acme:user:42",
		Window: &quotav1.Window{
			Type:        quotav1.WindowType_WINDOW_TYPE_SLIDING,
			DurationMs:  60000,
			BucketCount: 10,
		},
	}
	now := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)

	for b.Loop() {
		_, _, _, _ = SlidingBuckets(prefix, limit, now)
	}
}
