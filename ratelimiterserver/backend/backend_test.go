package backend

import (
	"encoding/json"
	"testing"
)

func TestDecisionResultUnmarshalStatusesShapes(t *testing.T) {
	tests := []struct {
		name       string
		statuses   string
		wantStatus int
	}{
		{name: "array", statuses: `[{"limit_id":"daily","used":3,"remaining":7,"allowed":true}]`, wantStatus: 1},
		{name: "null", statuses: `null`, wantStatus: 0},
		{name: "empty_object", statuses: `{}`, wantStatus: 0},
		{name: "omitted", statuses: ``, wantStatus: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := `{"decision_id":"decision-1","allowed":true`
			if tt.statuses != "" {
				raw += `,"statuses":` + tt.statuses
			}
			raw += `}`

			var result DecisionResult
			if err := json.Unmarshal([]byte(raw), &result); err != nil {
				t.Fatalf("UnmarshalJSON: %v", err)
			}
			if result.DecisionID != "decision-1" || !result.Allowed {
				t.Fatalf("unexpected envelope: %#v", result)
			}
			if len(result.Statuses) != tt.wantStatus {
				t.Fatalf("statuses len = %d, want %d: %#v", len(result.Statuses), tt.wantStatus, result.Statuses)
			}
		})
	}
}

func TestDecisionResultRejectsInvalidStatuses(t *testing.T) {
	var result DecisionResult
	err := json.Unmarshal([]byte(`{"allowed":true,"statuses":{"limit_id":"not-an-array"}}`), &result)
	if err == nil {
		t.Fatal("expected invalid statuses object to fail")
	}
}

func TestDecisionResultRejectsMalformedEnvelope(t *testing.T) {
	var result DecisionResult
	if err := json.Unmarshal([]byte(`{`), &result); err == nil {
		t.Fatal("expected malformed decision envelope to fail")
	}
}
