package anomaly

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/vaish725/tokenmeter/internal/store"
)

func TestComputeP95(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{name: "empty", values: nil, want: 0},
		{name: "single value", values: []float64{5}, want: 5},
		{name: "all equal", values: []float64{3, 3, 3, 3, 3}, want: 3},
		{name: "ten values takes the max", values: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, want: 10},
	}

	// 0..99 (100 values): nearest-rank at ceil(0.95*100)-1 = index 94.
	hundred := make([]float64, 100)
	for i := range hundred {
		hundred[i] = float64(i)
	}
	tests = append(tests, struct {
		name   string
		values []float64
		want   float64
	}{name: "one hundred values", values: hundred, want: 94})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := computeP95(tt.values); got != tt.want {
				t.Errorf("computeP95() = %v, want %v", got, tt.want)
			}
		})
	}
}

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "meter_test.db"))
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCheckProject_FlagsASpikeAboveTrailingP95(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	thisHour := now.Truncate(time.Hour)

	// A quiet trailing week: $0.10/hour for the last 48 hours (well within
	// the 7-day lookback), nothing beyond that.
	for h := 1; h <= 48; h++ {
		ts := thisHour.Add(-time.Duration(h) * time.Hour).Add(time.Minute)
		if _, err := st.Insert(ctx, store.Record{Timestamp: ts, Project: "p", Provider: "anthropic", Model: "m", CostUSD: 0.10}); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}
	// A spike in the current hour, far above that trailing baseline.
	if _, err := st.Insert(ctx, store.Record{Timestamp: now, Project: "p", Provider: "anthropic", Model: "m", CostUSD: 10.0}); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	c := NewChecker(st)
	anomalous, current, p95, err := c.checkProject(ctx, "p", now)
	if err != nil {
		t.Fatalf("checkProject() error = %v", err)
	}
	if !anomalous {
		t.Errorf("anomalous = false, want true (current %.4f vs p95 %.4f)", current, p95)
	}
	if current != 10.0 {
		t.Errorf("current = %v, want 10.0", current)
	}
}

func TestCheckProject_NormalSpendIsNotAnomalous(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	thisHour := now.Truncate(time.Hour)

	for h := 0; h <= 48; h++ {
		ts := thisHour.Add(-time.Duration(h) * time.Hour).Add(time.Minute)
		if _, err := st.Insert(ctx, store.Record{Timestamp: ts, Project: "p", Provider: "anthropic", Model: "m", CostUSD: 0.10}); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	c := NewChecker(st)
	anomalous, _, _, err := c.checkProject(ctx, "p", now)
	if err != nil {
		t.Fatalf("checkProject() error = %v", err)
	}
	if anomalous {
		t.Error("anomalous = true, want false for spend consistent with its own trailing baseline")
	}
}

func TestCheckProject_BrandNewProjectIsNeverAnomalous(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Only ever the current hour's activity - a zero trailing baseline
	// must not flag a project's very first request as an anomaly.
	if _, err := st.Insert(ctx, store.Record{Timestamp: now, Project: "brand-new", Provider: "anthropic", Model: "m", CostUSD: 5.0}); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	c := NewChecker(st)
	anomalous, _, p95, err := c.checkProject(ctx, "brand-new", now)
	if err != nil {
		t.Fatalf("checkProject() error = %v", err)
	}
	if p95 != 0 {
		t.Errorf("p95 = %v, want 0 (no meaningful trailing history yet)", p95)
	}
	if anomalous {
		t.Error("anomalous = true, want false for a brand new project's first request")
	}
}
