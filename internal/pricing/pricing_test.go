package pricing

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTestTable writes a minimal pricing file to a temp dir and loads it,
// so tests don't depend on the real configs/pricing.json seed data drifting.
func writeTestTable(t *testing.T) *Table {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "pricing.json")
	const contents = `{
		"models": {
			"test-model": {"input_per_mtok": 3.0, "output_per_mtok": 15.0}
		}
	}`
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("writing test pricing file: %v", err)
	}

	table, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	return table
}

func TestCost(t *testing.T) {
	table := writeTestTable(t)

	tests := []struct {
		name         string
		model        string
		inputTokens  int
		outputTokens int
		wantCost     float64
		wantKnown    bool
	}{
		{
			name:         "known model computes cost from per-million rate",
			model:        "test-model",
			inputTokens:  1_000_000,
			outputTokens: 1_000_000,
			wantCost:     18.0, // 1*3.0 (input) + 1*15.0 (output)
			wantKnown:    true,
		},
		{
			name:         "zero tokens costs zero even for a known model",
			model:        "test-model",
			inputTokens:  0,
			outputTokens: 0,
			wantCost:     0,
			wantKnown:    true,
		},
		{
			name:         "unknown model reports known=false rather than erroring",
			model:        "does-not-exist",
			inputTokens:  1000,
			outputTokens: 1000,
			wantCost:     0,
			wantKnown:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCost, gotKnown := table.Cost(tt.model, tt.inputTokens, tt.outputTokens)
			if gotKnown != tt.wantKnown {
				t.Errorf("known = %v, want %v", gotKnown, tt.wantKnown)
			}
			if gotCost != tt.wantCost {
				t.Errorf("cost = %v, want %v", gotCost, tt.wantCost)
			}
		})
	}
}

// TestCost_UnknownModelFallsBackToDefaultPrice guards against the real bug
// this closed: an unpriced model computing to a $0 cost, which meant it
// reserved nothing and could never be capped - a new model release or a
// typo in a model name silently bypassed the daily cap entirely.
func TestCost_UnknownModelFallsBackToDefaultPrice(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pricing.json")
	const contents = `{
		"models": {
			"test-model": {"input_per_mtok": 3.0, "output_per_mtok": 15.0}
		},
		"default_price": {"input_per_mtok": 15.0, "output_per_mtok": 75.0}
	}`
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("writing test pricing file: %v", err)
	}
	table, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	cost, known := table.Cost("some-brand-new-model", 1_000_000, 1_000_000)
	if !known {
		t.Fatal("known = false, want true - a configured default_price must always produce a usable cost")
	}
	const want = 90.0 // 1*15.0 (input) + 1*75.0 (output)
	if cost != want {
		t.Errorf("cost = %v, want %v (the fallback rate, not 0)", cost, want)
	}

	// An exact match must still win over the fallback.
	exactCost, _ := table.Cost("test-model", 1_000_000, 1_000_000)
	if exactCost != 18.0 {
		t.Errorf("cost for a listed model = %v, want 18.0 (its own rate, not the fallback)", exactCost)
	}
}
