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
