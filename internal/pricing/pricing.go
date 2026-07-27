// Package pricing turns token counts into a dollar cost using a table
// loaded from JSON (not hardcoded), since provider prices change on their
// own schedule. An unknown model returns known=false rather than an error.
package pricing

import (
	"encoding/json"
	"fmt"
	"os"
)

// ModelPrice is the USD cost per one million input/output tokens for a model.
type ModelPrice struct {
	InputPerMTok  float64 `json:"input_per_mtok"`
	OutputPerMTok float64 `json:"output_per_mtok"`
}

// fileFormat mirrors configs/pricing.json on disk. The "_note" field exists
// purely for humans editing the file and is intentionally not decoded here.
type fileFormat struct {
	Models map[string]ModelPrice `json:"models"`
}

// Table is a loaded, ready-to-query pricing table.
type Table struct {
	models map[string]ModelPrice
}

// Load reads and parses the pricing JSON file at path.
func Load(path string) (*Table, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("pricing: reading %s: %w", path, err)
	}

	var parsed fileFormat
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("pricing: parsing %s: %w", path, err)
	}

	return &Table{models: parsed.Models}, nil
}

// Cost computes USD cost from token counts. known=false for an unpriced
// model; cost is always 0 in that case, not an error.
func (t *Table) Cost(model string, inputTokens, outputTokens int) (cost float64, known bool) {
	price, ok := t.models[model]
	if !ok {
		return 0, false
	}

	const million = 1_000_000.0
	cost = (float64(inputTokens)/million)*price.InputPerMTok +
		(float64(outputTokens)/million)*price.OutputPerMTok
	return cost, true
}
