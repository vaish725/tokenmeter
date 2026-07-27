// Package pricing turns raw token counts into a dollar cost using a table
// loaded from a JSON file rather than hardcoded constants.
//
// The PRD's own risk section calls this out explicitly: provider prices
// change on their own schedule, so the table has to be editable without a
// rebuild. An unknown model is not an error here - it is logged as such and
// callers fall back to a zero, "cost unknown" cost rather than failing the
// proxied request. A broken price list must never block a real LLM call.
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

// Cost computes the USD cost of a request given its model name and token
// counts. known is false when the model isn't in the table, in which case
// cost is always zero - the caller should still persist the request with
// usage_known=false rather than dropping the row or failing the call.
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
