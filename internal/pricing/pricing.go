// Package pricing turns token counts into a dollar cost using a table
// loaded from JSON (not hardcoded), since provider prices change on their
// own schedule.
//
// A model missing from the table falls back to an optional configured
// default_price rather than silently costing $0. That matters beyond
// accounting: callers use this same cost both to size a pre-flight budget
// reservation and to reconcile it afterward (see internal/proxy), so a
// model priced at $0 reserves nothing and is never capped - a brand new
// model release or a typo in a model name would otherwise bypass the daily
// cap entirely and silently. known=false only when there is truly nothing
// to go on (no exact match and no fallback configured).
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
	Models       map[string]ModelPrice `json:"models"`
	DefaultPrice *ModelPrice           `json:"default_price"`
}

// Table is a loaded, ready-to-query pricing table.
type Table struct {
	models       map[string]ModelPrice
	defaultPrice *ModelPrice // nil if configs/pricing.json doesn't set one
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

	return &Table{models: parsed.Models, defaultPrice: parsed.DefaultPrice}, nil
}

// Cost computes USD cost from token counts: an exact per-model rate if one
// is configured, else the table's default_price fallback if one is
// configured, else known=false with cost always 0.
func (t *Table) Cost(model string, inputTokens, outputTokens int) (cost float64, known bool) {
	price, ok := t.models[model]
	if !ok {
		if t.defaultPrice == nil {
			return 0, false
		}
		price = *t.defaultPrice
	}

	const million = 1_000_000.0
	cost = (float64(inputTokens)/million)*price.InputPerMTok +
		(float64(outputTokens)/million)*price.OutputPerMTok
	return cost, true
}
