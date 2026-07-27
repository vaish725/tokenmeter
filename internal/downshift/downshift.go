// Package downshift implements the opt-in "cheaper model instead of a
// 429" policy: a project only downshifts if configs/downshift.json exists
// and maps its model to a substitute - no file, no behavior change.
package downshift

import (
	"encoding/json"
	"fmt"
	"os"
)

// fileFormat mirrors configs/downshift.json on disk.
type fileFormat struct {
	Substitutes map[string]string `json:"substitutes"`
}

// Table is a loaded, ready-to-query substitute map.
type Table struct {
	substitutes map[string]string
}

// Load reads and parses the downshift JSON file at path. A missing file is
// a normal os.ErrNotExist-wrapped error - callers that want "no file means
// disabled" should check for that with errors.Is, not treat it as fatal.
func Load(path string) (*Table, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("downshift: reading %s: %w", path, err)
	}

	var parsed fileFormat
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("downshift: parsing %s: %w", path, err)
	}

	return &Table{substitutes: parsed.Substitutes}, nil
}

// Substitute returns the configured cheaper model for model, if any. A nil
// Table (downshift disabled entirely) always reports ok=false.
func (t *Table) Substitute(model string) (substitute string, ok bool) {
	if t == nil {
		return "", false
	}
	substitute, ok = t.substitutes[model]
	return substitute, ok
}
