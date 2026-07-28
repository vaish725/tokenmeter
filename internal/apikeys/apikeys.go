// Package apikeys implements the opt-in API-key-to-project attribution
// link (R4): a project only gets recognized this way if
// configs/api_keys.json exists and maps the key's suffix to it - no file,
// no behavior change.
//
// Matching is on the last 8 characters of a key, not the full key: this
// file never needs to be a second copy of a live secret to do its job.
package apikeys

import (
	"encoding/json"
	"fmt"
	"os"
)

// suffixLen is how many trailing characters of an API key identify it.
// Enough entropy to tell apart a personal set of keys without ever storing
// the full secret a second time.
const suffixLen = 8

// fileFormat mirrors configs/api_keys.json on disk.
type fileFormat struct {
	Projects map[string]string `json:"projects"`
}

// Table is a loaded, ready-to-query key-suffix-to-project map.
type Table struct {
	bySuffix map[string]string
}

// Load reads and parses the API key mapping JSON file at path. A missing
// file is a normal os.ErrNotExist-wrapped error - callers that want "no
// file means disabled" should check for that with errors.Is, not treat it
// as fatal (see downshift.Load, same convention).
func Load(path string) (*Table, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("apikeys: reading %s: %w", path, err)
	}

	var parsed fileFormat
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("apikeys: parsing %s: %w", path, err)
	}

	return &Table{bySuffix: parsed.Projects}, nil
}

// Lookup returns the project mapped to apiKey's suffix, if any. A nil
// Table (link disabled entirely) or a key shorter than the suffix length
// always reports ok=false. The raw key is never retained past this call.
func (t *Table) Lookup(apiKey string) (project string, ok bool) {
	if t == nil || len(apiKey) < suffixLen {
		return "", false
	}
	project, ok = t.bySuffix[apiKey[len(apiKey)-suffixLen:]]
	return project, ok
}
