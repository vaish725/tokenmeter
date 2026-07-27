package downshift

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAndSubstitute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "downshift.json")
	const contents = `{"substitutes": {"claude-opus-5": "claude-sonnet-5"}}`
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("writing test file: %v", err)
	}

	table, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	sub, ok := table.Substitute("claude-opus-5")
	if !ok || sub != "claude-sonnet-5" {
		t.Errorf("Substitute(claude-opus-5) = %q, %v, want claude-sonnet-5, true", sub, ok)
	}

	if _, ok := table.Substitute("no-such-model"); ok {
		t.Error("Substitute(no-such-model) = ok, want false for an unconfigured model")
	}
}

func TestLoadMissingFileIsNotFatalErrIsDetectable(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("errors.Is(err, os.ErrNotExist) = false, want true so callers can treat a missing file as \"downshift disabled\"")
	}
}

func TestNilTableSubstituteNeverPanics(t *testing.T) {
	var table *Table
	if _, ok := table.Substitute("anything"); ok {
		t.Error("Substitute on a nil Table = ok, want false")
	}
}
