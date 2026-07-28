package apikeys

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAndLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api_keys.json")
	const contents = `{"projects": {"abcd1234": "cognitiveradar"}}`
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("writing test file: %v", err)
	}

	table, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Only the last 8 characters have to match - the rest of a real key is
	// irrelevant to the lookup and never has to appear in the config file.
	project, ok := table.Lookup("sk-ant-api03-somereallylongkeyabcd1234")
	if !ok || project != "cognitiveradar" {
		t.Errorf("Lookup() = %q, %v, want cognitiveradar, true", project, ok)
	}

	if _, ok := table.Lookup("sk-ant-api03-somereallylongkeyzzzzzzzz"); ok {
		t.Error("Lookup() for an unmapped key = ok, want false")
	}

	if _, ok := table.Lookup("short"); ok {
		t.Error("Lookup() for a key shorter than the suffix length = ok, want false")
	}
}

func TestLoadMissingFileIsNotFatalErrIsDetectable(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("errors.Is(err, os.ErrNotExist) = false, want true so callers can treat a missing file as \"attribution link disabled\"")
	}
}

func TestNilTableLookupNeverPanics(t *testing.T) {
	var table *Table
	if _, ok := table.Lookup("anything12345678"); ok {
		t.Error("Lookup on a nil Table = ok, want false")
	}
}
