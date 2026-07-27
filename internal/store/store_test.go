package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "meter_test.db")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { s.Close() })

	return s
}

func TestInsertAndReadBack(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := Record{
		Timestamp:    time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC),
		Project:      "cognitiveradar",
		Model:        "claude-sonnet-5",
		InputTokens:  1200,
		OutputTokens: 340,
		CostUSD:      0.0087,
		LatencyMS:    842,
		StatusCode:   200,
		Stream:       false,
		UsageKnown:   true,
	}

	if err := s.Insert(ctx, want); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	// Read the row back through raw SQL (this test lives in package store,
	// so it can reach into s.db directly) to confirm every column round-trips.
	row := s.db.QueryRowContext(ctx, `
		SELECT project, model, input_tokens, output_tokens, cost_usd, latency_ms, status_code, stream, usage_known
		FROM requests WHERE id = 1
	`)

	var (
		project      string
		model        string
		inputTokens  int
		outputTokens int
		costUSD      float64
		latencyMS    int64
		statusCode   int
		stream       int
		usageKnown   int
	)
	if err := row.Scan(&project, &model, &inputTokens, &outputTokens, &costUSD, &latencyMS, &statusCode, &stream, &usageKnown); err != nil {
		t.Fatalf("scanning inserted row: %v", err)
	}

	if project != want.Project || model != want.Model {
		t.Errorf("project/model = %q/%q, want %q/%q", project, model, want.Project, want.Model)
	}
	if inputTokens != want.InputTokens || outputTokens != want.OutputTokens {
		t.Errorf("tokens = %d/%d, want %d/%d", inputTokens, outputTokens, want.InputTokens, want.OutputTokens)
	}
	if costUSD != want.CostUSD {
		t.Errorf("cost = %v, want %v", costUSD, want.CostUSD)
	}
	if statusCode != want.StatusCode {
		t.Errorf("status = %d, want %d", statusCode, want.StatusCode)
	}
	if stream != 0 {
		t.Errorf("stream = %d, want 0 (false)", stream)
	}
	if usageKnown != 1 {
		t.Errorf("usage_known = %d, want 1 (true)", usageKnown)
	}
}

func TestOpenCreatesSchemaIdempotently(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meter_test.db")

	// Opening the same DB file twice must not fail on "table already exists".
	s1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer s2.Close()
}
