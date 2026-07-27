package store

import (
	"context"
	"database/sql"
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
		Provider:     "anthropic",
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
		SELECT project, provider, model, input_tokens, output_tokens, cost_usd, latency_ms, status_code, stream, usage_known
		FROM requests WHERE id = 1
	`)

	var (
		project      string
		provider     string
		model        string
		inputTokens  int
		outputTokens int
		costUSD      float64
		latencyMS    int64
		statusCode   int
		stream       int
		usageKnown   int
	)
	if err := row.Scan(&project, &provider, &model, &inputTokens, &outputTokens, &costUSD, &latencyMS, &statusCode, &stream, &usageKnown); err != nil {
		t.Fatalf("scanning inserted row: %v", err)
	}

	if project != want.Project || provider != want.Provider || model != want.Model {
		t.Errorf("project/provider/model = %q/%q/%q, want %q/%q/%q", project, provider, model, want.Project, want.Provider, want.Model)
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

// TestOpenMigratesPreProviderColumnDatabase simulates a real dogfooded
// meter.db from before OpenAI support: the requests table exists but has
// no provider column. Open must add it, not error, and old rows must
// backfill as "anthropic" - the only provider that could have written them.
func TestOpenMigratesPreProviderColumnDatabase(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meter_test.db")

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening raw db: %v", err)
	}
	const oldSchema = `
	CREATE TABLE requests (
		id            INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp     TEXT    NOT NULL,
		project       TEXT    NOT NULL,
		model         TEXT    NOT NULL,
		input_tokens  INTEGER NOT NULL,
		output_tokens INTEGER NOT NULL,
		cost_usd      REAL    NOT NULL,
		latency_ms    INTEGER NOT NULL,
		status_code   INTEGER NOT NULL,
		stream        INTEGER NOT NULL,
		usage_known   INTEGER NOT NULL
	);
	`
	if _, err := db.Exec(oldSchema); err != nil {
		t.Fatalf("creating pre-migration schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO requests (timestamp, project, model, input_tokens, output_tokens, cost_usd, latency_ms, status_code, stream, usage_known) VALUES ('2026-01-01T00:00:00Z', 'legacy', 'claude-sonnet-5', 10, 5, 0.001, 100, 200, 0, 1)`); err != nil {
		t.Fatalf("inserting pre-migration row: %v", err)
	}
	db.Close()

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() on pre-migration database error = %v", err)
	}
	defer s.Close()

	var provider string
	if err := s.db.QueryRow(`SELECT provider FROM requests WHERE project = 'legacy'`).Scan(&provider); err != nil {
		t.Fatalf("reading migrated row: %v", err)
	}
	if provider != "anthropic" {
		t.Errorf("provider = %q, want %q for a backfilled pre-migration row", provider, "anthropic")
	}

	if err := s.Insert(context.Background(), Record{Timestamp: time.Now(), Project: "new", Provider: "openai", Model: "gpt-4o"}); err != nil {
		t.Fatalf("Insert() after migration error = %v", err)
	}
}
