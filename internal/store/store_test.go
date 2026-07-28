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

	id, err := s.Insert(ctx, want)
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if id != 1 {
		t.Errorf("Insert() id = %d, want 1 (first row in a fresh db)", id)
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

	if _, err := s.Insert(context.Background(), Record{Timestamp: time.Now(), Project: "new", Provider: "openai", Model: "gpt-4o"}); err != nil {
		t.Fatalf("Insert() after migration error = %v", err)
	}
}

func TestSpendByProject(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	now := time.Now()
	rows := []Record{
		{Timestamp: now, Project: "a", Provider: "anthropic", Model: "m", CostUSD: 1.0},
		{Timestamp: now, Project: "a", Provider: "anthropic", Model: "m", CostUSD: 0.5},
		{Timestamp: now, Project: "b", Provider: "openai", Model: "m", CostUSD: 2.0},
		{Timestamp: now.Add(-48 * time.Hour), Project: "a", Provider: "anthropic", Model: "m", CostUSD: 100.0}, // outside the window
	}
	for _, r := range rows {
		if _, err := s.Insert(ctx, r); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	spend, err := s.SpendByProject(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("SpendByProject() error = %v", err)
	}

	if spend["a"] != 1.5 {
		t.Errorf("spend[a] = %v, want 1.5 (old row outside the window must be excluded)", spend["a"])
	}
	if spend["b"] != 2.0 {
		t.Errorf("spend[b] = %v, want 2.0", spend["b"])
	}
	if len(spend) != 2 {
		t.Errorf("len(spend) = %d, want 2 projects", len(spend))
	}
}

func TestInsertPromptCapture(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	id, err := s.Insert(ctx, Record{Timestamp: time.Now(), Project: "a", Provider: "anthropic", Model: "m"})
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	if err := s.InsertPromptCapture(ctx, id, []byte("hello world"), false); err != nil {
		t.Fatalf("InsertPromptCapture() error = %v", err)
	}

	var body string
	var truncated int
	err = s.db.QueryRowContext(ctx, `SELECT body, truncated FROM prompt_captures WHERE request_id = ?`, id).Scan(&body, &truncated)
	if err != nil {
		t.Fatalf("reading capture: %v", err)
	}
	if body != "hello world" {
		t.Errorf("body = %q, want %q", body, "hello world")
	}
	if truncated != 0 {
		t.Errorf("truncated = %d, want 0", truncated)
	}
}

func TestAllRequests(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	now := time.Now()
	if _, err := s.Insert(ctx, Record{Timestamp: now, Project: "a", Provider: "anthropic", Model: "m", CostUSD: 1}); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if _, err := s.Insert(ctx, Record{Timestamp: now, Project: "b", Provider: "openai", Model: "m", CostUSD: 2}); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if _, err := s.Insert(ctx, Record{Timestamp: now.Add(-48 * time.Hour), Project: "old", Provider: "anthropic", Model: "m", CostUSD: 100}); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	records, err := s.AllRequests(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("AllRequests() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2 (the old row is outside the window)", len(records))
	}
	if records[0].Project != "a" || records[1].Project != "b" {
		t.Errorf("order = %q, %q, want chronological a, b", records[0].Project, records[1].Project)
	}
}

func TestActiveProjects(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	now := time.Now()
	for _, p := range []string{"a", "a", "b"} {
		if _, err := s.Insert(ctx, Record{Timestamp: now, Project: p, Provider: "anthropic", Model: "m"}); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}
	if _, err := s.Insert(ctx, Record{Timestamp: now.Add(-48 * time.Hour), Project: "stale", Provider: "anthropic", Model: "m"}); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	projects, err := s.ActiveProjects(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ActiveProjects() error = %v", err)
	}
	got := map[string]bool{}
	for _, p := range projects {
		got[p] = true
	}
	if len(got) != 2 || !got["a"] || !got["b"] {
		t.Errorf("projects = %v, want exactly {a, b} (stale project outside the window excluded)", projects)
	}
}

func TestHourlySpend(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	thisHour := now.Truncate(time.Hour)
	lastHour := thisHour.Add(-time.Hour)

	rows := []Record{
		{Timestamp: thisHour.Add(5 * time.Minute), Project: "a", Provider: "anthropic", Model: "m", CostUSD: 1.0},
		{Timestamp: thisHour.Add(10 * time.Minute), Project: "a", Provider: "anthropic", Model: "m", CostUSD: 0.5},
		{Timestamp: lastHour.Add(5 * time.Minute), Project: "a", Provider: "anthropic", Model: "m", CostUSD: 2.0},
	}
	for _, r := range rows {
		if _, err := s.Insert(ctx, r); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	hourly, err := s.HourlySpend(ctx, "a", now.Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("HourlySpend() error = %v", err)
	}
	if len(hourly) != 2 {
		t.Fatalf("len(hourly) = %d, want 2 buckets", len(hourly))
	}
	thisHourKey := thisHour.Format("2006-01-02T15")
	lastHourKey := lastHour.Format("2006-01-02T15")
	if hourly[thisHourKey] != 1.5 {
		t.Errorf("hourly[%q] = %v, want 1.5", thisHourKey, hourly[thisHourKey])
	}
	if hourly[lastHourKey] != 2.0 {
		t.Errorf("hourly[%q] = %v, want 2.0", lastHourKey, hourly[lastHourKey])
	}
}

func TestMetricsSnapshot(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	now := time.Now()
	rows := []Record{
		{Timestamp: now, Project: "a", Provider: "anthropic", Model: "claude-sonnet-5", StatusCode: 200, CostUSD: 1.0, InputTokens: 10, OutputTokens: 5},
		{Timestamp: now, Project: "a", Provider: "anthropic", Model: "claude-sonnet-5", StatusCode: 200, CostUSD: 2.0, InputTokens: 20, OutputTokens: 10},
		{Timestamp: now, Project: "a", Provider: "anthropic", Model: "claude-sonnet-5", StatusCode: 429, CostUSD: 0},
	}
	for _, r := range rows {
		if _, err := s.Insert(ctx, r); err != nil {
			t.Fatalf("Insert() error = %v", err)
		}
	}

	snapshot, err := s.MetricsSnapshot(ctx)
	if err != nil {
		t.Fatalf("MetricsSnapshot() error = %v", err)
	}
	if len(snapshot) != 2 {
		t.Fatalf("len(snapshot) = %d, want 2 groups (status 200 and 429 separately)", len(snapshot))
	}

	var got200 *MetricsRow
	for i := range snapshot {
		if snapshot[i].StatusCode == 200 {
			got200 = &snapshot[i]
		}
	}
	if got200 == nil {
		t.Fatal("no group found for status_code=200")
	}
	if got200.Count != 2 {
		t.Errorf("Count = %d, want 2", got200.Count)
	}
	if got200.CostUSD != 3.0 {
		t.Errorf("CostUSD = %v, want 3.0", got200.CostUSD)
	}
	if got200.InputTokens != 30 || got200.OutputTokens != 15 {
		t.Errorf("tokens = %d/%d, want 30/15", got200.InputTokens, got200.OutputTokens)
	}
}
