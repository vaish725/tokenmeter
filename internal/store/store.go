// Package store persists per-request spend metadata (never prompt bodies)
// to a local SQLite file, via modernc.org/sqlite - pure Go, no cgo, so the
// eventual static binary needs no rework later.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

// Record is one logged proxy request. UsageKnown distinguishes a real
// known-zero cost from "usage couldn't be parsed" - conflating the two
// would quietly corrupt spend totals.
type Record struct {
	Timestamp    time.Time
	Project      string
	Provider     string
	Model        string
	InputTokens  int
	OutputTokens int
	CostUSD      float64
	LatencyMS    int64
	StatusCode   int
	Stream       bool
	UsageKnown   bool
}

// Store wraps the underlying *sql.DB with meter's schema and queries.
type Store struct {
	db *sql.DB
}

// schema creates the requests table if needed - no migration framework
// yet, since there's only one schema version so far.
const schema = `
CREATE TABLE IF NOT EXISTS requests (
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

CREATE TABLE IF NOT EXISTS prompt_captures (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	request_id INTEGER NOT NULL,
	body       TEXT    NOT NULL,
	truncated  INTEGER NOT NULL
);
`

// Open opens (creating if necessary) the SQLite file at path and ensures the
// schema exists.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: opening %s: %w", path, err)
	}

	// SQLite allows one writer at a time; one connection avoids "database
	// is locked" errors from a pool trying to parallelize writes.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: creating schema: %w", err)
	}
	if err := migrateProviderColumn(db); err != nil {
		db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

// migrateProviderColumn adds the provider column for databases created
// before OpenAI support existed. CREATE TABLE IF NOT EXISTS is a no-op on
// an already-existing table, so a real dogfooded meter.db from week 1-3
// needs this explicitly. Backfilling as "anthropic" is correct: that was
// the only provider that could have written those rows.
func migrateProviderColumn(db *sql.DB) error {
	_, err := db.Exec(`ALTER TABLE requests ADD COLUMN provider TEXT NOT NULL DEFAULT 'anthropic'`)
	if err == nil || strings.Contains(err.Error(), "duplicate column name") {
		return nil
	}
	return fmt.Errorf("store: migrating provider column: %w", err)
}

// Close closes the underlying database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// Insert persists one request record and returns its new row id (used to
// link an optional prompt capture back to the request it belongs to).
// Called after every proxied request, success or failure, so that "what
// did this cost" always has an answer.
func (s *Store) Insert(ctx context.Context, r Record) (int64, error) {
	const q = `
	INSERT INTO requests
		(timestamp, project, provider, model, input_tokens, output_tokens, cost_usd, latency_ms, status_code, stream, usage_known)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	result, err := s.db.ExecContext(ctx, q,
		r.Timestamp.UTC().Format(time.RFC3339),
		r.Project,
		r.Provider,
		r.Model,
		r.InputTokens,
		r.OutputTokens,
		r.CostUSD,
		r.LatencyMS,
		r.StatusCode,
		boolToInt(r.Stream),
		boolToInt(r.UsageKnown),
	)
	if err != nil {
		return 0, fmt.Errorf("store: inserting request record: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("store: reading inserted request id: %w", err)
	}
	return id, nil
}

// InsertPromptCapture persists a request's prompt body, opt-in and
// separate from the always-on requests table (see config.CapturePrompts).
// body should already be truncated to the configured size limit by the
// caller; truncated records whether that happened.
func (s *Store) InsertPromptCapture(ctx context.Context, requestID int64, body []byte, truncated bool) error {
	const q = `INSERT INTO prompt_captures (request_id, body, truncated) VALUES (?, ?, ?)`
	if _, err := s.db.ExecContext(ctx, q, requestID, string(body), boolToInt(truncated)); err != nil {
		return fmt.Errorf("store: inserting prompt capture: %w", err)
	}
	return nil
}

// TopRequests returns the costliest requests since since, for `meter top`.
func (s *Store) TopRequests(ctx context.Context, since time.Time, limit int) ([]Record, error) {
	const q = `
	SELECT timestamp, project, provider, model, input_tokens, output_tokens, cost_usd, latency_ms, status_code, stream, usage_known
	FROM requests WHERE timestamp >= ? ORDER BY cost_usd DESC LIMIT ?
	`
	rows, err := s.db.QueryContext(ctx, q, since.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("store: querying top requests: %w", err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		var ts string
		var stream, usageKnown int
		if err := rows.Scan(&ts, &r.Project, &r.Provider, &r.Model, &r.InputTokens, &r.OutputTokens, &r.CostUSD, &r.LatencyMS, &r.StatusCode, &stream, &usageKnown); err != nil {
			return nil, fmt.Errorf("store: scanning top request row: %w", err)
		}
		r.Timestamp, err = time.Parse(time.RFC3339, ts)
		if err != nil {
			return nil, fmt.Errorf("store: parsing timestamp %q: %w", ts, err)
		}
		r.Stream = stream != 0
		r.UsageKnown = usageKnown != 0
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating top requests: %w", err)
	}
	return records, nil
}

// SpendByProject sums cost_usd since since, grouped by project - the
// building block for `meter watch`'s cumulative and burn-rate figures
// (called with two different since values for each).
func (s *Store) SpendByProject(ctx context.Context, since time.Time) (map[string]float64, error) {
	const q = `SELECT project, SUM(cost_usd) FROM requests WHERE timestamp >= ? GROUP BY project`
	rows, err := s.db.QueryContext(ctx, q, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("store: querying spend by project: %w", err)
	}
	defer rows.Close()

	spend := make(map[string]float64)
	for rows.Next() {
		var project string
		var total float64
		if err := rows.Scan(&project, &total); err != nil {
			return nil, fmt.Errorf("store: scanning spend row: %w", err)
		}
		spend[project] = total
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating spend rows: %w", err)
	}
	return spend, nil
}

// AllRequests returns every request since since, ordered chronologically -
// for `meter export`, which wants a full dump of a window, not a ranking.
func (s *Store) AllRequests(ctx context.Context, since time.Time) ([]Record, error) {
	const q = `
	SELECT timestamp, project, provider, model, input_tokens, output_tokens, cost_usd, latency_ms, status_code, stream, usage_known
	FROM requests WHERE timestamp >= ? ORDER BY id ASC
	`
	rows, err := s.db.QueryContext(ctx, q, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("store: querying all requests: %w", err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		var ts string
		var stream, usageKnown int
		if err := rows.Scan(&ts, &r.Project, &r.Provider, &r.Model, &r.InputTokens, &r.OutputTokens, &r.CostUSD, &r.LatencyMS, &r.StatusCode, &stream, &usageKnown); err != nil {
			return nil, fmt.Errorf("store: scanning request row: %w", err)
		}
		r.Timestamp, err = time.Parse(time.RFC3339, ts)
		if err != nil {
			return nil, fmt.Errorf("store: parsing timestamp %q: %w", ts, err)
		}
		r.Stream = stream != 0
		r.UsageKnown = usageKnown != 0
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating request rows: %w", err)
	}
	return records, nil
}

// ActiveProjects returns the distinct projects with any activity since
// since - the anomaly checker's starting point for which projects to check.
func (s *Store) ActiveProjects(ctx context.Context, since time.Time) ([]string, error) {
	const q = `SELECT DISTINCT project FROM requests WHERE timestamp >= ?`
	rows, err := s.db.QueryContext(ctx, q, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("store: querying active projects: %w", err)
	}
	defer rows.Close()

	var projects []string
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return nil, fmt.Errorf("store: scanning project row: %w", err)
		}
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating project rows: %w", err)
	}
	return projects, nil
}

// HourlySpend sums project's cost_usd since since, bucketed by the local
// hour it happened in ("2006-01-02T15" keys) - the anomaly checker's raw
// material for a trailing p95. Hours with no requests simply don't appear
// in the map; the caller decides how to treat missing hours.
func (s *Store) HourlySpend(ctx context.Context, project string, since time.Time) (map[string]float64, error) {
	const q = `
	SELECT strftime('%Y-%m-%dT%H', timestamp), SUM(cost_usd)
	FROM requests WHERE project = ? AND timestamp >= ?
	GROUP BY strftime('%Y-%m-%dT%H', timestamp)
	`
	rows, err := s.db.QueryContext(ctx, q, project, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("store: querying hourly spend: %w", err)
	}
	defer rows.Close()

	hourly := make(map[string]float64)
	for rows.Next() {
		var hour string
		var total float64
		if err := rows.Scan(&hour, &total); err != nil {
			return nil, fmt.Errorf("store: scanning hourly spend row: %w", err)
		}
		hourly[hour] = total
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating hourly spend rows: %w", err)
	}
	return hourly, nil
}

// MetricsRow is one grouped aggregate for the Prometheus /metrics endpoint.
type MetricsRow struct {
	Project      string
	Provider     string
	Model        string
	StatusCode   int
	Count        int64
	CostUSD      float64
	InputTokens  int64
	OutputTokens int64
}

// MetricsSnapshot aggregates the entire requests table, grouped the way
// /metrics needs it. Computed fresh on every scrape rather than kept as an
// in-process counter: since rows are only ever inserted, never deleted,
// this gives genuinely monotonic counters that survive a meter restart,
// which an in-memory counter couldn't offer for free.
func (s *Store) MetricsSnapshot(ctx context.Context) ([]MetricsRow, error) {
	const q = `
	SELECT project, provider, model, status_code, COUNT(*), SUM(cost_usd), SUM(input_tokens), SUM(output_tokens)
	FROM requests GROUP BY project, provider, model, status_code
	`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: querying metrics snapshot: %w", err)
	}
	defer rows.Close()

	var out []MetricsRow
	for rows.Next() {
		var r MetricsRow
		if err := rows.Scan(&r.Project, &r.Provider, &r.Model, &r.StatusCode, &r.Count, &r.CostUSD, &r.InputTokens, &r.OutputTokens); err != nil {
			return nil, fmt.Errorf("store: scanning metrics row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterating metrics rows: %w", err)
	}
	return out, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
