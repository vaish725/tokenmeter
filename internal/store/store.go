// Package store persists per-request spend metadata (never prompt bodies)
// to a local SQLite file, via modernc.org/sqlite - pure Go, no cgo, so the
// eventual static binary needs no rework later.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

// Record is one logged proxy request. UsageKnown distinguishes a real
// known-zero cost from "usage couldn't be parsed" - conflating the two
// would quietly corrupt spend totals.
type Record struct {
	Timestamp    time.Time
	Project      string
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

	return &Store{db: db}, nil
}

// Close closes the underlying database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// Insert persists one request record. Called after every proxied request,
// success or failure, so that "what did this cost" always has an answer.
func (s *Store) Insert(ctx context.Context, r Record) error {
	const q = `
	INSERT INTO requests
		(timestamp, project, model, input_tokens, output_tokens, cost_usd, latency_ms, status_code, stream, usage_known)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err := s.db.ExecContext(ctx, q,
		r.Timestamp.UTC().Format(time.RFC3339),
		r.Project,
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
		return fmt.Errorf("store: inserting request record: %w", err)
	}
	return nil
}

// TopRequests returns the costliest requests since since, for `meter top`.
func (s *Store) TopRequests(ctx context.Context, since time.Time, limit int) ([]Record, error) {
	const q = `
	SELECT timestamp, project, model, input_tokens, output_tokens, cost_usd, latency_ms, status_code, stream, usage_known
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
		if err := rows.Scan(&ts, &r.Project, &r.Model, &r.InputTokens, &r.OutputTokens, &r.CostUSD, &r.LatencyMS, &r.StatusCode, &stream, &usageKnown); err != nil {
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

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
