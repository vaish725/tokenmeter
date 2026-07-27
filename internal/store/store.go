// Package store persists per-request spend metadata to a local SQLite file.
//
// The driver is modernc.org/sqlite, a pure-Go implementation with no cgo
// dependency. That choice is deliberate and made this early on purpose: the
// PRD's packaging goal (weeks 5-6) is a single static binary built with
// CGO_ENABLED=0, and a cgo-based driver (e.g. mattn/go-sqlite3) would have
// to be ripped out later to get there. Paying that cost now is free; paying
// it after the schema and queries exist would not be.
//
// Only metadata is stored here, never prompt bodies - that mirrors the PRD's
// non-goal of storing prompt bodies by default.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

// Record is one logged proxy request, matching every field the PRD's R6
// requires: timestamp, project, model, input/output tokens, cost, latency,
// status, and stream-vs-not. UsageKnown distinguishes "zero tokens" (a real,
// known-zero cost) from "we could not parse usage from this response" -
// collapsing those two would quietly corrupt spend totals.
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

// schema creates the requests table if it doesn't already exist. Using
// "IF NOT EXISTS" here rather than a migration framework is intentional:
// week 1 has exactly one schema version, and a migration tool is complexity
// with no payoff until the schema actually needs to change.
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

	// SQLite only allows one writer at a time; a single connection avoids
	// "database is locked" errors from the driver trying to parallelize
	// writes across a pool it can't actually serialize itself.
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

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
