// Package budget implements reserve-then-reconcile daily spend caps.
// Checking "completed spend < cap" is racy under concurrency; Reserve
// atomically commits a pessimistic estimate before the call, and Reconcile
// swaps it for the real cost afterward.
package budget

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// defaultOrphanTimeout bounds how long a reservation can sit un-reconciled
// (a crash, a bug) before it's swept and released as unspent.
const defaultOrphanTimeout = 30 * time.Minute

// Caps is the loaded configs/caps.json: a global daily ceiling, a default
// per-project ceiling, and optional per-project overrides.
type Caps struct {
	GlobalDailyCapUSD    float64            `json:"global_daily_cap_usd"`
	DefaultProjectCapUSD float64            `json:"default_project_cap_usd"`
	ProjectCapsUSD       map[string]float64 `json:"project_caps_usd"`
}

// ProjectCap returns project's daily cap: its own override if configured,
// otherwise the default. Exported for meter watch and downshift re-estimates,
// which need cap values outside the Ledger that enforces them.
func (c *Caps) ProjectCap(project string) float64 {
	if v, ok := c.ProjectCapsUSD[project]; ok {
		return v
	}
	return c.DefaultProjectCapUSD
}

// Decision describes why Reserve rejected a request - exactly what the
// proxy needs to build a structured 429 body.
type Decision struct {
	Cap      string // "project" or "global"
	LimitUSD float64
	UsedUSD  float64
	ResetAt  time.Time
}

// bucket is one day's committed (reconciled) and reserved (pending) spend.
// day is a "2006-01-02" local-date key; a mismatch means it's stale and
// gets zeroed before use.
type bucket struct {
	day       string
	committed float64
	reserved  float64
}

func (b *bucket) resetIfStale(today string) {
	if b.day != today {
		b.day = today
		b.committed = 0
		b.reserved = 0
	}
}

type reservation struct {
	project       string
	estimatedCost float64
	createdAt     time.Time
}

// Ledger is the in-process, mutex-guarded budget store for one meter
// process. State is in memory only - it doesn't survive a restart.
type Ledger struct {
	mu            sync.Mutex
	caps          Caps
	global        bucket
	projects      map[string]*bucket
	reservations  map[string]*reservation
	orphanTimeout time.Duration // overridable in tests; defaultOrphanTimeout otherwise
}

// LoadCaps reads and parses configs/caps.json. Exported so callers that
// only need cap values (meter watch, downshift re-estimates) don't have to
// build a whole Ledger to get them.
func LoadCaps(path string) (Caps, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Caps{}, fmt.Errorf("budget: reading %s: %w", path, err)
	}
	var caps Caps
	if err := json.Unmarshal(data, &caps); err != nil {
		return Caps{}, fmt.Errorf("budget: parsing %s: %w", path, err)
	}
	return caps, nil
}

// New loads caps from path and returns a ready-to-use Ledger.
func New(path string) (*Ledger, error) {
	caps, err := LoadCaps(path)
	if err != nil {
		return nil, err
	}
	return &Ledger{
		caps:          caps,
		projects:      make(map[string]*bucket),
		reservations:  make(map[string]*reservation),
		orphanTimeout: defaultOrphanTimeout,
	}, nil
}

func todayKey(t time.Time) string {
	return t.In(time.Local).Format("2006-01-02")
}

func nextMidnight(t time.Time) time.Time {
	t = t.In(time.Local)
	y, m, d := t.Date()
	return time.Date(y, m, d+1, 0, 0, 0, 0, t.Location())
}

// Reserve atomically checks estimatedCost against both the project's and
// the global cap and, if both allow it, commits the reservation. ok=false
// means nothing was reserved - the request must not be forwarded.
func (l *Ledger) Reserve(project string, estimatedCost float64) (id string, ok bool, decision Decision) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	today := todayKey(now)
	l.expireOrphans(now)

	l.global.resetIfStale(today)
	proj := l.projectBucket(project)
	proj.resetIfStale(today)

	projectCap := l.caps.ProjectCap(project)
	if used := proj.committed + proj.reserved + estimatedCost; used > projectCap {
		return "", false, Decision{Cap: "project", LimitUSD: projectCap, UsedUSD: used, ResetAt: nextMidnight(now)}
	}
	if used := l.global.committed + l.global.reserved + estimatedCost; used > l.caps.GlobalDailyCapUSD {
		return "", false, Decision{Cap: "global", LimitUSD: l.caps.GlobalDailyCapUSD, UsedUSD: used, ResetAt: nextMidnight(now)}
	}

	id = newReservationID()
	l.reservations[id] = &reservation{project: project, estimatedCost: estimatedCost, createdAt: now}
	proj.reserved += estimatedCost
	l.global.reserved += estimatedCost
	return id, true, Decision{}
}

// Reconcile swaps a reservation's estimated cost for its real one. A
// missing id (already expired as an orphan) is a no-op, not an error.
func (l *Ledger) Reconcile(id string, actualCost float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	res, ok := l.reservations[id]
	if !ok {
		return
	}
	delete(l.reservations, id)

	today := todayKey(time.Now())
	l.global.resetIfStale(today)
	proj := l.projectBucket(res.project)
	proj.resetIfStale(today)

	// A reservation spanning midnight finds its bucket already reset;
	// clamping to 0 turns that into a harmless no-op, not a negative balance.
	l.global.reserved = maxFloat(0, l.global.reserved-res.estimatedCost)
	l.global.committed += actualCost
	proj.reserved = maxFloat(0, proj.reserved-res.estimatedCost)
	proj.committed += actualCost
}

// expireOrphans releases reservations that outlived orphanTimeout without
// ever being reconciled, so a lost reconcile can't permanently eat budget.
func (l *Ledger) expireOrphans(now time.Time) {
	for id, res := range l.reservations {
		if now.Sub(res.createdAt) <= l.orphanTimeout {
			continue
		}
		delete(l.reservations, id)
		l.global.reserved = maxFloat(0, l.global.reserved-res.estimatedCost)
		if proj, ok := l.projects[res.project]; ok {
			proj.reserved = maxFloat(0, proj.reserved-res.estimatedCost)
		}
	}
}

func (l *Ledger) projectBucket(project string) *bucket {
	b, ok := l.projects[project]
	if !ok {
		b = &bucket{}
		l.projects[project] = b
	}
	return b
}

// Snapshot returns a project's committed (reconciled) and reserved
// (pending) spend for today - for tests and future observability (e.g.
// `meter watch`).
func (l *Ledger) Snapshot(project string) (committed, reserved float64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	proj := l.projectBucket(project)
	proj.resetIfStale(todayKey(time.Now()))
	return proj.committed, proj.reserved
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func newReservationID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
