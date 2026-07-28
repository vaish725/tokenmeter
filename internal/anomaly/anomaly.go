// Package anomaly implements the "alert when a project's hourly rate
// exceeds its trailing-7-day p95" check. Always-on, not opt-in: unlike
// downshift or prompt capture, this never changes request handling, only
// logs a warning line - there's no behavior to guard by default.
package anomaly

import (
	"context"
	"log"
	"math"
	"sort"
	"time"

	"github.com/vaish725/tokenmeter/internal/store"
)

const (
	checkInterval = 10 * time.Minute
	lookback      = 7 * 24 * time.Hour
	hoursInWeek   = 7 * 24
)

// Checker periodically compares each active project's current-hour spend
// against its trailing 7-day p95, logging a warning when it's exceeded.
type Checker struct {
	store *store.Store
}

// NewChecker builds a Checker reading from st.
func NewChecker(st *store.Store) *Checker {
	return &Checker{store: st}
}

// Run blocks, checking on a 10-minute ticker until ctx is canceled -
// intended to be started as its own goroutine from runServe.
func (c *Checker) Run(ctx context.Context) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkAll(ctx)
		}
	}
}

func (c *Checker) checkAll(ctx context.Context) {
	now := time.Now()
	projects, err := c.store.ActiveProjects(ctx, now.Add(-lookback))
	if err != nil {
		log.Printf("meter: anomaly check: listing active projects: %v", err)
		return
	}
	for _, project := range projects {
		anomalous, current, p95, err := c.checkProject(ctx, project, now)
		if err != nil {
			log.Printf("meter: anomaly check: %s: %v", project, err)
			continue
		}
		if anomalous {
			log.Printf("meter: cost anomaly: project %q is at $%.4f this hour, above its trailing 7-day p95 of $%.4f", project, current, p95)
		}
	}
}

// checkProject compares project's current-hour spend to its trailing
// 7-day p95. A p95 of exactly 0 (a project with no history yet, or a
// genuinely idle trailing week) never counts as anomalous - otherwise a
// brand new project would trip this on its very first request, which is
// normal use, not a spike.
func (c *Checker) checkProject(ctx context.Context, project string, now time.Time) (anomalous bool, current, p95 float64, err error) {
	hourly, err := c.store.HourlySpend(ctx, project, now.Add(-lookback))
	if err != nil {
		return false, 0, 0, err
	}

	nowUTC := now.UTC()
	current = hourly[nowUTC.Truncate(time.Hour).Format("2006-01-02T15")]

	// Missing hours count as $0, not excluded from the sample - a quiet
	// week is real signal about what "normal" looks like for this project.
	values := make([]float64, 0, hoursInWeek)
	for h := 0; h < hoursInWeek; h++ {
		key := nowUTC.Add(-time.Duration(h) * time.Hour).Truncate(time.Hour).Format("2006-01-02T15")
		values = append(values, hourly[key])
	}

	p95 = computeP95(values)
	return p95 > 0 && current > p95, current, p95, nil
}

// computeP95 returns the 95th percentile via the nearest-rank method: sort
// ascending, take the value at ceil(0.95*n)-1. Simple and standard for a
// heuristic alert threshold, not statistically exact interpolation.
func computeP95(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	rank := int(math.Ceil(0.95*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}
