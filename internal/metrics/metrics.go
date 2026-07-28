// Package metrics formats meter's request history as Prometheus text
// exposition format for the /metrics endpoint. Hand-written, not the
// official client_golang library: every value here is computed fresh from
// the store on each scrape (see store.MetricsSnapshot), not a live
// in-process counter, so there's no library-managed state that would
// justify pulling in a real dependency for it.
package metrics

import (
	"fmt"
	"strings"

	"github.com/vaish725/tokenmeter/internal/store"
)

// rollupKey groups the coarser cost/token metrics below - project and
// provider only, not model/status, so a project's total isn't split
// across every model it happened to use.
type rollupKey struct{ project, provider string }

// Format renders rows (from store.MetricsSnapshot) as Prometheus text
// exposition format. Computed fresh on every call rather than cached:
// since requests are only ever inserted, never deleted, this gives
// genuinely monotonic counters that survive a meter restart, which an
// in-memory counter couldn't offer for free.
func Format(rows []store.MetricsRow) string {
	var b strings.Builder

	writeHeader(&b, "meter_requests_total", "Total proxied requests.")
	for _, r := range rows {
		fmt.Fprintf(&b, "meter_requests_total{project=%s,provider=%s,model=%s,status=%s} %d\n",
			escapeLabel(r.Project), escapeLabel(r.Provider), escapeLabel(r.Model), escapeLabel(fmt.Sprint(r.StatusCode)), r.Count)
	}

	cost := map[rollupKey]float64{}
	inputTokens := map[rollupKey]int64{}
	outputTokens := map[rollupKey]int64{}
	for _, r := range rows {
		k := rollupKey{r.Project, r.Provider}
		cost[k] += r.CostUSD
		inputTokens[k] += r.InputTokens
		outputTokens[k] += r.OutputTokens
	}

	writeHeader(&b, "meter_cost_usd_total", "Total cost in USD.")
	for k, v := range cost {
		fmt.Fprintf(&b, "meter_cost_usd_total{project=%s,provider=%s} %v\n", escapeLabel(k.project), escapeLabel(k.provider), v)
	}

	writeHeader(&b, "meter_tokens_total", "Total tokens, by direction.")
	for k, v := range inputTokens {
		fmt.Fprintf(&b, "meter_tokens_total{project=%s,provider=%s,direction=\"input\"} %d\n", escapeLabel(k.project), escapeLabel(k.provider), v)
	}
	for k, v := range outputTokens {
		fmt.Fprintf(&b, "meter_tokens_total{project=%s,provider=%s,direction=\"output\"} %d\n", escapeLabel(k.project), escapeLabel(k.provider), v)
	}

	return b.String()
}

func writeHeader(b *strings.Builder, name, help string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
}

// escapeLabel quotes a Prometheus label value. %q is a strict superset of
// what the text format requires escaped (backslash, quote, newline) - a
// project name is ultimately caller-controlled (X-Meter-Project), so this
// is what keeps a crafted value from producing malformed scrape output.
func escapeLabel(s string) string {
	return fmt.Sprintf("%q", s)
}
