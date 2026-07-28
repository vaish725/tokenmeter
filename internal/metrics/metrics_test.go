package metrics

import (
	"strings"
	"testing"

	"github.com/vaish725/tokenmeter/internal/store"
)

func TestFormat_RequestsAndRollups(t *testing.T) {
	rows := []store.MetricsRow{
		{Project: "a", Provider: "anthropic", Model: "claude-sonnet-5", StatusCode: 200, Count: 3, CostUSD: 1.5, InputTokens: 100, OutputTokens: 50},
		{Project: "a", Provider: "anthropic", Model: "claude-haiku-4-5", StatusCode: 200, Count: 2, CostUSD: 0.5, InputTokens: 20, OutputTokens: 10},
		{Project: "a", Provider: "anthropic", Model: "claude-sonnet-5", StatusCode: 429, Count: 1, CostUSD: 0, InputTokens: 0, OutputTokens: 0},
	}

	out := Format(rows)

	// Per-(project,provider,model,status) request counters, unrolled.
	wantLines := []string{
		`meter_requests_total{project="a",provider="anthropic",model="claude-sonnet-5",status="200"} 3`,
		`meter_requests_total{project="a",provider="anthropic",model="claude-haiku-4-5",status="200"} 2`,
		`meter_requests_total{project="a",provider="anthropic",model="claude-sonnet-5",status="429"} 1`,
		// Cost and tokens rolled up to (project, provider), summed across
		// the three rows above (1.5 + 0.5 + 0 = 2, tokens likewise).
		`meter_cost_usd_total{project="a",provider="anthropic"} 2`,
		`meter_tokens_total{project="a",provider="anthropic",direction="input"} 120`,
		`meter_tokens_total{project="a",provider="anthropic",direction="output"} 60`,
	}
	for _, want := range wantLines {
		if !strings.Contains(out, want) {
			t.Errorf("output missing line %q\nfull output:\n%s", want, out)
		}
	}

	for _, name := range []string{"meter_requests_total", "meter_cost_usd_total", "meter_tokens_total"} {
		if !strings.Contains(out, "# HELP "+name) || !strings.Contains(out, "# TYPE "+name+" counter") {
			t.Errorf("missing HELP/TYPE header for %s", name)
		}
	}
}

func TestFormat_EscapesLabelValues(t *testing.T) {
	rows := []store.MetricsRow{
		{Project: `weird"project` + "\n", Provider: "anthropic", Model: "m", StatusCode: 200, Count: 1},
	}

	out := Format(rows)

	// A raw, unescaped quote or newline inside a label value would produce
	// invalid Prometheus text format - the escaped form must appear instead.
	if strings.Contains(out, `project="weird"project`) {
		t.Error("label value was not escaped - output would be invalid Prometheus text format")
	}
	if !strings.Contains(out, `project="weird\"project\n"`) {
		t.Errorf("expected an escaped label value in output:\n%s", out)
	}
}

func TestFormat_EmptyRowsStillProducesValidHeaders(t *testing.T) {
	out := Format(nil)
	if !strings.Contains(out, "# TYPE meter_requests_total counter") {
		t.Error("expected valid metric headers even with no data yet")
	}
}
