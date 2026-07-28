package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/downshift"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// newDownshiftTestEnv wires expensive-model -> cheap-model (huge price gap,
// so estimateCost makes the fit-or-not outcome obvious regardless of exact
// body length) with a configurable project cap.
func newDownshiftTestEnv(t *testing.T, upstreamURL string, projectCapUSD float64) *testEnv {
	t.Helper()
	dir := t.TempDir()

	pricingPath := filepath.Join(dir, "pricing.json")
	const pricingJSON = `{"models":{
		"expensive-model": {"input_per_mtok": 1000000, "output_per_mtok": 1000000},
		"cheap-model": {"input_per_mtok": 0.001, "output_per_mtok": 0.001}
	}}`
	if err := os.WriteFile(pricingPath, []byte(pricingJSON), 0o644); err != nil {
		t.Fatalf("writing pricing file: %v", err)
	}
	table, err := pricing.Load(pricingPath)
	if err != nil {
		t.Fatalf("pricing.Load() error = %v", err)
	}

	capsPath := filepath.Join(dir, "caps.json")
	capsJSON := fmt.Sprintf(`{"global_daily_cap_usd": 1000, "default_project_cap_usd": %f, "project_caps_usd": {}}`, projectCapUSD)
	if err := os.WriteFile(capsPath, []byte(capsJSON), 0o644); err != nil {
		t.Fatalf("writing caps file: %v", err)
	}
	ledger, err := budget.New(capsPath)
	if err != nil {
		t.Fatalf("budget.New() error = %v", err)
	}

	downshiftPath := filepath.Join(dir, "downshift.json")
	const downshiftJSON = `{"substitutes": {"expensive-model": "cheap-model"}}`
	if err := os.WriteFile(downshiftPath, []byte(downshiftJSON), 0o644); err != nil {
		t.Fatalf("writing downshift file: %v", err)
	}
	dt, err := downshift.Load(downshiftPath)
	if err != nil {
		t.Fatalf("downshift.Load() error = %v", err)
	}

	dbPath := filepath.Join(dir, "meter_test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { st.Close() })

	p, err := NewAnthropic(upstreamURL, st, table, ledger, dt, nil, CaptureConfig{})
	if err != nil {
		t.Fatalf("NewAnthropic() error = %v", err)
	}
	return &testEnv{proxy: p, st: st, dbPath: dbPath}
}

func TestDownshift_SucceedsAndAnnotatesResponse(t *testing.T) {
	var receivedModel string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		json.Unmarshal(body, &req)
		receivedModel, _ = req["model"].(string)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	// $1 project cap: comfortably above cheap-model's near-zero estimate,
	// comfortably below expensive-model's (dominated by max_tokens*$1/token).
	env := newDownshiftTestEnv(t, upstream.URL, 1.0)

	reqBody, _ := json.Marshal(map[string]any{"model": "expensive-model", "max_tokens": 100})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (downshift should let this through)", w.Code)
	}
	if receivedModel != "cheap-model" {
		t.Errorf("upstream received model = %q, want %q", receivedModel, "cheap-model")
	}
	if got := w.Header().Get("X-Meter-Downshifted-From"); got != "expensive-model" {
		t.Errorf("X-Meter-Downshifted-From = %q, want %q", got, "expensive-model")
	}
	if got := w.Header().Get("X-Meter-Downshifted-To"); got != "cheap-model" {
		t.Errorf("X-Meter-Downshifted-To = %q, want %q", got, "cheap-model")
	}

	row := env.lastRow(t)
	if row.model != "cheap-model" {
		t.Errorf("persisted model = %q, want %q (the model actually called)", row.model, "cheap-model")
	}
}

func TestDownshift_SubstituteAlsoOverCapFallsThroughTo429(t *testing.T) {
	var upstreamHit bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	// Cap of 0: even cheap-model's near-zero estimate doesn't fit.
	env := newDownshiftTestEnv(t, upstream.URL, 0)

	reqBody, _ := json.Marshal(map[string]any{"model": "expensive-model", "max_tokens": 100})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if upstreamHit {
		t.Fatal("upstream was hit even though the substitute was also over cap")
	}
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}
	if got := w.Header().Get("X-Meter-Downshifted-From"); got != "" {
		t.Errorf("X-Meter-Downshifted-From = %q, want unset for a request that was never actually downshifted", got)
	}
}
