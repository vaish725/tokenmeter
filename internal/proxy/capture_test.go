package proxy

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// newCaptureTestEnv is newTestEnvWithCaps's shape, but lets each capture
// test pick its own CaptureConfig instead of the shared helper's hardcoded
// disabled default.
func newCaptureTestEnv(t *testing.T, upstreamURL string, cc CaptureConfig) *testEnv {
	t.Helper()

	dir := t.TempDir()

	pricingPath := filepath.Join(dir, "pricing.json")
	const pricingJSON = `{"models": {"test-model": {"input_per_mtok": 3.0, "output_per_mtok": 15.0}}}`
	if err := os.WriteFile(pricingPath, []byte(pricingJSON), 0o644); err != nil {
		t.Fatalf("writing test pricing file: %v", err)
	}
	table, err := pricing.Load(pricingPath)
	if err != nil {
		t.Fatalf("pricing.Load() error = %v", err)
	}

	capsPath := filepath.Join(dir, "caps.json")
	const capsJSON = `{"global_daily_cap_usd": 1000, "default_project_cap_usd": 1000, "project_caps_usd": {}}`
	if err := os.WriteFile(capsPath, []byte(capsJSON), 0o644); err != nil {
		t.Fatalf("writing test caps file: %v", err)
	}
	ledger, err := budget.New(capsPath)
	if err != nil {
		t.Fatalf("budget.New() error = %v", err)
	}

	dbPath := filepath.Join(dir, "meter_test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { st.Close() })

	p, err := NewAnthropic(upstreamURL, st, table, ledger, nil, nil, cc)
	if err != nil {
		t.Fatalf("NewAnthropic() error = %v", err)
	}

	return &testEnv{proxy: p, st: st, dbPath: dbPath}
}

// lastPromptCapture reads back the most recently inserted prompt_captures
// row, linked to the most recent requests row via request_id.
func (e *testEnv) lastPromptCapture(t *testing.T) (body string, truncated int, found bool) {
	t.Helper()

	db, err := sql.Open("sqlite", e.dbPath)
	if err != nil {
		t.Fatalf("opening db for assertions: %v", err)
	}
	defer db.Close()

	err = db.QueryRow(`
		SELECT pc.body, pc.truncated FROM prompt_captures pc
		JOIN requests r ON r.id = pc.request_id
		ORDER BY pc.id DESC LIMIT 1
	`).Scan(&body, &truncated)
	if err == sql.ErrNoRows {
		return "", 0, false
	}
	if err != nil {
		t.Fatalf("reading prompt capture: %v", err)
	}
	return body, truncated, true
}

func upstreamOK(usageJSON string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, usageJSON)
	}))
}

func TestHandleMessages_CaptureEnabled_StoresPromptBody(t *testing.T) {
	upstream := upstreamOK(`{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":5}}`)
	defer upstream.Close()

	env := newCaptureTestEnv(t, upstream.URL, CaptureConfig{Enabled: true, MaxBytes: 1024})

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model", "stream": false, "prompt": "hello there"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "cap-test")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	body, truncated, found := env.lastPromptCapture(t)
	if !found {
		t.Fatal("no prompt_captures row was written for a successful request with capture enabled")
	}
	if body != string(reqBody) {
		t.Errorf("captured body = %q, want %q", body, string(reqBody))
	}
	if truncated != 0 {
		t.Errorf("truncated = %d, want 0 (body was under MaxBytes)", truncated)
	}
}

func TestHandleMessages_CaptureEnabled_TruncatesOversizedBody(t *testing.T) {
	upstream := upstreamOK(`{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":5}}`)
	defer upstream.Close()

	const maxBytes = 20
	env := newCaptureTestEnv(t, upstream.URL, CaptureConfig{Enabled: true, MaxBytes: maxBytes})

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model", "prompt": "this prompt body is deliberately long enough to exceed the configured capture limit"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "cap-test")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	body, truncated, found := env.lastPromptCapture(t)
	if !found {
		t.Fatal("no prompt_captures row was written")
	}
	if truncated != 1 {
		t.Errorf("truncated = %d, want 1 (body exceeded MaxBytes)", truncated)
	}
	if len(body) != maxBytes {
		t.Errorf("captured body length = %d, want %d", len(body), maxBytes)
	}
}

func TestHandleMessages_CaptureEnabled_RejectedRequestNotCaptured(t *testing.T) {
	upstream := upstreamOK(`{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":5}}`)
	defer upstream.Close()

	env := newCaptureTestEnv(t, upstream.URL, CaptureConfig{Enabled: true, MaxBytes: 1024})
	// The project's own cap is $0 - the very first request must be rejected
	// before ever reaching the upstream.
	const overCapJSON = `{"global_daily_cap_usd": 1000, "default_project_cap_usd": 0, "project_caps_usd": {}}`
	if err := os.WriteFile(filepath.Join(filepath.Dir(env.dbPath), "caps.json"), []byte(overCapJSON), 0o644); err != nil {
		t.Fatalf("writing caps file: %v", err)
	}
	ledger, err := budget.New(filepath.Join(filepath.Dir(env.dbPath), "caps.json"))
	if err != nil {
		t.Fatalf("budget.New() error = %v", err)
	}
	env.proxy.budget = ledger

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model", "prompt": "should never reach upstream"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "cap-test")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}

	if _, _, found := env.lastPromptCapture(t); found {
		t.Error("a rejected (429) request must never produce a prompt_captures row - there is no call to associate it with")
	}
}

func TestHandleMessages_CaptureDisabled_NothingStored(t *testing.T) {
	upstream := upstreamOK(`{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":5}}`)
	defer upstream.Close()

	env := newCaptureTestEnv(t, upstream.URL, CaptureConfig{}) // zero value: disabled

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model", "prompt": "hello"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "cap-test")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if _, _, found := env.lastPromptCapture(t); found {
		t.Error("capture is disabled by default (zero value CaptureConfig) - no row should have been written")
	}
}
