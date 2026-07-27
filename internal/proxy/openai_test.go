package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// newOpenAITestEnv mirrors newTestEnv (anthropic_test.go) but wires an
// OpenAI Proxy - separate helper since the two providers' constructors
// differ, not worth abstracting over for two call sites.
func newOpenAITestEnv(t *testing.T, upstreamURL string) *testEnv {
	t.Helper()

	dir := t.TempDir()

	pricingPath := filepath.Join(dir, "pricing.json")
	const pricingJSON = `{"models":{"test-gpt":{"input_per_mtok":3.0,"output_per_mtok":15.0}}}`
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

	p, err := NewOpenAI(upstreamURL, st, table, ledger)
	if err != nil {
		t.Fatalf("NewOpenAI() error = %v", err)
	}

	return &testEnv{proxy: p, st: st, dbPath: dbPath}
}

func TestOpenAI_NonStreamingHappyPath(t *testing.T) {
	const upstreamBody = `{"id":"chatcmpl_1","model":"test-gpt","usage":{"prompt_tokens":100,"completion_tokens":50}}`

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	env := newOpenAITestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-gpt", "stream": false})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "infra-mind")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != upstreamBody {
		t.Fatalf("body = %q, want passthrough of %q", w.Body.String(), upstreamBody)
	}

	row := env.lastRow(t)
	if row.provider != "openai" {
		t.Errorf("provider = %q, want %q", row.provider, "openai")
	}
	if row.inputTokens != 100 || row.outputTokens != 50 {
		t.Errorf("tokens = %d/%d, want 100/50", row.inputTokens, row.outputTokens)
	}
}

func TestOpenAI_StreamOptionsInjectedWhenMissing(t *testing.T) {
	var receivedBody map[string]any

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &receivedBody)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	env := newOpenAITestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-gpt", "stream": true})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	streamOptions, ok := receivedBody["stream_options"].(map[string]any)
	if !ok {
		t.Fatalf("upstream never received stream_options, got body = %+v", receivedBody)
	}
	if streamOptions["include_usage"] != true {
		t.Errorf("stream_options.include_usage = %v, want true (auto-injected)", streamOptions["include_usage"])
	}
}

func TestOpenAI_StreamOptionsNotOverriddenWhenAlreadySet(t *testing.T) {
	var receivedBody map[string]any

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &receivedBody)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	env := newOpenAITestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{
		"model": "test-gpt", "stream": true,
		"stream_options": map[string]any{"include_usage": false},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	streamOptions, ok := receivedBody["stream_options"].(map[string]any)
	if !ok {
		t.Fatalf("stream_options missing from upstream body entirely, got %+v", receivedBody)
	}
	if streamOptions["include_usage"] != false {
		t.Errorf("stream_options.include_usage = %v, want false (caller's own choice must not be overridden)", streamOptions["include_usage"])
	}
}
