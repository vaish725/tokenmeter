package proxy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vaish725/tokenmeter/internal/apikeys"
	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

const testAPIKey = "sk-ant-api03-thisisarealisticlengthtestkeyABCD1234"

// newAPIKeyTestEnv wires an env with testAPIKey's last 8 characters mapped
// to "cognitiveradar" - separate helper since none of the other env
// builders in this package configure an apikeys.Table.
func newAPIKeyTestEnv(t *testing.T, upstreamURL string) *testEnv {
	t.Helper()
	dir := t.TempDir()

	pricingPath := filepath.Join(dir, "pricing.json")
	const pricingJSON = `{"models":{"test-model":{"input_per_mtok":3.0,"output_per_mtok":15.0}}}`
	if err := os.WriteFile(pricingPath, []byte(pricingJSON), 0o644); err != nil {
		t.Fatalf("writing pricing file: %v", err)
	}
	table, err := pricing.Load(pricingPath)
	if err != nil {
		t.Fatalf("pricing.Load() error = %v", err)
	}

	capsPath := filepath.Join(dir, "caps.json")
	const capsJSON = `{"global_daily_cap_usd": 1000, "default_project_cap_usd": 1000, "project_caps_usd": {}}`
	if err := os.WriteFile(capsPath, []byte(capsJSON), 0o644); err != nil {
		t.Fatalf("writing caps file: %v", err)
	}
	ledger, err := budget.New(capsPath)
	if err != nil {
		t.Fatalf("budget.New() error = %v", err)
	}

	apiKeysPath := filepath.Join(dir, "api_keys.json")
	suffix := testAPIKey[len(testAPIKey)-8:]
	apiKeysJSON := `{"projects": {"` + suffix + `": "cognitiveradar"}}`
	if err := os.WriteFile(apiKeysPath, []byte(apiKeysJSON), 0o644); err != nil {
		t.Fatalf("writing api keys file: %v", err)
	}
	ak, err := apikeys.Load(apiKeysPath)
	if err != nil {
		t.Fatalf("apikeys.Load() error = %v", err)
	}

	dbPath := filepath.Join(dir, "meter_test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { st.Close() })

	p, err := NewAnthropic(upstreamURL, st, table, ledger, nil, ak, CaptureConfig{})
	if err != nil {
		t.Fatalf("NewAnthropic() error = %v", err)
	}
	return &testEnv{proxy: p, st: st, dbPath: dbPath}
}

func fakeJSONUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
}

func TestResolveProject_APIKeyAttributesWhenNoHeaderSet(t *testing.T) {
	upstream := fakeJSONUpstream(t)
	defer upstream.Close()
	env := newAPIKeyTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set("x-api-key", testAPIKey)
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	row := env.lastRow(t)
	if row.project != "cognitiveradar" {
		t.Errorf("project = %q, want %q (attributed via the API key)", row.project, "cognitiveradar")
	}
}

func TestResolveProject_APIKeyWorksViaOpenAIBearerHeaderToo(t *testing.T) {
	upstream := fakeJSONUpstream(t)
	defer upstream.Close()
	env := newAPIKeyTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	row := env.lastRow(t)
	if row.project != "cognitiveradar" {
		t.Errorf("project = %q, want %q (attributed via Authorization: Bearer)", row.project, "cognitiveradar")
	}
}

func TestResolveProject_UnrecognizedKeyFallsThroughToUnattributed(t *testing.T) {
	upstream := fakeJSONUpstream(t)
	defer upstream.Close()
	env := newAPIKeyTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set("x-api-key", "sk-ant-api03-someunrelatedkeyZZZZZZZZ")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	row := env.lastRow(t)
	if row.project != unattributedProject {
		t.Errorf("project = %q, want %q for an unmapped key", row.project, unattributedProject)
	}
}

func TestResolveProject_ExplicitHeaderWinsOverAPIKeyMatch(t *testing.T) {
	upstream := fakeJSONUpstream(t)
	defer upstream.Close()
	env := newAPIKeyTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set("x-api-key", testAPIKey) // would map to cognitiveradar
	req.Header.Set(projectHeader, "explicit-override")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	row := env.lastRow(t)
	if row.project != "explicit-override" {
		t.Errorf("project = %q, want %q - R4's explicit header must win over the API-key link", row.project, "explicit-override")
	}
}
