package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// testEnv wires up a real Store and a real pricing.Table against temp files,
// so these tests exercise the same code paths meter runs in production
// rather than mocks standing in for them.
type testEnv struct {
	proxy  *Proxy
	st     *store.Store
	dbPath string
}

// newTestEnv wires an env with generous caps, so existing tests aren't
// affected by budget rejection; newTestEnvWithCaps is for cap-specific tests.
func newTestEnv(t *testing.T, upstreamURL string) *testEnv {
	t.Helper()
	return newTestEnvWithCaps(t, upstreamURL, 1000, 1000)
}

func newTestEnvWithCaps(t *testing.T, upstreamURL string, globalCapUSD, projectCapUSD float64) *testEnv {
	t.Helper()

	dir := t.TempDir()

	pricingPath := filepath.Join(dir, "pricing.json")
	const pricingJSON = `{
		"models": {
			"test-model": {"input_per_mtok": 3.0, "output_per_mtok": 15.0}
		}
	}`
	if err := os.WriteFile(pricingPath, []byte(pricingJSON), 0o644); err != nil {
		t.Fatalf("writing test pricing file: %v", err)
	}
	table, err := pricing.Load(pricingPath)
	if err != nil {
		t.Fatalf("pricing.Load() error = %v", err)
	}

	capsPath := filepath.Join(dir, "caps.json")
	capsJSON := fmt.Sprintf(`{"global_daily_cap_usd": %f, "default_project_cap_usd": %f, "project_caps_usd": {}}`, globalCapUSD, projectCapUSD)
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

	p, err := NewAnthropic(upstreamURL, st, table, ledger, nil, nil)
	if err != nil {
		t.Fatalf("NewAnthropic() error = %v", err)
	}

	return &testEnv{
		proxy:  p,
		st:     st,
		dbPath: dbPath,
	}
}

// requestRow is what tests read back via a direct SQL query, independent
// of the store.TopRequests query path used by `meter top`.
type requestRow struct {
	project      string
	provider     string
	model        string
	inputTokens  int
	outputTokens int
	costUSD      float64
	statusCode   int
	usageKnown   int
}

func (e *testEnv) lastRow(t *testing.T) requestRow {
	t.Helper()

	// modernc.org/sqlite registers driver name "sqlite"; it's already
	// registered process-wide via the store package's blank import.
	db, err := sql.Open("sqlite", e.dbPath)
	if err != nil {
		t.Fatalf("opening db for assertions: %v", err)
	}
	defer db.Close()

	var row requestRow
	err = db.QueryRow(`
		SELECT project, provider, model, input_tokens, output_tokens, cost_usd, status_code, usage_known
		FROM requests ORDER BY id DESC LIMIT 1
	`).Scan(&row.project, &row.provider, &row.model, &row.inputTokens, &row.outputTokens, &row.costUSD, &row.statusCode, &row.usageKnown)
	if err != nil {
		t.Fatalf("reading last row: %v", err)
	}
	return row
}

func TestHandleMessages_NonStreamingHappyPath(t *testing.T) {
	const upstreamBody = `{"id":"msg_1","model":"test-model","usage":{"input_tokens":100,"output_tokens":50}}`

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	env := newTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model", "stream": false})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "cognitiveradar")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != upstreamBody {
		t.Fatalf("body = %q, want %q (response must pass through unmodified)", w.Body.String(), upstreamBody)
	}

	row := env.lastRow(t)
	if row.project != "cognitiveradar" {
		t.Errorf("project = %q, want %q", row.project, "cognitiveradar")
	}
	if row.provider != "anthropic" {
		t.Errorf("provider = %q, want %q", row.provider, "anthropic")
	}
	if row.inputTokens != 100 || row.outputTokens != 50 {
		t.Errorf("tokens = %d/%d, want 100/50", row.inputTokens, row.outputTokens)
	}
	// Tolerance, not "==": constant-folded and runtime float64 arithmetic
	// can differ in the last bit despite both being correct.
	wantCost := (100.0/1_000_000)*3.0 + (50.0/1_000_000)*15.0
	if math.Abs(row.costUSD-wantCost) > 1e-9 {
		t.Errorf("cost = %v, want %v", row.costUSD, wantCost)
	}
	if row.usageKnown != 1 {
		t.Errorf("usage_known = %d, want 1", row.usageKnown)
	}
}

func TestHandleMessages_MalformedUsageBody_StillSucceeds(t *testing.T) {
	// Upstream claims JSON but sends garbage. The PRD's own risk mitigation
	// says a parse failure here must record usage_unknown, not fail the call.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not valid json"))
	}))
	defer upstream.Close()

	env := newTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (a broken usage block must not fail the call)", w.Code)
	}
	if w.Body.String() != "not valid json" {
		t.Fatalf("body = %q, want passthrough of the raw upstream bytes", w.Body.String())
	}

	row := env.lastRow(t)
	if row.usageKnown != 0 {
		t.Errorf("usage_known = %d, want 0", row.usageKnown)
	}
	if row.inputTokens != 0 || row.outputTokens != 0 || row.costUSD != 0 {
		t.Errorf("tokens/cost = %d/%d/%v, want 0/0/0 when usage is unparseable", row.inputTokens, row.outputTokens, row.costUSD)
	}
	if row.project != unattributedProject {
		t.Errorf("project = %q, want %q (no header was set)", row.project, unattributedProject)
	}
}

func TestHandleMessages_ClientDisconnect_CancelsUpstreamCall(t *testing.T) {
	upstreamHit := make(chan struct{})
	upstreamCanceled := make(chan struct{})

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Draining the body first matters: Go's server can only detect the
		// client disconnecting once no unread body bytes remain.
		io.Copy(io.Discard, r.Body)
		close(upstreamHit)
		<-r.Context().Done() // blocks until the client's cancellation propagates here
		close(upstreamCanceled)
	}))
	defer upstream.Close()

	env := newTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody)).WithContext(ctx)
	w := httptest.NewRecorder()

	handlerDone := make(chan struct{})
	go func() {
		env.proxy.HandleMessages(w, req)
		close(handlerDone)
	}()

	select {
	case <-upstreamHit:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never received the request")
	}

	cancel() // simulate the client closing the connection mid-request

	select {
	case <-upstreamCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellation never propagated to the upstream call")
	}

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("HandleMessages did not return after upstream cancellation")
	}

	row := env.lastRow(t)
	if row.usageKnown != 0 {
		t.Errorf("usage_known = %d, want 0 for an aborted request", row.usageKnown)
	}
	if row.statusCode != 0 {
		t.Errorf("status_code = %d, want 0 (no response was ever received)", row.statusCode)
	}
}

func TestHandleMessages_StreamingIsNotFullyBuffered(t *testing.T) {
	const event1 = "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n"
	const event2 = "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":25}}\n\n"
	const upstreamDelay = 150 * time.Millisecond

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		w.Write([]byte(event1))
		flusher.Flush()
		time.Sleep(upstreamDelay)
		w.Write([]byte(event2))
		flusher.Flush()
	}))
	defer upstream.Close()

	env := newTestEnv(t, upstream.URL)

	// A real server, not a ResponseRecorder: proving events arrive
	// progressively needs a real connection read incrementally.
	meterServer := httptest.NewServer(http.HandlerFunc(env.proxy.HandleMessages))
	defer meterServer.Close()

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model", "stream": true})

	start := time.Now()
	resp, err := http.Post(meterServer.URL, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST to meter failed: %v", err)
	}
	defer resp.Body.Close()

	var all []byte
	var firstByteAt time.Duration
	buf := make([]byte, 256)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if len(all) == 0 {
				firstByteAt = time.Since(start)
			}
			all = append(all, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading response body: %v", err)
		}
	}
	totalAt := time.Since(start)

	// The actual proof: a fully-buffered implementation would only hand
	// back bytes once the upstream finished, so firstByteAt would be close
	// to totalAt instead of close to zero.
	if firstByteAt > upstreamDelay/2 {
		t.Errorf("first event arrived after %v, want well under the %v upstream delay before the second event - looks like the response is being buffered before forwarding", firstByteAt, upstreamDelay)
	}
	if totalAt < upstreamDelay {
		t.Errorf("total response time %v was less than the upstream's own %v delay - test setup issue", totalAt, upstreamDelay)
	}

	want := event1 + event2
	if string(all) != want {
		t.Fatalf("body mismatch:\ngot:  %q\nwant: %q", all, want)
	}

	row := env.lastRow(t)
	if row.inputTokens != 10 || row.outputTokens != 25 {
		t.Errorf("tokens = %d/%d, want 10/25", row.inputTokens, row.outputTokens)
	}
	if row.usageKnown != 1 {
		t.Error("usage_known = 0, want 1")
	}
}

func TestHandleMessages_ProjectHeaderNotForwardedUpstream(t *testing.T) {
	var headerPresent bool

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerPresent = r.Header.Get(projectHeader) != ""
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	env := newTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "shouldnotleak")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if headerPresent {
		t.Errorf("%s reached the upstream, want it stripped before forwarding (it's meter-internal)", projectHeader)
	}
}

// TestHandleMessages_UpstreamNeverAsksForCompression guards against a real
// bug found during dogfooding: relaying the client's own Accept-Encoding
// header made Go's Transport skip its usual transparent gzip decompression,
// so a real provider's compressed response arrived unreadable as JSON to
// the usage scanner - readable fine by the client's own HTTP library, but
// silently zeroing out cost tracking (status 200, cost $0, usage_known=0).
// This fake upstream behaves like a real compression-aware server: it
// actually gzips the response if asked to, so the test only passes if
// meter no longer asks for it.
func TestHandleMessages_UpstreamNeverAsksForCompression(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const body = `{"usage":{"input_tokens":10,"output_tokens":5}}`
		w.Header().Set("Content-Type", "application/json")

		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			w.WriteHeader(http.StatusOK)
			gz := gzip.NewWriter(w)
			gz.Write([]byte(body))
			gz.Close()
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	defer upstream.Close()

	env := newTestEnv(t, upstream.URL)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	// Simulate a real client that advertises compression support, like
	// virtually every production HTTP client does by default.
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if w.Header().Get("Content-Encoding") == "gzip" {
		t.Fatal("client received a gzip-encoded response - the upstream compressed it, meaning Accept-Encoding was not overridden")
	}

	row := env.lastRow(t)
	if row.usageKnown != 1 {
		t.Error("usage_known = 0, want 1 - the response must have arrived compressed and unreadable to the usage scanner")
	}
	if row.inputTokens != 10 || row.outputTokens != 5 {
		t.Errorf("tokens = %d/%d, want 10/5", row.inputTokens, row.outputTokens)
	}
}

func TestHandleMessages_OverCapRejectsBeforeReachingUpstream(t *testing.T) {
	var upstreamHit bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	// Project cap of 0: any nonzero estimate is rejected, deterministically.
	env := newTestEnvWithCaps(t, upstream.URL, 1000, 0)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model", "max_tokens": 100})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "tight-project")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if upstreamHit {
		t.Fatal("upstream was hit despite being over cap - reservation must happen before forwarding")
	}
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}

	var body struct {
		Error   string `json:"error"`
		Cap     string `json:"cap"`
		Project string `json:"project"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding 429 body: %v", err)
	}
	if body.Error != "budget_exceeded" || body.Cap != "project" || body.Project != "tight-project" {
		t.Errorf("body = %+v, want error=budget_exceeded cap=project project=tight-project", body)
	}

	row := env.lastRow(t)
	if row.statusCode != http.StatusTooManyRequests {
		t.Errorf("persisted status_code = %d, want 429", row.statusCode)
	}
	if row.usageKnown != 0 {
		t.Errorf("usage_known = %d, want 0 for a rejected request", row.usageKnown)
	}
}

func TestHandleMessages_SuccessReconcilesIntoLedger(t *testing.T) {
	const upstreamBody = `{"usage":{"input_tokens":100,"output_tokens":50}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	env := newTestEnvWithCaps(t, upstream.URL, 1000, 1000)

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model", "max_tokens": 100})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "cognitiveradar")
	w := httptest.NewRecorder()

	env.proxy.HandleMessages(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	committed, reserved := env.proxy.budget.Snapshot("cognitiveradar")
	wantCommitted := (100.0/1_000_000)*3.0 + (50.0/1_000_000)*15.0
	if math.Abs(committed-wantCommitted) > 1e-9 {
		t.Errorf("committed = %v, want %v", committed, wantCommitted)
	}
	if reserved != 0 {
		t.Errorf("reserved = %v, want 0 - the reservation should be fully released once reconciled", reserved)
	}
}

// TestHandleMessages_UnpricedModelStillGetsCapped guards against a real bug:
// pricing.Table.Cost returned 0 for any model missing from pricing.json,
// which meant it reserved nothing and could never be capped - a brand new
// model release or a typo in a model name silently bypassed the daily cap
// entirely. A configured default_price fallback closes that gap.
func TestHandleMessages_UnpricedModelStillGetsCapped(t *testing.T) {
	var upstreamHit bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	dir := t.TempDir()

	pricingPath := filepath.Join(dir, "pricing.json")
	// No model-specific rates at all - only the fallback. If Cost() ever
	// regresses to returning 0 for an unlisted model, this test catches it.
	const pricingJSON = `{"models": {}, "default_price": {"input_per_mtok": 1000000, "output_per_mtok": 1000000}}`
	if err := os.WriteFile(pricingPath, []byte(pricingJSON), 0o644); err != nil {
		t.Fatalf("writing pricing file: %v", err)
	}
	table, err := pricing.Load(pricingPath)
	if err != nil {
		t.Fatalf("pricing.Load() error = %v", err)
	}

	capsPath := filepath.Join(dir, "caps.json")
	const capsJSON = `{"global_daily_cap_usd": 1000, "default_project_cap_usd": 0.01, "project_caps_usd": {}}`
	if err := os.WriteFile(capsPath, []byte(capsJSON), 0o644); err != nil {
		t.Fatalf("writing caps file: %v", err)
	}
	ledger, err := budget.New(capsPath)
	if err != nil {
		t.Fatalf("budget.New() error = %v", err)
	}

	st, err := store.Open(filepath.Join(dir, "meter_test.db"))
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { st.Close() })

	p, err := NewAnthropic(upstream.URL, st, table, ledger, nil, nil)
	if err != nil {
		t.Fatalf("NewAnthropic() error = %v", err)
	}

	reqBody, _ := json.Marshal(map[string]any{"model": "some-brand-new-model", "max_tokens": 10})
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set(projectHeader, "unpriced-test")
	w := httptest.NewRecorder()

	p.HandleMessages(w, req)

	if upstreamHit {
		t.Fatal("upstream was hit for an unpriced model against a tiny cap - it should have been rejected like any other over-cap request")
	}
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 - an unpriced model must still be capped via the fallback price, not treated as free", w.Code)
	}
}

// setupBenchUpstream is a minimal fake Anthropic for the benchmarks below:
// drain the request, return a small fixed JSON response immediately.
func setupBenchUpstream() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"msg","usage":{"input_tokens":10,"output_tokens":10}}`))
	}))
}

// BenchmarkDirectRequest is the baseline: client straight to the fake
// upstream, no meter in between.
func BenchmarkDirectRequest(b *testing.B) {
	upstream := setupBenchUpstream()
	defer upstream.Close()

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Post(upstream.URL, "application/json", bytes.NewReader(reqBody))
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// BenchmarkProxyRequest is the same request through meter's accounted
// handler; the ns/op delta from BenchmarkDirectRequest is the added overhead.
func BenchmarkProxyRequest(b *testing.B) {
	upstream := setupBenchUpstream()
	defer upstream.Close()

	dir := b.TempDir()
	pricingPath := filepath.Join(dir, "pricing.json")
	const pricingJSON = `{"models":{"test-model":{"input_per_mtok":3.0,"output_per_mtok":15.0}}}`
	if err := os.WriteFile(pricingPath, []byte(pricingJSON), 0o644); err != nil {
		b.Fatalf("writing pricing file: %v", err)
	}
	table, err := pricing.Load(pricingPath)
	if err != nil {
		b.Fatalf("pricing.Load() error = %v", err)
	}
	st, err := store.Open(filepath.Join(dir, "meter.db"))
	if err != nil {
		b.Fatalf("store.Open() error = %v", err)
	}
	defer st.Close()

	capsPath := filepath.Join(dir, "caps.json")
	const capsJSON = `{"global_daily_cap_usd": 1000, "default_project_cap_usd": 1000, "project_caps_usd": {}}`
	if err := os.WriteFile(capsPath, []byte(capsJSON), 0o644); err != nil {
		b.Fatalf("writing caps file: %v", err)
	}
	ledger, err := budget.New(capsPath)
	if err != nil {
		b.Fatalf("budget.New() error = %v", err)
	}

	p, err := NewAnthropic(upstream.URL, st, table, ledger, nil, nil)
	if err != nil {
		b.Fatalf("NewAnthropic() error = %v", err)
	}

	meterServer := httptest.NewServer(http.HandlerFunc(p.HandleMessages))
	defer meterServer.Close()

	reqBody, _ := json.Marshal(map[string]any{"model": "test-model"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Post(meterServer.URL, "application/json", bytes.NewReader(reqBody))
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}
