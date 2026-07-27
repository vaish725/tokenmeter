package proxy

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// testEnv wires up a real Store and a real pricing.Table against temp files,
// so these tests exercise the same code paths meter runs in production
// rather than mocks standing in for them.
type testEnv struct {
	proxy  *AnthropicProxy
	st     *store.Store
	dbPath string
}

func newTestEnv(t *testing.T, upstreamURL string) *testEnv {
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

	dbPath := filepath.Join(dir, "meter_test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { st.Close() })

	p, err := New(upstreamURL, st, table)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return &testEnv{
		proxy:  p,
		st:     st,
		dbPath: dbPath,
	}
}

// requestRow is what the tests read back to assert a persisted record - a
// direct SQL query against the same file, since Store deliberately doesn't
// expose a read API yet (that belongs to `meter top` in week 3).
type requestRow struct {
	project      string
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
		SELECT project, model, input_tokens, output_tokens, cost_usd, status_code, usage_known
		FROM requests ORDER BY id DESC LIMIT 1
	`).Scan(&row.project, &row.model, &row.inputTokens, &row.outputTokens, &row.costUSD, &row.statusCode, &row.usageKnown)
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
	if row.inputTokens != 100 || row.outputTokens != 50 {
		t.Errorf("tokens = %d/%d, want 100/50", row.inputTokens, row.outputTokens)
	}
	// Compare with a tolerance rather than "==": the compiler evaluates a
	// constant expression like this one at arbitrary precision and rounds
	// once at the end, while the production code path divides/multiplies
	// float64s at runtime with a rounding step after each operation - the
	// two can differ in the last bit even though both are "correct".
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
		// A real handler always reads the request body before responding.
		// That drain matters here: Go's http.Server can only detect the
		// client's connection closing (and cancel r.Context()) once it
		// knows no unread body bytes remain - otherwise it can't tell a
		// dead connection apart from a client that's just slow to finish
		// sending. Skipping this line makes the cancellation below
		// (correctly) never observed by this handler, which looks like a
		// hang but is actually a net/http server guarantee, not a bug.
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

	// A real server this time, not a ResponseRecorder: proving events reach
	// the client before the upstream is done needs a real connection and a
	// real client reading incrementally, not just an end-state comparison.
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

	// The actual proof this isn't week 1's buffer-everything approach: a
	// fully-buffered implementation would only hand back any bytes at all
	// once the upstream had already finished, so firstByteAt would be close
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
// handler. The delta between this and BenchmarkDirectRequest's ns/op is
// the actual added overhead per request - the week 2 "latency benchmark
// vs. direct calls" deliverable.
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
	p, err := New(upstream.URL, st, table)
	if err != nil {
		b.Fatalf("New() error = %v", err)
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
