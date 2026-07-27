// Package proxy implements meter's HTTP handlers: forwards via
// httputil.ReverseProxy (real streaming, hop-by-hop headers handled for
// free), while attributing, capping, and pricing what ReverseProxy itself
// doesn't know about. See usage_scan.go for how usage is pulled from a
// response without buffering it.
//
// Proxy is provider-agnostic; anthropic.go and openai.go each supply a
// spec (upstream default, path, usage field names, and OpenAI's one body-
// rewrite hook) rather than duplicating this logic per provider.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/downshift"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// maxRequestBodyBytes bounds how much of a request we buffer to inspect it
// (model/stream/project) - a defensive cap, not a business rule. Buffering
// the request is fine; prompts are small, unlike a streamed response.
const maxRequestBodyBytes = 10 * 1024 * 1024

// projectHeader is the explicit override in R4's attribution priority chain.
const projectHeader = "X-Meter-Project"

// unattributedProject is what gets logged when no attribution signal exists.
const unattributedProject = "unattributed"

// spec is what makes a Proxy provider-specific: everything Anthropic and
// OpenAI differ on, isolated to data instead of duplicated code.
type spec struct {
	name        string      // "anthropic" | "openai" - stored in the provider column
	path        string      // registered route, e.g. "/v1/messages"
	usageFields usageFields // JSON field names for this provider's usage block
	// rewriteBody optionally rewrites the request body before it's
	// forwarded (nil => identity). OpenAI uses this to inject
	// stream_options.include_usage; Anthropic has no need for it.
	rewriteBody func(body []byte, meta requestMeta) []byte
}

// Proxy is the generic accounted-and-streaming reverse proxy for one
// provider, built from a spec.
type Proxy struct {
	spec      spec
	store     *store.Store
	pricing   *pricing.Table
	budget    *budget.Ledger
	downshift *downshift.Table // nil => policy disabled, always 429 at cap

	reverseProxy *httputil.ReverseProxy
}

// requestStateKey is the context key carrying requestState from
// HandleMessages to ModifyResponse/ErrorHandler.
type requestStateKey struct{}

// requestState is what ModifyResponse/ErrorHandler need to log and
// reconcile a request, gathered once before it's forwarded.
type requestState struct {
	start           time.Time
	project         string
	model           string
	stream          bool
	reservationID   string
	downshiftedFrom string // "" unless this request was rewritten to a cheaper model
}

// newProxy builds a Proxy targeting upstreamURL for the given spec. No
// fixed request timeout is set - LLM calls can run for minutes; only
// client disconnect should cut one short. dt may be nil (downshift disabled).
func newProxy(sp spec, upstreamURL string, st *store.Store, pt *pricing.Table, bl *budget.Ledger, dt *downshift.Table) (*Proxy, error) {
	target, err := url.Parse(strings.TrimSuffix(upstreamURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("proxy: parsing upstream URL %q: %w", upstreamURL, err)
	}
	if sp.rewriteBody == nil {
		sp.rewriteBody = func(body []byte, _ requestMeta) []byte { return body }
	}

	p := &Proxy{spec: sp, store: st, pricing: pt, budget: bl, downshift: dt}
	p.reverseProxy = &httputil.ReverseProxy{
		Director:       p.director(target),
		ModifyResponse: p.modifyResponse,
		ErrorHandler:   p.handleProxyError,
		// -1: flush after every write, so streamed tokens reach the client
		// as they arrive instead of getting batched up.
		FlushInterval: -1,
	}
	return p, nil
}

// Path is the route this provider's accounted handler should be
// registered at, e.g. "/v1/messages".
func (p *Proxy) Path() string {
	return p.spec.path
}

// requestMeta is the only part of the request body meter looks at; unknown
// fields are ignored here but still forwarded untouched in the raw bytes.
// Model/Stream/MaxTokens are the same field names Anthropic and OpenAI both
// use; MaxCompletionTokens is OpenAI's reasoning-model variant of MaxTokens.
type requestMeta struct {
	Model               string `json:"model"`
	Stream              bool   `json:"stream"`
	MaxTokens           int    `json:"max_tokens"`
	MaxCompletionTokens int    `json:"max_completion_tokens"`
}

// defaultMaxTokensEstimate is the output-token ceiling used when a request
// omits max_tokens/max_completion_tokens (required by providers, but don't
// trust that).
const defaultMaxTokensEstimate = 4096

// estimateCost is R3's pre-flight estimate: input tokens ~= body length / 4
// (cheap heuristic - reconciliation against real usage corrects it later)
// plus max_tokens (or the fallback) as the pessimistic output bound.
func estimateCost(pt *pricing.Table, body []byte, meta requestMeta) float64 {
	estimatedInputTokens := len(body) / 4
	estimatedOutputTokens := meta.MaxTokens
	if meta.MaxCompletionTokens > estimatedOutputTokens {
		estimatedOutputTokens = meta.MaxCompletionTokens
	}
	if estimatedOutputTokens <= 0 {
		estimatedOutputTokens = defaultMaxTokensEstimate
	}
	cost, _ := pt.Cost(meta.Model, estimatedInputTokens, estimatedOutputTokens)
	return cost
}

// HandleMessages is the accounted handler for this provider's chat
// endpoint: resolve project + model/stream, reserve budget, stash
// requestState, forward. ModifyResponse/ErrorHandler do the actual logging
// once it's known how the request turned out.
func (p *Proxy) HandleMessages(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	project := resolveProject(r)

	body, err := readLimitedBody(w, r, maxRequestBodyBytes)
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusBadRequest)
		return
	}

	// Best-effort decode; a malformed body is still forwarded as-is and is
	// the upstream's problem to reject, not meter's to block.
	var meta requestMeta
	_ = json.Unmarshal(body, &meta)

	// Reserve before forwarding - what makes the cap real under concurrency.
	estimated := estimateCost(p.pricing, body, meta)
	reservationID, ok, decision := p.budget.Reserve(project, estimated)

	downshiftedFrom := ""
	if !ok {
		// Opt-in policy (nil Table => always the 429 below): one substitute
		// attempt, no cascading through chained substitutes.
		if substitute, hasSubstitute := p.downshift.Substitute(meta.Model); hasSubstitute {
			substituteMeta := meta
			substituteMeta.Model = substitute
			substituteEstimate := estimateCost(p.pricing, body, substituteMeta)
			if subID, subOK, _ := p.budget.Reserve(project, substituteEstimate); subOK {
				reservationID, ok = subID, true
				downshiftedFrom = meta.Model
				body = rewriteModelField(body, substitute)
				meta.Model = substitute
			}
		}
	}
	if !ok {
		latency := time.Since(start)
		p.persist(r.Context(), project, meta.Model, 0, 0, latency, http.StatusTooManyRequests, meta.Stream, false, "")
		writeBudgetExceeded(w, project, decision)
		return
	}

	// Re-wrap the (possibly provider- or downshift-rewritten) body so it
	// can be forwarded.
	body = p.spec.rewriteBody(body, meta)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	state := &requestState{start: start, project: project, model: meta.Model, stream: meta.Stream, reservationID: reservationID, downshiftedFrom: downshiftedFrom}
	r = r.WithContext(context.WithValue(r.Context(), requestStateKey{}, state))

	p.reverseProxy.ServeHTTP(w, r)
}

// rewriteModelField swaps the request body's top-level "model" field, used
// to actually route a downshifted request to its cheaper substitute. A
// decode failure returns the body unchanged - the upstream rejects
// malformed JSON on its own, meter doesn't need to.
func rewriteModelField(body []byte, model string) []byte {
	var generic map[string]any
	if err := json.Unmarshal(body, &generic); err != nil {
		return body
	}
	generic["model"] = model
	rewritten, err := json.Marshal(generic)
	if err != nil {
		return body
	}
	return rewritten
}

// writeBudgetExceeded writes R5's structured 429: which cap was hit, its
// limit, what it would have used, and when it resets.
func writeBudgetExceeded(w http.ResponseWriter, project string, decision budget.Decision) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	json.NewEncoder(w).Encode(struct {
		Error    string  `json:"error"`
		Cap      string  `json:"cap"`
		Project  string  `json:"project"`
		LimitUSD float64 `json:"limit_usd"`
		UsedUSD  float64 `json:"used_usd"`
		ResetsAt string  `json:"resets_at"`
	}{
		Error:    "budget_exceeded",
		Cap:      decision.Cap,
		Project:  project,
		LimitUSD: decision.LimitUSD,
		UsedUSD:  decision.UsedUSD,
		ResetsAt: decision.ResetAt.Format(time.RFC3339),
	})
}

// HandlePassthrough forwards any other path (e.g. GET /v1/models) with no
// attribution or persistence - nothing gets blocked just because meter
// doesn't account for it yet.
func (p *Proxy) HandlePassthrough(w http.ResponseWriter, r *http.Request) {
	p.reverseProxy.ServeHTTP(w, r)
}

// director rewrites the outbound request to point at the real provider.
// Path and query are left as ReverseProxy already cloned them from the
// inbound request - only scheme/host need to change.
func (p *Proxy) director(target *url.URL) func(*http.Request) {
	return func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		// Meter-internal; the provider has no use for it and shouldn't see it.
		req.Header.Del(projectHeader)
		// A client's own Accept-Encoding (nearly universal in real HTTP
		// clients) makes Go's Transport skip its usual transparent gzip
		// decompression, so the response would arrive still gzip-encoded -
		// readable fine by the client's own HTTP library, but unreadable
		// as JSON/SSE text by usage_scan.go, silently zeroing out cost
		// tracking. Forcing identity here means the provider never
		// compresses in the first place; an uncompressed response is
		// something every HTTP client handles regardless of what it asked for.
		req.Header.Set("Accept-Encoding", "identity")
	}
}

// modifyResponse installs the usage-scanning wrapper before any body bytes
// reach the client, so it sees every byte without buffering them.
func (p *Proxy) modifyResponse(resp *http.Response) error {
	state, ok := resp.Request.Context().Value(requestStateKey{}).(*requestState)
	if !ok {
		return nil // a passthrough route - nothing to attribute or price
	}

	isSSE := strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")
	scan := resp.StatusCode >= 200 && resp.StatusCode < 300

	// Annotate via headers, not the provider's own JSON body, so no
	// caller's SDK has to specially parse a substitution out of the response.
	if state.downshiftedFrom != "" {
		resp.Header.Set("X-Meter-Downshifted-From", state.downshiftedFrom)
		resp.Header.Set("X-Meter-Downshifted-To", state.model)
	}

	resp.Body = newUsageScanningBody(resp.Body, isSSE, scan, p.spec.usageFields, func(inputTokens, outputTokens int, usageKnown bool) {
		latency := time.Since(state.start)
		// WithoutCancel: the client's own disconnect must not also cancel
		// the DB write logging that same disconnect.
		p.persist(context.WithoutCancel(resp.Request.Context()), state.project, state.model, inputTokens, outputTokens, latency, resp.StatusCode, state.stream, usageKnown, state.reservationID)
	})
	return nil
}

// handleProxyError covers failures before any response arrived (connection
// refused, or client context canceled mid-wait). A disconnect after headers
// were already relayed is instead handled by usageScanningBody.Close.
func (p *Proxy) handleProxyError(w http.ResponseWriter, r *http.Request, err error) {
	if state, ok := r.Context().Value(requestStateKey{}).(*requestState); ok {
		latency := time.Since(state.start)
		p.persist(context.WithoutCancel(r.Context()), state.project, state.model, 0, 0, latency, 0, state.stream, false, state.reservationID)
	}

	if r.Context().Err() != nil {
		// Client already gone; nothing left to write a response to.
		return
	}
	http.Error(w, fmt.Sprintf("upstream request failed: %v", err), http.StatusBadGateway)
}

// persist writes a request record; a DB failure is logged, not returned -
// a broken meter must never block a working call. reservationID "" means
// nothing was reserved (a rejected request); otherwise it's reconciled here.
func (p *Proxy) persist(ctx context.Context, project, model string, inputTokens, outputTokens int, latency time.Duration, statusCode int, stream, usageKnown bool, reservationID string) {
	cost, _ := p.pricing.Cost(model, inputTokens, outputTokens)

	if reservationID != "" {
		p.budget.Reconcile(reservationID, cost)
	}

	rec := store.Record{
		Timestamp:    time.Now(),
		Project:      project,
		Provider:     p.spec.name,
		Model:        model,
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		CostUSD:      cost,
		LatencyMS:    latency.Milliseconds(),
		StatusCode:   statusCode,
		Stream:       stream,
		UsageKnown:   usageKnown,
	}

	if err := p.store.Insert(ctx, rec); err != nil {
		log.Printf("meter: failed to persist request record: %v", err)
	}
}

// resolveProject is R4's first attribution link: an explicit header.
// API-key mapping and cwd inference (the rest of the chain) remain deferred.
func resolveProject(r *http.Request) string {
	if p := r.Header.Get(projectHeader); p != "" {
		return p
	}
	return unattributedProject
}

// readLimitedBody reads the request body up to maxBytes, using
// http.MaxBytesReader so an oversized body produces a clean error (and a
// 413-friendly response via w) instead of unbounded memory growth.
func readLimitedBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}
