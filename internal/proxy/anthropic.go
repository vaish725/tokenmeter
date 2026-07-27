// Package proxy implements meter's HTTP handlers for the Anthropic Messages
// API.
//
// The forwarding mechanism is httputil.ReverseProxy with a custom
// ModifyResponse and ErrorHandler, per the PRD's own Go-engineering section:
// it flushes to the client after every write (FlushInterval: -1) and
// already strips hop-by-hop headers on both legs, which is what makes real
// token-by-token streaming possible instead of the buffer-then-forward
// approach this package started with. What still needs custom code is
// everything ReverseProxy doesn't know about: which project a request
// belongs to, and what it cost - see usage_scan.go for how usage is pulled
// out of a response without ever buffering the whole thing.
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

	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// maxRequestBodyBytes caps how much of a client REQUEST we'll buffer into
// memory in order to inspect it (model/stream/project). This is a
// defensive boundary check (the proxy is network-facing), not a business
// rule - 10MB comfortably covers real Messages API payloads. Buffering the
// request is not the thing week 2 set out to fix: prompts are small and
// bounded, unlike a streamed response's token-by-token output.
const maxRequestBodyBytes = 10 * 1024 * 1024

// projectHeader is the explicit override in R4's attribution priority chain.
const projectHeader = "X-Meter-Project"

// unattributedProject is what gets logged when no attribution signal exists.
const unattributedProject = "unattributed"

// AnthropicProxy holds everything the Messages API handler needs: where to
// forward requests, how to price them, and where to log them.
type AnthropicProxy struct {
	store   *store.Store
	pricing *pricing.Table

	reverseProxy *httputil.ReverseProxy
}

// requestStateKey is the context key used to pass per-request accounting
// state (start time, project, model, stream) from HandleMessages through to
// ModifyResponse/ErrorHandler. A private key type avoids collisions with
// context values set by anything else.
type requestStateKey struct{}

// requestState is everything ModifyResponse/ErrorHandler need to know about
// a request in order to log it, gathered once up front before the request
// is forwarded.
type requestState struct {
	start   time.Time
	project string
	model   string
	stream  bool
}

// New builds an AnthropicProxy targeting upstreamURL. No fixed request
// timeout is configured anywhere in this chain - LLM calls can legitimately
// run for minutes, so the only thing that should ever cut a request short
// is the client's own context cancellation (propagated automatically by
// ReverseProxy via the request it forwards), not an arbitrary deadline here.
func New(upstreamURL string, st *store.Store, pt *pricing.Table) (*AnthropicProxy, error) {
	target, err := url.Parse(strings.TrimSuffix(upstreamURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("proxy: parsing upstream URL %q: %w", upstreamURL, err)
	}

	p := &AnthropicProxy{store: st, pricing: pt}
	p.reverseProxy = &httputil.ReverseProxy{
		Director:       p.director(target),
		ModifyResponse: p.modifyResponse,
		ErrorHandler:   p.handleProxyError,
		// Flush after every write instead of on a timer: the whole point of
		// this rewrite is that a client waiting on token-by-token output
		// sees each token as it arrives, not batched up.
		FlushInterval: -1,
	}
	return p, nil
}

// requestMeta is the only part of the request body meter actually needs to
// look at. Decoding into this narrow struct (rather than a full schema)
// means unrelated fields the client sends are ignored, not stripped - the
// original bytes are what get forwarded upstream, untouched.
type requestMeta struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
}

// HandleMessages is the accounted handler for POST /v1/messages. It buffers
// just the request body (small and bounded - see maxRequestBodyBytes) to
// resolve a project and read the model/stream fields, stashes that as
// requestState on the request's context, and hands off to the shared
// reverse proxy. ModifyResponse and ErrorHandler do the actual logging once
// they know how the request turned out.
func (p *AnthropicProxy) HandleMessages(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	project := resolveProject(r)

	body, err := readLimitedBody(w, r, maxRequestBodyBytes)
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusBadRequest)
		return
	}

	// Best-effort: if the body isn't valid JSON, meta stays zero-valued and
	// the request still gets forwarded byte-for-byte below. A malformed
	// request is the upstream's problem to reject, not meter's to block.
	var meta requestMeta
	_ = json.Unmarshal(body, &meta)

	// Re-wrap the body we just drained so the reverse proxy can still send
	// it upstream unmodified.
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	state := &requestState{start: start, project: project, model: meta.Model, stream: meta.Stream}
	r = r.WithContext(context.WithValue(r.Context(), requestStateKey{}, state))

	p.reverseProxy.ServeHTTP(w, r)
}

// HandlePassthrough forwards any non-Messages-API path (e.g. GET /v1/models)
// straight through with no attribution or persistence. Nothing the client
// calls should ever be blocked just because meter doesn't account for it
// yet. No requestState is stashed, so ModifyResponse knows (via a failed
// type assertion) there's nothing to log for these.
func (p *AnthropicProxy) HandlePassthrough(w http.ResponseWriter, r *http.Request) {
	p.reverseProxy.ServeHTTP(w, r)
}

// director rewrites the outbound request to point at the real provider.
// Path and query are left as ReverseProxy already cloned them from the
// inbound request - only scheme/host need to change.
func (p *AnthropicProxy) director(target *url.URL) func(*http.Request) {
	return func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
		// Meter-internal; Anthropic has no use for it and shouldn't see it.
		req.Header.Del(projectHeader)
	}
}

// modifyResponse installs the usage-scanning wrapper on accounted
// responses. It runs after headers arrive from upstream but before any body
// bytes have been copied to the client, so wrapping resp.Body here still
// gets every byte of the body through the scanner.
func (p *AnthropicProxy) modifyResponse(resp *http.Response) error {
	state, ok := resp.Request.Context().Value(requestStateKey{}).(*requestState)
	if !ok {
		// A passthrough route - nothing to attribute or price.
		return nil
	}

	isSSE := strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")
	scan := resp.StatusCode >= 200 && resp.StatusCode < 300

	resp.Body = newUsageScanningBody(resp.Body, isSSE, scan, func(inputTokens, outputTokens int, usageKnown bool) {
		latency := time.Since(state.start)
		// Insert happens once the body has been fully read (or the client
		// disconnected and reading stopped early) - accounting must never
		// add latency to the call the caller is waiting on. context.
		// WithoutCancel: the client's own disconnect must not also cancel
		// the DB write for the row describing that same disconnect.
		p.persist(context.WithoutCancel(resp.Request.Context()), state.project, state.model, inputTokens, outputTokens, latency, resp.StatusCode, state.stream, usageKnown)
	})
	return nil
}

// handleProxyError covers failures before any response was ever received -
// upstream connection refused, DNS failure, or the client's own context
// canceled while still waiting on a response. (A disconnect *after*
// headers were already relayed to the client is instead handled by
// usageScanningBody.Close, since ReverseProxy can no longer call this once
// a status code has been written.)
func (p *AnthropicProxy) handleProxyError(w http.ResponseWriter, r *http.Request, err error) {
	if state, ok := r.Context().Value(requestStateKey{}).(*requestState); ok {
		latency := time.Since(state.start)
		p.persist(context.WithoutCancel(r.Context()), state.project, state.model, 0, 0, latency, 0, state.stream, false)
	}

	if r.Context().Err() != nil {
		// Client already gone; nothing left to write a response to.
		return
	}
	http.Error(w, fmt.Sprintf("upstream request failed: %v", err), http.StatusBadGateway)
}

// persist writes a request record, logging (not failing the caller) if the
// database write itself fails - a broken meter must never block a working
// workflow.
func (p *AnthropicProxy) persist(ctx context.Context, project, model string, inputTokens, outputTokens int, latency time.Duration, statusCode int, stream, usageKnown bool) {
	cost, _ := p.pricing.Cost(model, inputTokens, outputTokens)

	rec := store.Record{
		Timestamp:    time.Now(),
		Project:      project,
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

// resolveProject implements the first link of R4's attribution priority
// chain: an explicit header. API-key-to-project mapping and client
// working-directory inference (the next two links) need config/infra that
// doesn't exist yet, and cwd inference is still an open question in the PRD
// itself (section 10) - both remain deferred.
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
