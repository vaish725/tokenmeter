// Package proxy implements meter's HTTP handlers for the Anthropic Messages
// API.
//
// Week 1 scope, deliberately: every request/response body is fully buffered
// in memory (no httputil.ReverseProxy, no streaming passthrough yet). That
// matches the PRD's own milestone table, which puts real SSE-aware, non-
// buffering passthrough in week 2. Buffering here is what lets this
// handler inspect the model name, resolve a project, and read the usage
// block back out of the response before persisting it - all things that
// fight httputil.ReverseProxy's streaming-first design. The plan is to
// replace this with httputil.ReverseProxy + a custom ModifyResponse once
// streaming lands, per the PRD's Go-engineering section.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// maxRequestBodyBytes caps how much of a client request we'll buffer into
// memory. This is a defensive boundary check (the proxy is network-facing),
// not a business rule - 10MB comfortably covers real Messages API payloads.
const maxRequestBodyBytes = 10 * 1024 * 1024

// projectHeader is the explicit override in R4's attribution priority chain.
const projectHeader = "X-Meter-Project"

// unattributedProject is what gets logged when no attribution signal exists.
const unattributedProject = "unattributed"

// hopByHopHeaders must not be copied between the inbound and outbound
// requests/responses (RFC 7230 6.1). Building a fresh outbound request from
// a fully-buffered body already sidesteps most of the danger here (there is
// no real "Transfer-Encoding: chunked" to preserve), but copying these
// verbatim can still confuse either side about connection lifecycle.
var hopByHopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailers", "Transfer-Encoding", "Upgrade",
}

// AnthropicProxy holds everything the Messages API handler needs: where to
// forward requests, how to price them, and where to log them.
type AnthropicProxy struct {
	upstreamURL string
	client      *http.Client
	store       *store.Store
	pricing     *pricing.Table
}

// New builds an AnthropicProxy. No fixed request timeout is set on the HTTP
// client - LLM calls can legitimately run for minutes, so the only thing
// that should ever cut a request short is the client's own context
// cancellation (see HandleMessages), not an arbitrary deadline here.
func New(upstreamURL string, st *store.Store, pt *pricing.Table) *AnthropicProxy {
	return &AnthropicProxy{
		upstreamURL: strings.TrimSuffix(upstreamURL, "/"),
		client:      &http.Client{},
		store:       st,
		pricing:     pt,
	}
}

// requestMeta is the only part of the request body meter actually needs to
// look at. Decoding into this narrow struct (rather than a full schema)
// means unrelated fields the client sends are ignored, not stripped - the
// original bytes are what get forwarded upstream, untouched.
type requestMeta struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
}

// HandleMessages is the accounted handler for POST /v1/messages: it
// attributes the request to a project, forwards it upstream unmodified,
// parses token usage out of the response, prices it, and persists a record -
// all without blocking or altering what the client actually receives.
func (p *AnthropicProxy) HandleMessages(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
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

	resp, err := p.forward(ctx, r, body)
	if err != nil {
		latency := time.Since(start)
		p.persist(context.WithoutCancel(ctx), project, meta.Model, 0, 0, latency, 0, meta.Stream, false)

		if ctx.Err() != nil {
			// Client disconnected; nothing left to write a response to.
			return
		}
		http.Error(w, fmt.Sprintf("upstream request failed: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		latency := time.Since(start)
		p.persist(context.WithoutCancel(ctx), project, meta.Model, 0, 0, latency, resp.StatusCode, meta.Stream, false)
		http.Error(w, "reading upstream response failed", http.StatusBadGateway)
		return
	}

	copyHeaders(w.Header(), resp.Header)
	w.Header().Set("Content-Length", strconv.Itoa(len(respBody)))
	w.WriteHeader(resp.StatusCode)
	w.Write(respBody)

	latency := time.Since(start)

	inputTokens, outputTokens, usageKnown := 0, 0, false
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		inputTokens, outputTokens, usageKnown = extractUsage(resp.Header.Get("Content-Type"), respBody)
	}

	// Insert happens after the response is already flushed to the client -
	// accounting must never add latency to the call the user is waiting on.
	p.persist(context.WithoutCancel(ctx), project, meta.Model, inputTokens, outputTokens, latency, resp.StatusCode, meta.Stream, usageKnown)
}

// HandlePassthrough forwards any non-Messages-API path (e.g. GET /v1/models)
// straight through with no attribution or persistence. Nothing the client
// calls should ever be blocked just because meter doesn't account for it yet.
func (p *AnthropicProxy) HandlePassthrough(w http.ResponseWriter, r *http.Request) {
	body, err := readLimitedBody(w, r, maxRequestBodyBytes)
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusBadRequest)
		return
	}

	resp, err := p.forward(r.Context(), r, body)
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		http.Error(w, fmt.Sprintf("upstream request failed: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// forward builds and sends the outbound request to the real provider,
// carrying the caller's context so a client disconnect cancels the upstream
// call rather than paying for tokens nobody will read.
func (p *AnthropicProxy) forward(ctx context.Context, r *http.Request, body []byte) (*http.Response, error) {
	url := p.upstreamURL + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}

	outReq, err := http.NewRequestWithContext(ctx, r.Method, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("building upstream request: %w", err)
	}
	copyHeaders(outReq.Header, r.Header)

	return p.client.Do(outReq)
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
// itself (section 10) - both are deferred past week 1.
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

// copyHeaders copies all headers from src to dst except hop-by-hop ones and
// Host (Host is derived from the outbound URL, not copied).
func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		if strings.EqualFold(key, "Host") || isHopByHop(key) {
			continue
		}
		for _, v := range values {
			dst.Add(key, v)
		}
	}
}

func isHopByHop(header string) bool {
	for _, h := range hopByHopHeaders {
		if strings.EqualFold(header, h) {
			return true
		}
	}
	return false
}

// inputTokensPattern and outputTokensPattern back the SSE fallback path in
// extractUsage: a real SSE parser (with a fuzz target) is a week 2
// deliverable, so week 1 settles for scanning the fully-buffered stream body
// for every usage number it can find.
var (
	inputTokensPattern  = regexp.MustCompile(`"input_tokens"\s*:\s*(\d+)`)
	outputTokensPattern = regexp.MustCompile(`"output_tokens"\s*:\s*(\d+)`)
)

// extractUsage reads token counts out of a response body. For a plain JSON
// response (the non-streaming case) it decodes the usage block directly.
// For an SSE response (a client asked for stream:true; the body is still
// fully buffered in week 1) it falls back to scanning for the last
// occurrence of each token field, since Anthropic's stream splits input
// tokens (message_start) and the final output token count (the last
// message_delta) across separate events. If neither approach finds
// anything, known is false and the caller records the row as
// usage-unparseable rather than failing the request - matching the
// fallback the PRD's own risk section prescribes.
func extractUsage(contentType string, body []byte) (inputTokens, outputTokens int, known bool) {
	if strings.Contains(contentType, "application/json") {
		var parsed struct {
			Usage struct {
				InputTokens  int `json:"input_tokens"`
				OutputTokens int `json:"output_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			return 0, 0, false
		}
		return parsed.Usage.InputTokens, parsed.Usage.OutputTokens, true
	}

	inMatches := inputTokensPattern.FindAllSubmatch(body, -1)
	outMatches := outputTokensPattern.FindAllSubmatch(body, -1)
	if len(inMatches) == 0 && len(outMatches) == 0 {
		return 0, 0, false
	}

	// Output tokens accumulate across delta events, so the largest value
	// seen is the final total. Input tokens are constant across the stream,
	// so the largest value seen is also correct and tolerates any one
	// malformed match.
	return maxCapturedInt(inMatches), maxCapturedInt(outMatches), true
}

func maxCapturedInt(matches [][][]byte) int {
	max := 0
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		n, err := strconv.Atoi(string(m[1]))
		if err != nil {
			continue
		}
		if n > max {
			max = n
		}
	}
	return max
}
