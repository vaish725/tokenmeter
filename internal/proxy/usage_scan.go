// The SSE-aware response body wrapper: this is the piece the PRD calls out
// as the interesting constraint of the whole project - it has to forward
// every byte to the client the instant it arrives (no full-body buffering,
// or the client's token-by-token stream turns into one big pause-then-dump)
// while still surfacing the usage numbers that only become fully known once
// the tail of the response has gone by.
//
// The trick is that Read() never delays the caller: it passes through
// exactly the bytes the upstream just produced, and only *afterward*, on
// those same already-in-hand bytes, does any scanning. Nothing here holds a
// byte back waiting to decide what to do with it.
package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strconv"
	"sync"
)

// maxJSONScanBytes bounds how much of a non-streaming JSON response body
// gets accumulated for usage parsing. Real Messages API JSON responses are
// small and arrive as a single chunk regardless - this cap is a defensive
// boundary against something unexpectedly huge claiming to be JSON, not a
// real limit on the response the client receives (the client still gets
// every byte; this only bounds what we keep a second copy of for scanning).
const maxJSONScanBytes = 10 * 1024 * 1024

// maxPendingSSELineBytes bounds the "bytes since the last newline" buffer
// used while scanning an SSE stream line-by-line. An upstream that never
// sends '\n' would otherwise grow this forever; past the cap we simply give
// up on usage extraction for this response (the client's own copy of the
// bytes is completely unaffected).
const maxPendingSSELineBytes = 64 * 1024

// inputTokensPattern and outputTokensPattern find token counts inside a
// single SSE line or a JSON usage block. Anthropic's stream splits these
// across events: input_tokens appears once, in message_start; output_tokens
// appears repeatedly (cumulatively) across message_delta events, so the
// largest value seen is the final total.
var (
	inputTokensPattern  = regexp.MustCompile(`"input_tokens"\s*:\s*(\d+)`)
	outputTokensPattern = regexp.MustCompile(`"output_tokens"\s*:\s*(\d+)`)
)

// usageDoneFunc is called exactly once, when the wrapped body is closed,
// with whatever usage information was found (or usageKnown=false if none
// was, or scanning was skipped entirely for a non-2xx response).
type usageDoneFunc func(inputTokens, outputTokens int, usageKnown bool)

// usageScanningBody wraps a response body so that reading it is a pure
// passthrough to the caller, while a side channel accumulates just enough
// state to answer "how many tokens did this cost" once the body is fully
// read (or the client disconnects and reading stops early - either way,
// Close() still fires and whatever was seen so far gets logged).
type usageScanningBody struct {
	upstream io.ReadCloser
	scan     bool // false for non-2xx responses: nothing to bill, don't bother
	isSSE    bool

	pending []byte // SSE mode: bytes since the last '\n'
	jsonBuf []byte // JSON mode: bounded accumulation of the whole body

	input  int
	output int
	known  bool

	once   sync.Once
	onDone usageDoneFunc
}

// newUsageScanningBody wraps body. isSSE selects which of the two scanning
// strategies applies; scan=false skips scanning entirely (still passes
// bytes through) for responses where there's nothing meaningful to extract.
func newUsageScanningBody(body io.ReadCloser, isSSE, scan bool, onDone usageDoneFunc) *usageScanningBody {
	return &usageScanningBody{
		upstream: body,
		scan:     scan,
		isSSE:    isSSE,
		onDone:   onDone,
	}
}

// Read is a pure passthrough: whatever bytes the upstream produced go
// straight into the caller's buffer, immediately. Scanning happens as a
// side effect on the same bytes, after they're already on their way to the
// client - it can never add latency to delivery.
func (b *usageScanningBody) Read(p []byte) (int, error) {
	n, err := b.upstream.Read(p)
	if n > 0 && b.scan {
		b.feed(p[:n])
	}
	return n, err
}

// Close closes the real body and, exactly once, reports whatever usage was
// found. sync.Once guards against double-reporting if something ever calls
// Close more than once (httputil.ReverseProxy only calls it once itself,
// but a wrong double cost/count would be a real correctness bug, so this
// costs nothing to guarantee).
func (b *usageScanningBody) Close() error {
	err := b.upstream.Close()
	b.once.Do(func() {
		if b.scan && !b.isSSE {
			b.finalizeJSON()
		}
		b.onDone(b.input, b.output, b.known)
	})
	return err
}

func (b *usageScanningBody) feed(chunk []byte) {
	if b.isSSE {
		b.feedSSE(chunk)
		return
	}
	if len(b.jsonBuf) >= maxJSONScanBytes {
		return
	}
	room := maxJSONScanBytes - len(b.jsonBuf)
	if len(chunk) > room {
		chunk = chunk[:room]
	}
	b.jsonBuf = append(b.jsonBuf, chunk...)
}

// feedSSE splits the accumulated bytes into complete lines and scans each
// one as it completes, keeping only the still-incomplete tail around for
// next time - the bounded-memory alternative to buffering the whole stream.
func (b *usageScanningBody) feedSSE(chunk []byte) {
	b.pending = append(b.pending, chunk...)
	for {
		i := bytes.IndexByte(b.pending, '\n')
		if i < 0 {
			break
		}
		line := b.pending[:i]
		b.scanLine(line)
		b.pending = b.pending[i+1:]
	}
	if len(b.pending) > maxPendingSSELineBytes {
		// Defensive only: this never affects what the client receives,
		// just gives up on finding usage in this response.
		b.pending = nil
	}
}

func (b *usageScanningBody) scanLine(line []byte) {
	if m := inputTokensPattern.FindSubmatch(line); m != nil {
		if n, err := strconv.Atoi(string(m[1])); err == nil {
			b.input = n
			b.known = true
		}
	}
	if m := outputTokensPattern.FindSubmatch(line); m != nil {
		if n, err := strconv.Atoi(string(m[1])); err == nil {
			if n > b.output {
				b.output = n
			}
			b.known = true
		}
	}
}

// finalizeJSON runs once, on Close, for non-streaming responses: decode the
// bounded buffer accumulated by feed() and pull the usage block out of it.
// A parse failure (truncated capture, or a response that wasn't really
// JSON despite its Content-Type) leaves known=false rather than panicking
// or failing the request - the bytes already reached the client untouched
// regardless of whether this succeeds.
func (b *usageScanningBody) finalizeJSON() {
	var parsed struct {
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(b.jsonBuf, &parsed); err != nil {
		return
	}
	b.input = parsed.Usage.InputTokens
	b.output = parsed.Usage.OutputTokens
	b.known = true
}
