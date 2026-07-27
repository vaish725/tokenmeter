// The SSE-aware response body wrapper: forwards every byte to the client
// immediately (no full-body buffering) while still surfacing usage numbers
// that only become known once the tail of the response has gone by.
package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strconv"
	"sync"
)

// maxJSONScanBytes bounds the second copy kept for usage parsing on a
// non-streaming response - a defensive cap, not a limit on what the client
// receives (real JSON responses are small anyway).
const maxJSONScanBytes = 10 * 1024 * 1024

// maxPendingSSELineBytes bounds the "bytes since the last newline" buffer
// while scanning SSE line-by-line, in case an upstream never sends one.
const maxPendingSSELineBytes = 64 * 1024

// inputTokensPattern and outputTokensPattern find token counts in an SSE
// line or JSON usage block. input_tokens appears once (message_start);
// output_tokens is cumulative across message_delta events, so the max seen
// is the final total.
var (
	inputTokensPattern  = regexp.MustCompile(`"input_tokens"\s*:\s*(\d+)`)
	outputTokensPattern = regexp.MustCompile(`"output_tokens"\s*:\s*(\d+)`)
)

// usageDoneFunc fires exactly once, on Close, with whatever usage was found
// (usageKnown=false if none, or scanning was skipped for a non-2xx response).
type usageDoneFunc func(inputTokens, outputTokens int, usageKnown bool)

// usageScanningBody wraps a response body: Read is a pure passthrough,
// while a side channel accumulates enough state to answer "what did this
// cost" once Close fires - even if the client disconnected early.
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

// newUsageScanningBody wraps body. isSSE picks the scanning strategy;
// scan=false still passes bytes through but skips extraction entirely.
func newUsageScanningBody(body io.ReadCloser, isSSE, scan bool, onDone usageDoneFunc) *usageScanningBody {
	return &usageScanningBody{
		upstream: body,
		scan:     scan,
		isSSE:    isSSE,
		onDone:   onDone,
	}
}

// Read passes upstream bytes straight into the caller's buffer; scanning
// happens as a side effect afterward, so it never adds delivery latency.
func (b *usageScanningBody) Read(p []byte) (int, error) {
	n, err := b.upstream.Read(p)
	if n > 0 && b.scan {
		b.feed(p[:n])
	}
	return n, err
}

// Close closes the real body and, exactly once (sync.Once guards against a
// double-call double-counting cost), reports whatever usage was found.
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

// feedSSE scans each complete line as it forms, keeping only the
// incomplete tail for next time - bounded memory instead of buffering it all.
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

// finalizeJSON decodes the bounded buffer feed() accumulated. A parse
// failure just leaves known=false - the client already got the real bytes.
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
