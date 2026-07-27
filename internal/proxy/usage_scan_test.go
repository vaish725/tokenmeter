package proxy

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// fakeReadCloser lets tests wrap arbitrary bytes as an io.ReadCloser and
// count how many times Close was actually called on the underlying body.
type fakeReadCloser struct {
	r          *bytes.Reader
	closeCount int
}

func newFakeReadCloser(data []byte) *fakeReadCloser {
	return &fakeReadCloser{r: bytes.NewReader(data)}
}

func (f *fakeReadCloser) Read(p []byte) (int, error) { return f.r.Read(p) }
func (f *fakeReadCloser) Close() error               { f.closeCount++; return nil }

// readInChunks reads with a small, odd-sized buffer so lines and JSON
// tokens split across arbitrary byte boundaries, exercising the scanner
// the way io.ReadAll (one big read) wouldn't.
func readInChunks(t *testing.T, r io.Reader, chunkSize int) []byte {
	t.Helper()
	var out []byte
	buf := make([]byte, chunkSize)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("unexpected Read error: %v", err)
		}
	}
}

func TestUsageScanningBody_SSE_ExtractsAcrossEventsAndPassesThroughUnchanged(t *testing.T) {
	const full = `data: {"type":"message_start","message":{"usage":{"input_tokens":123,"output_tokens":1}}}` + "\n\n" +
		`data: {"type":"message_delta","usage":{"output_tokens":45}}` + "\n\n" +
		`data: {"type":"message_delta","usage":{"output_tokens":90}}` + "\n\n"

	fake := newFakeReadCloser([]byte(full))

	var gotInput, gotOutput int
	var gotKnown, doneCalled bool
	body := newUsageScanningBody(fake, true, true, func(input, output int, known bool) {
		doneCalled = true
		gotInput, gotOutput, gotKnown = input, output, known
	})

	got := readInChunks(t, body, 7)
	if string(got) != full {
		t.Fatalf("passthrough bytes altered: got %q, want %q", got, full)
	}

	if err := body.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !doneCalled {
		t.Fatal("onDone was never called")
	}
	if !gotKnown {
		t.Fatal("known = false, want true")
	}
	if gotInput != 123 {
		t.Errorf("input tokens = %d, want 123 (from message_start, unaffected by later events)", gotInput)
	}
	if gotOutput != 90 {
		t.Errorf("output tokens = %d, want 90 (the max seen across message_delta events)", gotOutput)
	}
	if fake.closeCount != 1 {
		t.Errorf("upstream Close() called %d times, want 1", fake.closeCount)
	}
}

func TestUsageScanningBody_JSON_ExtractsUsage(t *testing.T) {
	const full = `{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":20}}`

	fake := newFakeReadCloser([]byte(full))
	var gotInput, gotOutput int
	var gotKnown bool
	body := newUsageScanningBody(fake, false, true, func(input, output int, known bool) {
		gotInput, gotOutput, gotKnown = input, output, known
	})

	got := readInChunks(t, body, 5)
	if string(got) != full {
		t.Fatalf("passthrough bytes altered: got %q, want %q", got, full)
	}
	body.Close()

	if !gotKnown || gotInput != 10 || gotOutput != 20 {
		t.Errorf("usage = %d/%d known=%v, want 10/20 known=true", gotInput, gotOutput, gotKnown)
	}
}

func TestUsageScanningBody_JSON_MalformedBodyStillPassesThrough(t *testing.T) {
	const full = `not valid json at all`

	fake := newFakeReadCloser([]byte(full))
	var gotKnown bool
	body := newUsageScanningBody(fake, false, true, func(_, _ int, known bool) {
		gotKnown = known
	})

	got := readInChunks(t, body, 4)
	if string(got) != full {
		t.Fatalf("passthrough bytes altered: got %q, want %q", got, full)
	}
	body.Close()

	if gotKnown {
		t.Error("known = true, want false for a body that isn't valid JSON")
	}
}

func TestUsageScanningBody_ScanDisabled_NeverExtractsEvenIfContentLooksLikeUsage(t *testing.T) {
	// Simulates a non-2xx response: modifyResponse sets scan=false
	// regardless of what the error body happens to contain.
	const full = `{"usage":{"input_tokens":999,"output_tokens":999}}`

	fake := newFakeReadCloser([]byte(full))
	var gotKnown bool
	var doneCalled bool
	body := newUsageScanningBody(fake, false, false, func(_, _ int, known bool) {
		doneCalled = true
		gotKnown = known
	})

	got := readInChunks(t, body, 128)
	if string(got) != full {
		t.Fatalf("passthrough bytes altered even though scanning was disabled: got %q, want %q", got, full)
	}
	body.Close()

	if !doneCalled {
		t.Fatal("onDone was never called")
	}
	if gotKnown {
		t.Error("known = true, want false when scanning is disabled")
	}
}

func TestUsageScanningBody_JSON_OversizedBodyIsTruncatedForScanningButNotForTheClient(t *testing.T) {
	// A body past maxJSONScanBytes must still reach the client in full;
	// only the scanning copy is capped, truncating the JSON mid-string and
	// making known=false the observable proof the cap applied.
	padding := strings.Repeat("x", maxJSONScanBytes+1024)
	full := `{"usage":{"input_tokens":1,"output_tokens":2},"padding":"` + padding + `"}`

	fake := newFakeReadCloser([]byte(full))
	var gotKnown bool
	body := newUsageScanningBody(fake, false, true, func(_, _ int, known bool) {
		gotKnown = known
	})

	got := readInChunks(t, body, 256*1024)
	if len(got) != len(full) {
		t.Fatalf("passthrough length = %d, want %d - the cap must never affect what the client receives", len(got), len(full))
	}
	if string(got) != full {
		t.Fatal("passthrough bytes altered for an oversized body")
	}
	body.Close()

	if gotKnown {
		t.Error("known = true, want false - the scan buffer should have been truncated mid-string, making it invalid JSON")
	}
}

func TestUsageScanningBody_CloseIsIdempotent(t *testing.T) {
	fake := newFakeReadCloser([]byte(`{"usage":{"input_tokens":1,"output_tokens":1}}`))
	calls := 0
	body := newUsageScanningBody(fake, false, true, func(_, _ int, _ bool) {
		calls++
	})

	io.ReadAll(body)
	body.Close()
	body.Close() // must not double-report even if called again

	if calls != 1 {
		t.Errorf("onDone called %d times, want exactly 1", calls)
	}
}
