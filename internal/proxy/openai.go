package proxy

import (
	"encoding/json"

	"github.com/vaish725/tokenmeter/internal/apikeys"
	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/downshift"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// openAISpec: OpenAI's Chat Completions usage block is
// {"usage":{"prompt_tokens":N,"completion_tokens":N}}.
var openAISpec = spec{
	name:        "openai",
	path:        "/v1/chat/completions",
	usageFields: newUsageFields("prompt_tokens", "completion_tokens"),
	rewriteBody: injectStreamUsageOption,
}

// NewOpenAI builds a Proxy for the OpenAI Chat Completions API. dt and ak
// may both be nil (downshift / API-key attribution disabled); cc's zero
// value disables prompt capture.
func NewOpenAI(upstreamURL string, st *store.Store, pt *pricing.Table, bl *budget.Ledger, dt *downshift.Table, ak *apikeys.Table, cc CaptureConfig) (*Proxy, error) {
	return newProxy(openAISpec, upstreamURL, st, pt, bl, dt, ak, cc)
}

// injectStreamUsageOption is OpenAI's one provider-specific body rewrite:
// it only includes usage in a streamed response if stream_options.
// include_usage is set, which most SDKs don't set by default. Without
// this, every streaming OpenAI request would log usage_known=false. A
// caller's own stream_options is never overridden; a decode failure leaves
// the body untouched and lets the upstream reject it on its own.
func injectStreamUsageOption(body []byte, meta requestMeta) []byte {
	if !meta.Stream {
		return body
	}

	var generic map[string]any
	if err := json.Unmarshal(body, &generic); err != nil {
		return body
	}
	if _, exists := generic["stream_options"]; exists {
		return body
	}

	generic["stream_options"] = map[string]bool{"include_usage": true}
	rewritten, err := json.Marshal(generic)
	if err != nil {
		return body
	}
	return rewritten
}
