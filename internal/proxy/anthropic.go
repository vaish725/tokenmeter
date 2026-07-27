package proxy

import (
	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/store"
)

// anthropicSpec: Anthropic's Messages API usage block is
// {"usage":{"input_tokens":N,"output_tokens":N}}, no body rewriting needed.
var anthropicSpec = spec{
	name:        "anthropic",
	path:        "/v1/messages",
	usageFields: newUsageFields("input_tokens", "output_tokens"),
}

// NewAnthropic builds a Proxy for the Anthropic Messages API.
func NewAnthropic(upstreamURL string, st *store.Store, pt *pricing.Table, bl *budget.Ledger) (*Proxy, error) {
	return newProxy(anthropicSpec, upstreamURL, st, pt, bl)
}
