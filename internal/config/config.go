// Package config resolves meter's runtime settings from environment
// variables. Every setting has a default, so meter runs with zero config.
package config

import (
	"os"
	"strconv"
)

// Config holds everything the rest of the program needs to start. Two
// listen addresses, one per provider: ANTHROPIC_BASE_URL and
// OPENAI_BASE_URL clients could point at the same port, but both APIs
// expose overlapping paths (e.g. /v1/models), so a single listener
// couldn't tell an unmatched request's provider apart. Separate ports sidestep
// the ambiguity entirely.
type Config struct {
	// ListenAddr is where meter's Anthropic-facing server binds.
	ListenAddr string

	// OpenAIListenAddr is where meter's OpenAI-facing server binds.
	OpenAIListenAddr string

	// AnthropicUpstreamURL is the real Anthropic API requests forward to.
	AnthropicUpstreamURL string

	// OpenAIUpstreamURL is the real OpenAI API requests forward to.
	OpenAIUpstreamURL string

	// DBPath is the SQLite file meter persists request metadata to.
	DBPath string

	// PricingPath is the JSON file holding per-model $/token rates.
	PricingPath string

	// CapsPath is the JSON file holding daily spend caps.
	CapsPath string

	// DownshiftPath is the JSON file mapping model -> cheaper substitute.
	// The downshift policy is opt-in: a missing file (the default - none
	// ships in this repo) means every project hard-blocks at cap, unchanged
	// from before this policy existed.
	DownshiftPath string

	// APIKeysPath is the JSON file mapping an API key's last 8 characters
	// to a project. Opt-in like DownshiftPath: a missing file (the default)
	// means this attribution link is simply skipped.
	APIKeysPath string

	// CapturePrompts opts into storing request prompt bodies (never on by
	// default - see the PRD's own non-goal on this). CapturePromptsMaxBytes
	// bounds how much of a body gets kept, truncating anything larger.
	CapturePrompts         bool
	CapturePromptsMaxBytes int

	// MetricsListenAddr is where the Prometheus /metrics endpoint binds.
	MetricsListenAddr string
}

// Environment variable names, kept as constants so main.go and tests agree.
const (
	envListenAddr        = "METER_LISTEN_ADDR"
	envOpenAIListenAddr  = "METER_OPENAI_LISTEN_ADDR"
	envAnthropicUpstream = "METER_ANTHROPIC_UPSTREAM"
	envOpenAIUpstream    = "METER_OPENAI_UPSTREAM"
	envDBPath            = "METER_DB_PATH"
	envPricingPath       = "METER_PRICING_PATH"
	envCapsPath          = "METER_CAPS_PATH"
	envDownshiftPath     = "METER_DOWNSHIFT_PATH"
	envAPIKeysPath       = "METER_API_KEYS_PATH"
	envCapturePrompts    = "METER_CAPTURE_PROMPTS"
	envCaptureMaxBytes   = "METER_CAPTURE_PROMPTS_MAX_BYTES"
	envMetricsListenAddr = "METER_METRICS_LISTEN_ADDR"
)

// Defaults chosen so `meter` with no environment set up at all still runs.
const (
	defaultListenAddr        = "127.0.0.1:8080"
	defaultOpenAIListenAddr  = "127.0.0.1:8081"
	defaultAnthropicUpstream = "https://api.anthropic.com"
	defaultOpenAIUpstream    = "https://api.openai.com"
	defaultDBPath            = "./meter.db"
	defaultPricingPath       = "./configs/pricing.json"
	defaultCapsPath          = "./configs/caps.json"
	defaultDownshiftPath     = "./configs/downshift.json"
	defaultAPIKeysPath       = "./configs/api_keys.json"
	defaultCaptureMaxBytes   = 50 * 1024 // 50KB
	defaultMetricsListenAddr = "127.0.0.1:9090"
)

// Load reads config from the environment, defaulting anything unset. It
// never fails - there's no required setting.
func Load() Config {
	return Config{
		ListenAddr:           getEnvOrDefault(envListenAddr, defaultListenAddr),
		OpenAIListenAddr:     getEnvOrDefault(envOpenAIListenAddr, defaultOpenAIListenAddr),
		AnthropicUpstreamURL: getEnvOrDefault(envAnthropicUpstream, defaultAnthropicUpstream),
		OpenAIUpstreamURL:    getEnvOrDefault(envOpenAIUpstream, defaultOpenAIUpstream),
		DBPath:               getEnvOrDefault(envDBPath, defaultDBPath),
		PricingPath:          getEnvOrDefault(envPricingPath, defaultPricingPath),
		CapsPath:             getEnvOrDefault(envCapsPath, defaultCapsPath),
		DownshiftPath:        getEnvOrDefault(envDownshiftPath, defaultDownshiftPath),
		APIKeysPath:          getEnvOrDefault(envAPIKeysPath, defaultAPIKeysPath),

		CapturePrompts:         getEnvBool(envCapturePrompts, false),
		CapturePromptsMaxBytes: getEnvInt(envCaptureMaxBytes, defaultCaptureMaxBytes),
		MetricsListenAddr:      getEnvOrDefault(envMetricsListenAddr, defaultMetricsListenAddr),
	}
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getEnvBool parses key as a bool, defaulting (and silently ignoring an
// unparseable value) rather than failing startup over an opt-in flag.
func getEnvBool(key string, fallback bool) bool {
	v, err := strconv.ParseBool(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}

// getEnvInt parses key as an int, same defaulting behavior as getEnvBool.
func getEnvInt(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}
