// Package config resolves meter's runtime settings from environment variables.
//
// Every setting has a default so the binary runs with zero configuration -
// matching the PRD goal that adopting meter costs nothing more than pointing
// ANTHROPIC_BASE_URL at it. Flags are intentionally not used here: env vars
// are what a shell profile or launchd unit sets once and forgets, which is
// the expected daily-driver deployment shape for this tool.
package config

import "os"

// Config holds everything the rest of the program needs to start.
type Config struct {
	// ListenAddr is where meter's HTTP server binds, e.g. "127.0.0.1:8080".
	ListenAddr string

	// UpstreamURL is the real provider base URL that requests get forwarded to.
	// Only the Anthropic Messages API is wired up in week 1.
	UpstreamURL string

	// DBPath is the SQLite file meter persists request metadata to.
	DBPath string

	// PricingPath is the JSON file holding per-model $/token rates.
	PricingPath string
}

// Environment variable names, kept as constants so main.go and tests agree.
const (
	envListenAddr  = "METER_LISTEN_ADDR"
	envUpstreamURL = "METER_ANTHROPIC_UPSTREAM"
	envDBPath      = "METER_DB_PATH"
	envPricingPath = "METER_PRICING_PATH"
)

// Defaults chosen so `meter` with no environment set up at all still runs.
const (
	defaultListenAddr  = "127.0.0.1:8080"
	defaultUpstreamURL = "https://api.anthropic.com"
	defaultDBPath      = "./meter.db"
	defaultPricingPath = "./configs/pricing.json"
)

// Load reads configuration from the environment, falling back to defaults
// for anything unset. It never fails - there is no required setting - which
// keeps startup simple and matches "a broken meter must never block a
// working workflow" from the PRD's risk section.
func Load() Config {
	return Config{
		ListenAddr:  getEnvOrDefault(envListenAddr, defaultListenAddr),
		UpstreamURL: getEnvOrDefault(envUpstreamURL, defaultUpstreamURL),
		DBPath:      getEnvOrDefault(envDBPath, defaultDBPath),
		PricingPath: getEnvOrDefault(envPricingPath, defaultPricingPath),
	}
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
