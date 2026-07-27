// Package config resolves meter's runtime settings from environment
// variables. Every setting has a default, so meter runs with zero config.
package config

import "os"

// Config holds everything the rest of the program needs to start.
type Config struct {
	// ListenAddr is where meter's HTTP server binds, e.g. "127.0.0.1:8080".
	ListenAddr string

	// UpstreamURL is the real provider base URL requests get forwarded to.
	UpstreamURL string

	// DBPath is the SQLite file meter persists request metadata to.
	DBPath string

	// PricingPath is the JSON file holding per-model $/token rates.
	PricingPath string

	// CapsPath is the JSON file holding daily spend caps.
	CapsPath string
}

// Environment variable names, kept as constants so main.go and tests agree.
const (
	envListenAddr  = "METER_LISTEN_ADDR"
	envUpstreamURL = "METER_ANTHROPIC_UPSTREAM"
	envDBPath      = "METER_DB_PATH"
	envPricingPath = "METER_PRICING_PATH"
	envCapsPath    = "METER_CAPS_PATH"
)

// Defaults chosen so `meter` with no environment set up at all still runs.
const (
	defaultListenAddr  = "127.0.0.1:8080"
	defaultUpstreamURL = "https://api.anthropic.com"
	defaultDBPath      = "./meter.db"
	defaultPricingPath = "./configs/pricing.json"
	defaultCapsPath    = "./configs/caps.json"
)

// Load reads config from the environment, defaulting anything unset. It
// never fails - there's no required setting.
func Load() Config {
	return Config{
		ListenAddr:  getEnvOrDefault(envListenAddr, defaultListenAddr),
		UpstreamURL: getEnvOrDefault(envUpstreamURL, defaultUpstreamURL),
		DBPath:      getEnvOrDefault(envDBPath, defaultDBPath),
		PricingPath: getEnvOrDefault(envPricingPath, defaultPricingPath),
		CapsPath:    getEnvOrDefault(envCapsPath, defaultCapsPath),
	}
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
