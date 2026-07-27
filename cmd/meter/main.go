// Command meter runs the local LLM spend proxy described in prd.md: a
// streaming-safe, accounted pass-through for the Anthropic Messages API,
// with every request logged to SQLite. Point ANTHROPIC_BASE_URL at this
// process's listen address and nothing else about how Claude Code / an SDK
// / a script calls Anthropic needs to change - that "zero code changes at
// call sites" property is the whole point of a base-URL proxy.
package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/vaish725/tokenmeter/internal/config"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/proxy"
	"github.com/vaish725/tokenmeter/internal/store"
)

// shutdownTimeout bounds how long meter waits for in-flight requests to
// finish on SIGTERM/SIGINT before forcing a close. Long enough to let a
// real LLM call complete, short enough that a stuck connection doesn't hang
// a shutdown forever.
const shutdownTimeout = 30 * time.Second

func main() {
	cfg := config.Load()

	pricingTable, err := pricing.Load(cfg.PricingPath)
	if err != nil {
		log.Fatalf("meter: loading pricing table: %v", err)
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("meter: opening store: %v", err)
	}
	defer st.Close()

	anthropicProxy, err := proxy.New(cfg.UpstreamURL, st, pricingTable)
	if err != nil {
		log.Fatalf("meter: building proxy: %v", err)
	}

	mux := http.NewServeMux()
	// The only path meter actually accounts for right now. Everything else
	// (other Anthropic endpoints a client might hit) still gets proxied,
	// just without attribution/cost tracking.
	mux.HandleFunc("POST /v1/messages", anthropicProxy.HandleMessages)
	mux.HandleFunc("/", anthropicProxy.HandlePassthrough)

	server := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	// Listen for SIGINT/SIGTERM so a Ctrl-C or `systemctl stop` drains
	// in-flight requests instead of dropping them mid-call.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("meter: listening on %s, forwarding to %s", cfg.ListenAddr, cfg.UpstreamURL)
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("meter: server error: %v", err)
		}
	case <-ctx.Done():
		log.Printf("meter: shutdown signal received, draining in-flight requests")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("meter: graceful shutdown did not complete cleanly: %v", err)
		}
	}
}
