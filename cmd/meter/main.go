// Command meter runs the local LLM spend proxy described in prd.md. Point
// ANTHROPIC_BASE_URL at this process's listen address - nothing else about
// how a client calls Anthropic needs to change.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vaish725/tokenmeter/internal/apikeys"
	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/config"
	"github.com/vaish725/tokenmeter/internal/downshift"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/proxy"
	"github.com/vaish725/tokenmeter/internal/store"
)

// shutdownTimeout bounds how long meter drains in-flight requests on
// SIGTERM/SIGINT before forcing a close.
const shutdownTimeout = 30 * time.Second

// version is overridden at build time via -ldflags "-X main.version=...",
// which goreleaser sets to the release tag; "dev" for a local build.
var version = "dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "top":
			runTop(os.Args[2:])
			return
		case "watch":
			runWatch(os.Args[2:])
			return
		case "version":
			fmt.Println("meter " + version)
			return
		}
	}
	runServe()
}

func runServe() {
	cfg := config.Load()

	pricingTable, err := pricing.Load(cfg.PricingPath)
	if err != nil {
		log.Fatalf("meter: loading pricing table: %v", err)
	}

	ledger, err := budget.New(cfg.CapsPath)
	if err != nil {
		log.Fatalf("meter: loading budget caps: %v", err)
	}

	// Downshift is opt-in: no configs/downshift.json means the policy is
	// disabled entirely (nil table), not an error - every project just
	// hard-blocks at cap the way it always has.
	downshiftTable, err := downshift.Load(cfg.DownshiftPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("meter: loading downshift policy: %v", err)
		}
		log.Printf("meter: no downshift policy at %s - caps hard-block at 429", cfg.DownshiftPath)
		downshiftTable = nil
	}

	// API-key attribution is opt-in the same way: no configs/api_keys.json
	// means that link of R4's chain is simply skipped.
	apiKeysTable, err := apikeys.Load(cfg.APIKeysPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("meter: loading API key mapping: %v", err)
		}
		log.Printf("meter: no API key mapping at %s - attribution falls back to %s header or unattributed", cfg.APIKeysPath, "X-Meter-Project")
		apiKeysTable = nil
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("meter: opening store: %v", err)
	}
	defer st.Close()

	anthropicProxy, err := proxy.NewAnthropic(cfg.AnthropicUpstreamURL, st, pricingTable, ledger, downshiftTable, apiKeysTable)
	if err != nil {
		log.Fatalf("meter: building anthropic proxy: %v", err)
	}
	openaiProxy, err := proxy.NewOpenAI(cfg.OpenAIUpstreamURL, st, pricingTable, ledger, downshiftTable, apiKeysTable)
	if err != nil {
		log.Fatalf("meter: building openai proxy: %v", err)
	}

	anthropicServer := &http.Server{Addr: cfg.ListenAddr, Handler: mux(anthropicProxy)}
	openaiServer := &http.Server{Addr: cfg.OpenAIListenAddr, Handler: mux(openaiProxy)}

	// Listen for SIGINT/SIGTERM so a Ctrl-C or `systemctl stop` drains
	// in-flight requests instead of dropping them mid-call.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 2)
	go func() {
		log.Printf("meter: anthropic listening on %s, forwarding to %s", cfg.ListenAddr, cfg.AnthropicUpstreamURL)
		serverErr <- anthropicServer.ListenAndServe()
	}()
	go func() {
		log.Printf("meter: openai listening on %s, forwarding to %s", cfg.OpenAIListenAddr, cfg.OpenAIUpstreamURL)
		serverErr <- openaiServer.ListenAndServe()
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
		if err := anthropicServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("meter: anthropic server shutdown did not complete cleanly: %v", err)
		}
		if err := openaiServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("meter: openai server shutdown did not complete cleanly: %v", err)
		}
	}
}

// mux builds a server's routes from a provider Proxy: its own accounted
// path, plus a catch-all passthrough for everything else that provider's
// SDK might call (e.g. GET /v1/models) without attribution/cost tracking.
func mux(p *proxy.Proxy) *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("POST "+p.Path(), p.HandleMessages)
	m.HandleFunc("/", p.HandlePassthrough)
	return m
}
