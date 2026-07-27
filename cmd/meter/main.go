// Command meter runs the local LLM spend proxy described in prd.md. Point
// ANTHROPIC_BASE_URL at this process's listen address - nothing else about
// how a client calls Anthropic needs to change.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/config"
	"github.com/vaish725/tokenmeter/internal/pricing"
	"github.com/vaish725/tokenmeter/internal/proxy"
	"github.com/vaish725/tokenmeter/internal/store"
)

// shutdownTimeout bounds how long meter drains in-flight requests on
// SIGTERM/SIGINT before forcing a close.
const shutdownTimeout = 30 * time.Second

func main() {
	if len(os.Args) > 1 && os.Args[1] == "top" {
		runTop(os.Args[2:])
		return
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

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("meter: opening store: %v", err)
	}
	defer st.Close()

	anthropicProxy, err := proxy.New(cfg.UpstreamURL, st, pricingTable, ledger)
	if err != nil {
		log.Fatalf("meter: building proxy: %v", err)
	}

	mux := http.NewServeMux()
	// The only accounted path; everything else still proxies through, just
	// without attribution/cost tracking.
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
