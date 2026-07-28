// `meter export`: dumps a time window of requests as CSV - a full window,
// not a top-N ranking like `meter top`.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/vaish725/tokenmeter/internal/config"
	"github.com/vaish725/tokenmeter/internal/store"
)

func runExport(args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	since := fs.Duration("since", 24*time.Hour, "how far back to export")
	outPath := fs.String("out", "", "output file (default: stdout)")
	fs.Parse(args)

	cfg := config.Load()
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("meter export: opening store: %v", err)
	}
	defer st.Close()

	records, err := st.AllRequests(context.Background(), time.Now().Add(-*since))
	if err != nil {
		log.Fatalf("meter export: querying: %v", err)
	}

	out := os.Stdout
	if *outPath != "" {
		f, err := os.Create(*outPath)
		if err != nil {
			log.Fatalf("meter export: creating %s: %v", *outPath, err)
		}
		defer f.Close()
		out = f
	}

	w := csv.NewWriter(out)
	defer w.Flush()

	w.Write([]string{"timestamp", "project", "provider", "model", "input_tokens", "output_tokens", "cost_usd", "latency_ms", "status_code", "stream", "usage_known"})
	for _, r := range records {
		w.Write([]string{
			r.Timestamp.UTC().Format(time.RFC3339),
			r.Project,
			r.Provider,
			r.Model,
			fmt.Sprint(r.InputTokens),
			fmt.Sprint(r.OutputTokens),
			fmt.Sprint(r.CostUSD),
			fmt.Sprint(r.LatencyMS),
			fmt.Sprint(r.StatusCode),
			fmt.Sprint(r.Stream),
			fmt.Sprint(r.UsageKnown),
		})
	}
}
