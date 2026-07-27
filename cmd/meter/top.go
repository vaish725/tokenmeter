// `meter top`: R7's "what cost the most today" in one command.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/vaish725/tokenmeter/internal/config"
	"github.com/vaish725/tokenmeter/internal/store"
)

func runTop(args []string) {
	fs := flag.NewFlagSet("top", flag.ExitOnError)
	since := fs.Duration("since", 24*time.Hour, "how far back to look")
	limit := fs.Int("limit", 10, "how many requests to show")
	fs.Parse(args)

	cfg := config.Load()
	st, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("meter top: opening store: %v", err)
	}
	defer st.Close()

	records, err := st.TopRequests(context.Background(), time.Now().Add(-*since), *limit)
	if err != nil {
		log.Fatalf("meter top: querying: %v", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "TIME\tPROJECT\tMODEL\tCOST\tTOKENS IN/OUT\tSTATUS\tLATENCY")
	for _, r := range records {
		fmt.Fprintf(w, "%s\t%s\t%s\t$%.4f\t%d/%d\t%d\t%dms\n",
			r.Timestamp.Local().Format("15:04:05"), r.Project, r.Model, r.CostUSD,
			r.InputTokens, r.OutputTokens, r.StatusCode, r.LatencyMS)
	}
	w.Flush()
}
