// `meter watch`: a live spend dashboard - today's spend by project, burn
// rate, and time-to-cap, refreshed on an interval. Read-only; no raw-mode
// keyboard input, just Ctrl-C to quit.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/vaish725/tokenmeter/internal/budget"
	"github.com/vaish725/tokenmeter/internal/config"
	"github.com/vaish725/tokenmeter/internal/store"
)

// burnRateWindow is the trailing window used to compute $/hr - short
// enough to reflect current activity, long enough not to be noisy.
const burnRateWindow = 15 * time.Minute

func runWatch(args []string) {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	interval := fs.Duration("interval", 2*time.Second, "refresh interval")
	fs.Parse(args)

	cfg := config.Load()

	caps, err := budget.LoadCaps(cfg.CapsPath)
	if err != nil {
		log.Fatalf("meter watch: loading caps: %v", err)
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("meter watch: opening store: %v", err)
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	render(ctx, st, caps)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nmeter watch: stopped")
			return
		case <-ticker.C:
			render(ctx, st, caps)
		}
	}
}

// render queries the store's own persisted rows - this is a separate CLI
// process from `meter serve`, so it has no visibility into that process's
// in-memory reservations, only reconciled spend. Fine for a dashboard;
// reservations are short-lived.
func render(ctx context.Context, st *store.Store, caps budget.Caps) {
	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)

	today, err := st.SpendByProject(ctx, todayStart)
	if err != nil {
		log.Printf("meter watch: querying today's spend: %v", err)
		return
	}
	recent, err := st.SpendByProject(ctx, now.Add(-burnRateWindow))
	if err != nil {
		log.Printf("meter watch: querying recent spend: %v", err)
		return
	}

	projects := make([]string, 0, len(today))
	for p := range today {
		projects = append(projects, p)
	}
	sort.Strings(projects)

	// \033[H\033[J: cursor home + clear to end of screen - less flicker
	// than a hard clear before every redraw.
	fmt.Print("\033[H\033[J")
	fmt.Printf("meter watch - %s (refreshing every tick, Ctrl-C to quit)\n\n", now.Format("15:04:05"))

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "PROJECT\tTODAY\tCAP\t$/HR\tTIME TO CAP")

	var globalToday, globalRecent float64
	for _, project := range projects {
		spent := today[project]
		rate := recent[project] / burnRateWindow.Hours()
		projectCap := caps.ProjectCap(project)
		fmt.Fprintf(w, "%s\t$%.4f\t$%.2f\t$%.4f\t%s\n", project, spent, projectCap, rate, timeToCap(projectCap-spent, rate))
		globalToday += spent
		globalRecent += recent[project]
	}
	globalRate := globalRecent / burnRateWindow.Hours()
	fmt.Fprintf(w, "GLOBAL\t$%.4f\t$%.2f\t$%.4f\t%s\n", globalToday, caps.GlobalDailyCapUSD, globalRate, timeToCap(caps.GlobalDailyCapUSD-globalToday, globalRate))
	w.Flush()
}

// timeToCap estimates how long until remaining budget runs out at
// ratePerHour. "over" once remaining is already gone; "-" when there's no
// measurable current burn (rather than a bogus divide-by-zero reading).
func timeToCap(remaining, ratePerHour float64) string {
	if remaining <= 0 {
		return "over"
	}
	if ratePerHour <= 0 {
		return "-"
	}
	hours := remaining / ratePerHour
	return time.Duration(hours * float64(time.Hour)).Round(time.Minute).String()
}
