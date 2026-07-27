package budget

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newTestLedger writes a caps.json with the given values and loads it.
func newTestLedger(t *testing.T, globalCap, defaultProjectCap float64, overrides map[string]float64) *Ledger {
	t.Helper()

	caps := Caps{
		GlobalDailyCapUSD:    globalCap,
		DefaultProjectCapUSD: defaultProjectCap,
		ProjectCapsUSD:       overrides,
	}
	data, err := json.Marshal(caps)
	if err != nil {
		t.Fatalf("marshaling test caps: %v", err)
	}

	path := filepath.Join(t.TempDir(), "caps.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("writing test caps file: %v", err)
	}

	l, err := New(path)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return l
}

// The PRD-mandated test: N concurrent reservations against a cap sized for
// N/2 must let exactly N/2 through, never more.
func TestReserve_ConcurrentRequestsRespectCap(t *testing.T) {
	const n = 100
	const costPerRequest = 1.0
	const wantSucceed = n / 2

	l := newTestLedger(t, costPerRequest*wantSucceed, 1000, nil)

	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, _ := l.Reserve("proj", costPerRequest)
			if ok {
				mu.Lock()
				succeeded++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if succeeded != wantSucceed {
		t.Errorf("succeeded = %d, want exactly %d", succeeded, wantSucceed)
	}
}

func TestReserve_ProjectCapRejectsBeforeGlobal(t *testing.T) {
	l := newTestLedger(t, 100, 1, nil) // global plenty, project tight

	if _, ok, _ := l.Reserve("proj", 1); !ok {
		t.Fatal("first reservation at exactly the project cap should succeed")
	}

	_, ok, decision := l.Reserve("proj", 0.01)
	if ok {
		t.Fatal("reservation over the project cap should be rejected")
	}
	if decision.Cap != "project" {
		t.Errorf("decision.Cap = %q, want %q", decision.Cap, "project")
	}
}

func TestReserve_GlobalCapRejectsAcrossProjects(t *testing.T) {
	l := newTestLedger(t, 1, 100, nil) // global tight, project plenty each

	if _, ok, _ := l.Reserve("proj-a", 1); !ok {
		t.Fatal("first reservation at exactly the global cap should succeed")
	}

	_, ok, decision := l.Reserve("proj-b", 0.01)
	if ok {
		t.Fatal("a second project pushing past the global cap should be rejected")
	}
	if decision.Cap != "global" {
		t.Errorf("decision.Cap = %q, want %q", decision.Cap, "global")
	}
}

func TestReserve_UnlistedProjectGetsDefaultCap(t *testing.T) {
	l := newTestLedger(t, 100, 2, map[string]float64{"special": 50})

	if _, ok, _ := l.Reserve("unattributed", 2); !ok {
		t.Fatal("reservation at exactly the default cap should succeed")
	}
	if _, ok, _ := l.Reserve("unattributed", 0.01); ok {
		t.Fatal("reservation over the default cap should be rejected")
	}
	if _, ok, _ := l.Reserve("special", 10); !ok {
		t.Fatal("a project with an explicit override should use it, not the default")
	}
}

func TestReconcile_ReleasesEstimateAndCommitsActual(t *testing.T) {
	l := newTestLedger(t, 100, 5, nil)

	id, ok, _ := l.Reserve("proj", 4) // reserve pessimistically...
	if !ok {
		t.Fatal("initial reservation should succeed")
	}
	l.Reconcile(id, 1) // ...but it actually only cost 1

	// Headroom should now reflect the real cost (1), not the estimate (4):
	// a second reservation of 3.9 should fit under the 5 cap (1 + 3.9 < 5).
	if _, ok, _ := l.Reserve("proj", 3.9); !ok {
		t.Fatal("reservation should succeed once the estimate was released and replaced by the smaller actual cost")
	}
}

func TestReserve_OrphanedReservationExpiresAndFreesBudget(t *testing.T) {
	l := newTestLedger(t, 100, 1, nil)
	l.orphanTimeout = 10 * time.Millisecond

	if _, ok, _ := l.Reserve("proj", 1); !ok {
		t.Fatal("initial reservation at the cap should succeed")
	}
	if _, ok, _ := l.Reserve("proj", 0.5); ok {
		t.Fatal("a second reservation before the timeout should be rejected")
	}

	time.Sleep(20 * time.Millisecond)

	if _, ok, _ := l.Reserve("proj", 0.5); !ok {
		t.Fatal("the orphaned reservation should have expired, freeing budget for a new one")
	}
}
