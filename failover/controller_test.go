package failover

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeProber struct {
	mu    sync.Mutex
	up    map[string]bool
	panic bool
}

func (f *fakeProber) set(tag string, up bool) { f.mu.Lock(); f.up[tag] = up; f.mu.Unlock() }

func (f *fakeProber) Probe(ctx context.Context, tag string, fresh bool) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.panic {
		panic("probe exploded")
	}
	return f.up[tag]
}

type fakePinner struct {
	mu       sync.Mutex
	pinned   string
	unpinned bool
}

func (f *fakePinner) Pin(tag string) error { f.mu.Lock(); f.pinned = tag; f.mu.Unlock(); return nil }
func (f *fakePinner) Unpin()               { f.mu.Lock(); f.unpinned = true; f.pinned = ""; f.mu.Unlock() }
func (f *fakePinner) get() string          { f.mu.Lock(); defer f.mu.Unlock(); return f.pinned }

func fastTiming() Timing {
	return Timing{
		ProbeInterval:   5 * time.Millisecond,
		StartupInterval: 2 * time.Millisecond, StartupWindow: 10 * time.Millisecond,
		FailThreshold: 3, SearchBackoff: 40 * time.Millisecond, DemoteFor: time.Hour,
		FreshEvery: time.Hour, ProbeTimeout: 50 * time.Millisecond, VerifyBatch: 3,
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func startController(t *testing.T, prober *fakeProber, pinner *fakePinner, order ...string) *Controller {
	t.Helper()
	labels := map[string]string{"a": "Germany", "b": "Netherlands", "c": "France"}
	c, err := NewController(fastTiming(), order, labels, prober, pinner, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c
}

func TestControllerPinsFirstOnStart(t *testing.T) {
	prober := &fakeProber{up: map[string]bool{"a": true, "b": true}}
	pinner := &fakePinner{}
	c := startController(t, prober, pinner, "a", "b")
	if pinner.get() != "a" || c.State().Current != "a" || !c.State().Running {
		t.Fatalf("pinned %q, state %+v", pinner.get(), c.State())
	}
}

func TestControllerSwitchesAfterOutage(t *testing.T) {
	prober := &fakeProber{up: map[string]bool{"a": false, "b": true, "c": true}}
	pinner := &fakePinner{}
	c := startController(t, prober, pinner, "a", "b", "c")
	eventually(t, "switch to b", func() bool { return pinner.get() == "b" })
	st := c.State()
	if st.Current != "b" || st.CurrentLabel != "Netherlands" || st.LastSwitch == nil {
		t.Fatalf("state %+v", st)
	}
	sw := st.LastSwitch
	if sw.From != "a" || sw.FromLabel != "Germany" || sw.To != "b" || sw.Reason != ReasonUnreachable || sw.Seq != 1 {
		t.Fatalf("switch %+v", sw)
	}
}

// A server that fails once and passes its confirmation stays: a blip is not
// an outage.
func TestControllerKeepsServerThatPassesConfirmation(t *testing.T) {
	prober := &blipProber{up: map[string]bool{"a": true, "b": true}, failFirst: "a"}
	pinner := &fakePinner{}
	c := startWith(t, prober, pinner, "a", "b")
	time.Sleep(100 * time.Millisecond)
	if pinner.get() != "a" || c.State().LastSwitch != nil {
		t.Fatalf("switched on one failure: pinned %q, state %+v", pinner.get(), c.State())
	}
}

// The first fallbacks are verified together with the confirmation: a
// confirmed outage switches without probing them again.
func TestControllerSwitchesOnTheConfirmationsFallbacks(t *testing.T) {
	pinner := &fakePinner{}
	prober := &countingProber{up: map[string]bool{"a": false, "b": true}, pinner: pinner, probes: map[string]int{}}
	startWith(t, prober, pinner, "a", "b")
	eventually(t, "switch to b", func() bool { return pinner.get() == "b" })
	if got := prober.before("b"); got != 1 {
		t.Fatalf("b probed %d times before the switch, want once (alongside the confirmation)", got)
	}
}

func startWith(t *testing.T, prober Prober, pinner *fakePinner, order ...string) *Controller {
	t.Helper()
	c, err := NewController(fastTiming(), order, nil, prober, pinner, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c
}

// blipProber fails the first probe of failFirst, then answers by up.
type blipProber struct {
	mu        sync.Mutex
	up        map[string]bool
	failFirst string
	failed    bool
}

func (f *blipProber) Probe(ctx context.Context, tag string, fresh bool) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if tag == f.failFirst && !f.failed {
		f.failed = true
		return false
	}
	return f.up[tag]
}

// countingProber answers by up, and counts each tag's probes made before
// the pinner moved to it.
type countingProber struct {
	mu     sync.Mutex
	up     map[string]bool
	pinner *fakePinner
	probes map[string]int
}

func (f *countingProber) Probe(ctx context.Context, tag string, fresh bool) bool {
	pinned := f.pinner.get()
	f.mu.Lock()
	defer f.mu.Unlock()
	if pinned != tag {
		f.probes[tag]++
	}
	return f.up[tag]
}

func (f *countingProber) before(tag string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.probes[tag]
}

func TestControllerStaysWhenNoFallbackIsHealthy(t *testing.T) {
	prober := &fakeProber{up: map[string]bool{}}
	pinner := &fakePinner{}
	c := startController(t, prober, pinner, "a", "b")
	eventually(t, "noFallback event", func() bool {
		sw := c.State().LastSwitch
		return sw != nil && sw.Reason == ReasonNoFallback
	})
	time.Sleep(150 * time.Millisecond) // several more searches
	st := c.State()
	if pinner.get() != "a" || st.Current != "a" || st.LastSwitch.Seq != 1 {
		t.Fatalf("pinned %q, state %+v (want one noFallback event, still on a)", pinner.get(), st)
	}
}

func TestControllerDoesNotReturnToRecoveredServer(t *testing.T) {
	prober := &fakeProber{up: map[string]bool{"a": false, "b": true}}
	pinner := &fakePinner{}
	c := startController(t, prober, pinner, "a", "b")
	eventually(t, "switch to b", func() bool { return pinner.get() == "b" })
	prober.set("a", true)
	time.Sleep(100 * time.Millisecond)
	if pinner.get() != "b" || c.State().Current != "b" {
		t.Fatalf("returned to a: pinned %q", pinner.get())
	}
}

func TestControllerStopIsPrompt(t *testing.T) {
	prober := &fakeProber{up: map[string]bool{"a": true}}
	c, err := NewController(fastTiming(), []string{"a"}, nil, prober, &fakePinner{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { c.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop hung")
	}
	if c.State().Running {
		t.Fatal("still running after Stop")
	}
}

func TestControllerRecoversPanicAndUnpins(t *testing.T) {
	prober := &fakeProber{up: map[string]bool{}, panic: true}
	pinner := &fakePinner{}
	c := startController(t, prober, pinner, "a", "b")
	eventually(t, "loop ends", func() bool { return !c.State().Running })
	pinner.mu.Lock()
	defer pinner.mu.Unlock()
	if !pinner.unpinned {
		t.Fatal("override not cleared after a panic")
	}
}
