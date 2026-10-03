package probe

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/libxray/xray"
	xlog "github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/transport/internet"
)

// A freedom outbound is a live server for these tests; a blackhole a dead one.
const (
	live = `{"protocol":"freedom","tag":"proxy"}`
	dead = `{"protocol":"blackhole","tag":"proxy"}`
)

func useFastTiming(t *testing.T) {
	t.Helper()
	old := timing
	timing = fastTiming
	t.Cleanup(func() { CloseHost(); timing = old })
}

// endpoint answers 204 after a few ms (a reading must be > 0 ms). With
// inFlight/peak set it holds each request long enough to count overlap.
func endpoint(t *testing.T, inFlight, peak *atomic.Int32) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if inFlight != nil {
			n := inFlight.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			defer inFlight.Add(-1)
			time.Sleep(40 * time.Millisecond)
		}
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/generate_204"
}

func servers(pairs ...string) []Server {
	var out []Server
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, Server{ID: pairs[i], Outbound: json.RawMessage(pairs[i+1])})
	}
	return out
}

// drain polls until the run is done, returning every reading by id.
func drain(t *testing.T) map[string]Reading {
	t.Helper()
	got := map[string]Reading{}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r := Results()
		for _, x := range r.Results {
			if _, dup := got[x.ID]; dup {
				t.Fatalf("%s delivered twice", x.ID)
			}
			got[x.ID] = x
		}
		if r.Done {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the run never finished")
	return nil
}

func TestProbeMeasuresLiveAndDeadServers(t *testing.T) {
	useFastTiming(t)
	resp, err := Start(StartRequest{URL: endpoint(t, nil, nil), Servers: servers("a", live, "b", dead), Measure: []string{"a", "b"}})
	if err != nil || len(resp.Dropped) != 0 || resp.DeadlineMs <= 0 {
		t.Fatalf("start: %+v %v", resp, err)
	}
	got := drain(t)
	if got["a"].Failed || got["a"].Ms <= 0 || !got["b"].Failed {
		t.Fatalf("got %+v", got)
	}
}

func TestProbeDropsWhatItCannotBuildOrDoesNotHold(t *testing.T) {
	useFastTiming(t)
	resp, err := Start(StartRequest{URL: endpoint(t, nil, nil),
		Servers: servers("a", live, "bad", `{"protocol":"no-such-protocol"}`),
		Measure: []string{"a", "bad", "ghost"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Dropped) != 2 {
		t.Fatalf("dropped %v", resp.Dropped)
	}
	if got := drain(t); len(got) != 1 || got["a"].Ms <= 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestProbeNeverMeasuresMoreThanEightAtOnce(t *testing.T) {
	useFastTiming(t)
	var inFlight, peak atomic.Int32
	url := endpoint(t, &inFlight, &peak)
	var pairs, ids []string
	for i := 0; i < 20; i++ {
		id := "s" + strconv.Itoa(i)
		pairs = append(pairs, id, live)
		ids = append(ids, id)
	}
	if _, err := Start(StartRequest{URL: url, Servers: servers(pairs...), Measure: ids}); err != nil {
		t.Fatal(err)
	}
	if got := drain(t); len(got) != 20 {
		t.Fatalf("%d readings", len(got))
	}
	if p := peak.Load(); p > Concurrency || p < 2 {
		t.Fatalf("peak %d in flight", p)
	}
}

func TestProbeResultsWithoutARunAreDone(t *testing.T) {
	StopSession()
	if r := Results(); !r.Done || r.Results == nil || len(r.Results) != 0 {
		t.Fatalf("got %+v", r)
	}
}

func TestProbeANewStartReplacesTheRunCleanly(t *testing.T) {
	useFastTiming(t)
	url := endpoint(t, nil, nil)
	if _, err := Start(StartRequest{URL: url, Servers: servers("a", live, "b", dead), Measure: []string{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(StartRequest{URL: url, Servers: servers("a", live, "b", dead), Measure: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if got := drain(t); len(got) != 1 || got["a"].Ms <= 0 {
		t.Fatalf("the replaced run leaked: %+v", got)
	}
	StopSession()
	StopSession() // idempotent
}

func TestProbeKeepsUnchangedHandlersAcrossSyncs(t *testing.T) {
	useFastTiming(t)
	url := endpoint(t, nil, nil)
	if _, err := Start(StartRequest{URL: url, Servers: servers("a", live, "b", live), Measure: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	drain(t)
	before := heldTags()
	if _, err := Start(StartRequest{URL: url, Servers: servers("a", live, "b", dead, "c", live), Measure: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	drain(t)
	after := heldTags()
	if after["a"] != before["a"] {
		t.Fatalf("an unchanged server was rebuilt: %v → %v", before, after)
	}
	if after["b"] == before["b"] || after["c"] == "" {
		t.Fatalf("changed or new servers not synced: %v → %v", before, after)
	}
	if _, err := Start(StartRequest{URL: url, Servers: servers("a", live)}); err != nil {
		t.Fatal(err)
	}
	if got := heldTags(); len(got) != 1 {
		t.Fatalf("removed servers still held: %v", got)
	}
}

// core.New repoints the process-global dialer at every new instance; the
// tunnel's DNS pins must survive the probe host being created, used and
// closed.
func TestProbeLeavesTheTunnelsDialerAlone(t *testing.T) {
	useFastTiming(t)
	cfg := filepath.Join(t.TempDir(), "tunnel.json")
	tunnelConfig := `{"log":{"loglevel":"none"},"dns":{"hosts":{"pinned.test":"10.9.8.7"}},"outbounds":[{"protocol":"freedom"}]}`
	if err := os.WriteFile(cfg, []byte(tunnelConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := xray.RunXray(cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { xray.StopXray() })
	tunnel := xray.Instance()
	pinned := func() bool {
		ips, err := internet.LookupForIP("pinned.test", internet.DomainStrategy_USE_IP4, nil)
		return err == nil && len(ips) == 1 && ips[0].String() == "10.9.8.7"
	}
	if !pinned() {
		t.Fatal("the pin is not active before the probe")
	}
	if _, err := Start(StartRequest{URL: endpoint(t, nil, nil), Servers: servers("a", live), Measure: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	if !pinned() {
		t.Fatal("the probe host took over the tunnel's dialer")
	}
	drain(t)
	CloseHost()
	if !pinned() || xray.Instance() != tunnel || !tunnel.IsRunning() {
		t.Fatal("the tunnel was disturbed")
	}
}

type capture struct{ n atomic.Int32 }

func (c *capture) Handle(xlog.Message) { c.n.Add(1) }

func TestProbeLeavesTheLogHandlerAlone(t *testing.T) {
	useFastTiming(t)
	c := &capture{}
	xlog.RegisterHandler(c)
	if _, err := Start(StartRequest{URL: endpoint(t, nil, nil), Servers: servers("a", live), Measure: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	drain(t)
	CloseHost()
	before := c.n.Load()
	xlog.Record(&xlog.GeneralMessage{Severity: xlog.Severity_Warning, Content: "still mine"})
	if c.n.Load() != before+1 {
		t.Fatal("the probe replaced the process log handler")
	}
}

func TestProbeStopAndCloseLeaveNoGoroutinesBehind(t *testing.T) {
	useFastTiming(t)
	url := endpoint(t, nil, nil)
	baseline := runtime.NumGoroutine()
	if _, err := Start(StartRequest{URL: url, Servers: servers("a", live, "b", dead), Measure: []string{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	StopSession()
	CloseHost()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > baseline+2 {
		t.Fatalf("goroutines %d, baseline %d", n, baseline)
	}
}
