package failover

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	_ "github.com/xtls/xray-core/main/distro/all"
)

// proxy-00 is a blackhole (a dead server), proxy-01 is freedom (a live one).
const testConfig = `{
  "log": {"loglevel": "none"},
  "outbounds": [
    {"tag": "proxy-00", "protocol": "blackhole"},
    {"tag": "proxy-01", "protocol": "freedom"}
  ],
  "routing": {"balancers": [{"tag": "smart", "selector": ["proxy-"], "strategy": {"type": "random"}}]}
}`

func startTestCore(t *testing.T) *core.Instance {
	t.Helper()
	instance, err := core.StartInstance("json", []byte(testConfig))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Stop(); instance.Close() })
	return instance
}

func probeServer(t *testing.T, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
	t.Cleanup(srv.Close)
	return srv.URL + "/generate_204"
}

func overrideOf(t *testing.T, instance *core.Instance) string {
	t.Helper()
	target, err := instance.GetFeature(routing.RouterType()).(routing.BalancerOverrider).GetOverrideTarget("smart")
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func TestXrayFailoverMovesOffDeadOutbound(t *testing.T) {
	instance := startTestCore(t)
	s := Settings{BalancerTag: "smart", Order: []string{"proxy-00", "proxy-01"}, ProbeURL: probeServer(t, http.StatusNoContent)}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "override on proxy-01", func() bool { return overrideOf(t, instance) == "proxy-01" })
	if st := CurrentState(); st.Current != "proxy-01" || st.LastSwitch == nil || st.LastSwitch.Reason != ReasonUnreachable {
		t.Fatalf("state %+v", st)
	}
}

func TestXrayFailoverStaysOnHealthyOutbound(t *testing.T) {
	instance := startTestCore(t)
	s := Settings{BalancerTag: "smart", Order: []string{"proxy-01", "proxy-00"}, ProbeURL: probeServer(t, http.StatusNoContent)}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond) // well past the startup window's probes
	if overrideOf(t, instance) != "proxy-01" || CurrentState().LastSwitch != nil {
		t.Fatalf("moved off a healthy server: %+v", CurrentState())
	}
}

func TestXrayFailoverTreats403AsDown(t *testing.T) {
	instance := startTestCore(t)
	s := Settings{BalancerTag: "smart", Order: []string{"proxy-01", "proxy-00"}, ProbeURL: probeServer(t, http.StatusForbidden)}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "noFallback", func() bool {
		sw := CurrentState().LastSwitch
		return sw != nil && sw.Reason == ReasonNoFallback
	})
}

func TestXrayFailoverRejectsUnknownTag(t *testing.T) {
	instance := startTestCore(t)
	err := Start(instance, Settings{BalancerTag: "smart", Order: []string{"proxy-09"}, ProbeURL: "http://x/"})
	if err == nil || CurrentState().Error == "" || CurrentState().Running {
		t.Fatalf("err %v, state %+v", err, CurrentState())
	}
}

// The app's region shape (XrayConfigBuilder): a `random` balancer per
// region with no fallbackTag and no observatory — random requires an
// observatory as soon as a fallbackTag is set, and a manual session has none.
// Blackholes are dead servers, freedoms live ones; `proxy` is a manual
// session's outbound, proxy-0N Smart's.
const regionConfig = `{
  "log": {"loglevel": "none"},
  "outbounds": [
    {"tag": "proxy", "protocol": "freedom"},
    {"tag": "proxy-00", "protocol": "blackhole"},
    {"tag": "proxy-01", "protocol": "freedom"},
    {"tag": "region-de-00", "protocol": "blackhole"},
    {"tag": "region-de-01", "protocol": "freedom"},
    {"tag": "region-tr-00", "protocol": "blackhole"}
  ],
  "routing": {"balancers": [
    {"tag": "smart", "selector": ["proxy-"], "strategy": {"type": "random"}},
    {"tag": "region-de", "selector": ["region-de-"], "strategy": {"type": "random"}},
    {"tag": "region-tr", "selector": ["region-tr-"], "strategy": {"type": "random"}}
  ]}
}`

func startRegionCore(t *testing.T) *core.Instance {
	t.Helper()
	instance, err := core.StartInstance("json", []byte(regionConfig))
	if err != nil {
		t.Fatalf("a session with regions and no observatory must start: %v", err)
	}
	t.Cleanup(func() { Stop(); instance.Close() })
	return instance
}

func overrideFor(t *testing.T, instance *core.Instance, balancer string) string {
	t.Helper()
	target, err := instance.GetFeature(routing.RouterType()).(routing.BalancerOverrider).GetOverrideTarget(balancer)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func TestXrayRegionsAloneRunWithoutMainController(t *testing.T) {
	instance := startRegionCore(t)
	url := probeServer(t, http.StatusNoContent)
	s := Settings{Regions: []RegionSettings{{BalancerTag: "region-de", Order: []string{"region-de-00", "region-de-01"}, ProbeURL: url}}}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "region-de on its live server", func() bool { return overrideFor(t, instance, "region-de") == "region-de-01" })
	st := CurrentState()
	if st.Running || st.Error != "" {
		t.Fatalf("no main controller was asked for, none must be reported: %+v", st)
	}
	if r := st.Regions["region-de"]; !r.Running || r.Current != "region-de-01" || r.LastOkMs < 1 {
		t.Fatalf("region state %+v", r)
	}
}

func TestXrayRegionDownWithoutFallbackKeepsItsPin(t *testing.T) {
	instance := startRegionCore(t)
	s := Settings{Regions: []RegionSettings{{BalancerTag: "region-tr", Order: []string{"region-tr-00"}, ProbeURL: probeServer(t, http.StatusNoContent)}}}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "region-tr down", func() bool { return CurrentState().Regions["region-tr"].Down })
	if got := overrideFor(t, instance, "region-tr"); got != "region-tr-00" {
		t.Fatalf("a down region with no fallback moved to %q", got)
	}
}

func TestXrayRegionFallsBackToAnOutbound(t *testing.T) {
	instance := startRegionCore(t)
	s := Settings{Regions: []RegionSettings{{BalancerTag: "region-tr", Order: []string{"region-tr-00"}, ProbeURL: probeServer(t, http.StatusNoContent), Fallback: "proxy"}}}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "region-tr on proxy", func() bool {
		return overrideFor(t, instance, "region-tr") == "proxy" && CurrentState().Regions["region-tr"].FallenBack
	})
}

func TestXrayRegionFallbackMainFollowsTheMainController(t *testing.T) {
	instance := startRegionCore(t)
	url := probeServer(t, http.StatusNoContent)
	s := Settings{
		BalancerTag: "smart", Order: []string{"proxy-00", "proxy-01"}, ProbeURL: url,
		Regions: []RegionSettings{{BalancerTag: "region-tr", Order: []string{"region-tr-00"}, ProbeURL: url, Fallback: FallbackMain}},
	}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "main on proxy-01", func() bool { return overrideFor(t, instance, "smart") == "proxy-01" })
	eventually(t, "region-tr following main", func() bool { return overrideFor(t, instance, "region-tr") == "proxy-01" })
}

func TestXrayRegionFallbackMainWithoutMainStaysDown(t *testing.T) {
	instance := startRegionCore(t)
	s := Settings{Regions: []RegionSettings{{BalancerTag: "region-tr", Order: []string{"region-tr-00"}, ProbeURL: probeServer(t, http.StatusNoContent), Fallback: FallbackMain}}}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "region-tr down", func() bool { return CurrentState().Regions["region-tr"].Down })
	time.Sleep(50 * time.Millisecond)
	if got, st := overrideFor(t, instance, "region-tr"), CurrentState().Regions["region-tr"]; got != "region-tr-00" || st.FallenBack {
		t.Fatalf("no main controller, yet the region moved to %q (%+v)", got, st)
	}
}

func TestXrayFailingRegionLeavesTheOthersRunning(t *testing.T) {
	instance := startRegionCore(t)
	url := probeServer(t, http.StatusNoContent)
	s := Settings{
		BalancerTag: "smart", Order: []string{"proxy-01"}, ProbeURL: url,
		Regions: []RegionSettings{
			{BalancerTag: "region-xx", Order: []string{"region-xx-00"}, ProbeURL: url},
			{BalancerTag: "region-de", Order: []string{"region-de-01"}, ProbeURL: url},
		},
	}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatalf("a failing region must not fail the main controller: %v", err)
	}
	st := CurrentState()
	if !st.Running || st.Error != "" {
		t.Fatalf("main %+v", st)
	}
	if r := st.Regions["region-xx"]; r.Running || r.Error == "" {
		t.Fatalf("unknown region %+v", r)
	}
	if r := st.Regions["region-de"]; !r.Running {
		t.Fatalf("region-de %+v", r)
	}
}

func TestXrayStopEndsEveryController(t *testing.T) {
	instance := startRegionCore(t)
	url := probeServer(t, http.StatusNoContent)
	s := Settings{
		BalancerTag: "smart", Order: []string{"proxy-01"}, ProbeURL: url,
		Regions: []RegionSettings{{BalancerTag: "region-de", Order: []string{"region-de-01"}, ProbeURL: url}},
	}
	if err := startWith(instance, s, fastTiming()); err != nil {
		t.Fatal(err)
	}
	Stop()
	if st := CurrentState(); st.Running || st.Regions != nil {
		t.Fatalf("after Stop: %+v", st)
	}
}

// The shape the app sends (cloakify-app's RegionFailover): regions alone,
// Smart's fields absent.
func TestSettingsDecodeRegionsAlone(t *testing.T) {
	var s Settings
	raw := `{"regions":[{"balancerTag":"region-de","order":["region-de-00","region-de-01"],"labels":{"region-de-00":"Germany"},"probeUrl":"http://p/","fallback":"main"}]}`
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatal(err)
	}
	if s.BalancerTag != "" || len(s.Regions) != 1 {
		t.Fatalf("%+v", s)
	}
	r := s.Regions[0]
	if r.BalancerTag != "region-de" || len(r.Order) != 2 || r.Labels["region-de-00"] != "Germany" || r.ProbeURL != "http://p/" || r.Fallback != FallbackMain {
		t.Fatalf("%+v", r)
	}
}

func TestSetOrderWithoutControllerFails(t *testing.T) {
	Stop()
	if err := SetOrder([]string{"proxy-00"}); err != ErrNotRunning {
		t.Fatalf("got %v", err)
	}
}
