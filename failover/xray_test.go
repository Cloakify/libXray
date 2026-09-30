package failover

import (
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

func TestSetOrderWithoutControllerFails(t *testing.T) {
	Stop()
	if err := SetOrder([]string{"proxy-00"}); err != ErrNotRunning {
		t.Fatalf("got %v", err)
	}
}
