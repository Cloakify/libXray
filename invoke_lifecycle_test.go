package libXray

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xtls/libxray/failover"
	"github.com/xtls/libxray/xray"
)

// A balancer over a dead (blackhole) and a live (freedom) outbound, as the
// failover package's own tests use.
const lifecycleTestConfig = `{
  "log": {"loglevel": "none"},
  "outbounds": [
    {"tag": "proxy-00", "protocol": "blackhole"},
    {"tag": "proxy-01", "protocol": "freedom"}
  ],
  "routing": {"balancers": [{"tag": "smart", "selector": ["proxy-"], "strategy": {"type": "random"}}]}
}`

func failoverSettingsForTest(t *testing.T) json.RawMessage {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	raw, err := json.Marshal(failover.Settings{
		BalancerTag: "smart",
		Order:       []string{"proxy-00", "proxy-01"},
		ProbeURL:    srv.URL + "/generate_204",
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func failoverRunningForTest(t *testing.T) bool {
	t.Helper()
	response := invokeForTest(t, LibXrayMethodGetFailoverState, nil)
	if !response.Success {
		t.Fatalf("getFailoverState failed: %s", response.Err)
	}
	var state failover.State
	if err := json.Unmarshal(response.Data, &state); err != nil {
		t.Fatal(err)
	}
	return state.Running
}

func TestInvokeRunXrayStartsFailover(t *testing.T) {
	response := invokeForTest(t, LibXrayMethodRunXray, RunXrayRequest{
		XrayJson: lifecycleTestConfig,
		Failover: failoverSettingsForTest(t),
	})
	defer xrayStopForTest(t)
	if !response.Success {
		t.Fatalf("runXray failed: %s", response.Err)
	}
	if !failoverRunningForTest(t) {
		t.Fatal("runXray with failover settings left no controller running")
	}
}

func TestInvokeRefusedDuplicateRunXrayKeepsFailover(t *testing.T) {
	settings := failoverSettingsForTest(t)
	first := invokeForTest(t, LibXrayMethodRunXray, RunXrayRequest{XrayJson: lifecycleTestConfig, Failover: settings})
	defer xrayStopForTest(t)
	if !first.Success {
		t.Fatalf("runXray failed: %s", first.Err)
	}
	duplicate := invokeForTest(t, LibXrayMethodRunXray, RunXrayRequest{XrayJson: lifecycleTestConfig})
	if duplicate.Success || duplicate.Err != xray.ErrAlreadyRunning.Error() {
		t.Fatalf("duplicate runXray: success=%v error=%q", duplicate.Success, duplicate.Err)
	}
	if !failoverRunningForTest(t) {
		t.Fatal("a refused duplicate runXray stopped the running tunnel's failover")
	}
}

// Two starts and a stop at once, in any order: afterwards the controller runs
// exactly when the core does. Without the lifecycle lock a stop can land
// between RunXray and failover.Start and leave a controller on a closed core.
func TestInvokeConcurrentRunAndStopKeepFailoverWithTheCore(t *testing.T) {
	payload, err := json.Marshal(RunXrayRequest{XrayJson: lifecycleTestConfig, Failover: failoverSettingsForTest(t)})
	if err != nil {
		t.Fatal(err)
	}
	runJSON, err := json.Marshal(&LibXrayInvokeRequest{APIVersion: LibXrayAPIVersion, Method: LibXrayMethodRunXray, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	stopJSON, err := json.Marshal(&LibXrayInvokeRequest{APIVersion: LibXrayAPIVersion, Method: LibXrayMethodStopXray})
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 10; round++ {
		var wg sync.WaitGroup
		for _, request := range []string{string(runJSON), string(runJSON), string(stopJSON)} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				Invoke(request)
			}()
		}
		wg.Wait()
		if core, controller := xray.GetXrayState(), failoverRunningForTest(t); core != controller {
			t.Fatalf("round %d: core running=%v, failover running=%v", round, core, controller)
		}
		xrayStopForTest(t)
	}
}

// The app sends its asset directory in the config's root env (Invoke's own
// env is gone). It must be applied before the routing rules load geodata —
// proven with a geosite rule that only the env can resolve.
func TestInvokeRunXrayConfigEnvLocatesGeodata(t *testing.T) {
	const assetKey = "xray.location.asset"
	t.Setenv(assetKey, "")
	datDir := t.TempDir()
	writeGeoSiteDatForTest(t, filepath.Join(datDir, "geosite.dat"))
	config := func(env map[string]string) string {
		t.Helper()
		raw, err := json.Marshal(map[string]any{
			"env":       env,
			"log":       map[string]string{"loglevel": "none"},
			"outbounds": []any{map[string]string{"tag": "direct", "protocol": "freedom"}},
			"routing": map[string]any{"rules": []any{
				map[string]any{"domain": []string{"geosite:test"}, "outboundTag": "direct"},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}

	without := invokeForTest(t, LibXrayMethodRunXray, RunXrayRequest{XrayJson: config(nil)})
	if without.Success {
		xrayStopForTest(t)
		t.Fatal("a geosite rule loaded with no asset location: the test proves nothing")
	}
	with := invokeForTest(t, LibXrayMethodRunXray, RunXrayRequest{XrayJson: config(map[string]string{assetKey: datDir})})
	defer xrayStopForTest(t)
	if !with.Success {
		t.Fatalf("runXray with the asset location in the config's env failed: %s", with.Err)
	}
	if got := os.Getenv(assetKey); got != datDir {
		t.Fatalf("%s = %q, want %q", assetKey, got, datDir)
	}
}
