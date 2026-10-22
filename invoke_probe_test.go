package libXray

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// probeRequest is an Invoke request in the current API version, written out as
// the app sends it.
func probeRequest(method, payload string) string {
	request := `{"apiVersion":` + strconv.Itoa(LibXrayAPIVersion) + `,"method":"` + method + `"`
	if payload != "" {
		request += `,"payload":` + payload
	}
	return request + `}`
}

func TestInvokeProbeRoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	t.Cleanup(func() { Invoke(probeRequest("closeProbeHost", "")) })

	start := Invoke(probeRequest("startProbe", `{"url":"`+srv.URL+
		`/generate_204","servers":[{"id":"a","outbound":{"protocol":"freedom"}}],"measure":["a"]}`))
	if !strings.HasPrefix(start, `{"success":true`) || !strings.Contains(start, `"deadlineMs":`) {
		t.Fatalf("start: %s", start)
	}
	var all string
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		r := Invoke(probeRequest("probeResults", ""))
		all += r
		if strings.Contains(r, `"done":true`) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(all, `{"id":"a","ms":`) {
		t.Fatalf("no reading for a: %s", all)
	}
	for _, m := range []string{"stopProbe", "closeProbeHost"} {
		if r := Invoke(probeRequest(m, "")); !strings.HasPrefix(r, `{"success":true`) {
			t.Fatalf("%s: %s", m, r)
		}
	}
	if r := Invoke(probeRequest("startProbe", `{"url":"https://x/"}`)); !strings.HasPrefix(r, `{"success":false`) {
		t.Fatalf("a bad url was accepted: %s", r)
	}
}
