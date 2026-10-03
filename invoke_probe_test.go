package libXray

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInvokeProbeRoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	t.Cleanup(func() { Invoke(`{"apiVersion":1,"method":"closeProbeHost"}`) })

	start := Invoke(`{"apiVersion":1,"method":"startProbe","payload":{"url":"` + srv.URL +
		`/generate_204","servers":[{"id":"a","outbound":{"protocol":"freedom"}}],"measure":["a"]}}`)
	if !strings.HasPrefix(start, `{"success":true`) || !strings.Contains(start, `"deadlineMs":`) {
		t.Fatalf("start: %s", start)
	}
	var all string
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		r := Invoke(`{"apiVersion":1,"method":"probeResults"}`)
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
		if r := Invoke(`{"apiVersion":1,"method":"` + m + `"}`); !strings.HasPrefix(r, `{"success":true`) {
			t.Fatalf("%s: %s", m, r)
		}
	}
	if r := Invoke(`{"apiVersion":1,"method":"startProbe","payload":{"url":"https://x/"}}`); !strings.HasPrefix(r, `{"success":false`) {
		t.Fatalf("a bad url was accepted: %s", r)
	}
}
