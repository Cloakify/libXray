package probe

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var fastTiming = Timing{WarmUp: 400 * time.Millisecond, Shot: 150 * time.Millisecond}

// reply is what the scripted server does with one request.
type reply func(w *bufio.Writer) (closeAfter bool)

func status204(delay time.Duration) reply {
	return func(w *bufio.Writer) bool {
		time.Sleep(delay)
		w.WriteString("HTTP/1.1 204 No Content\r\n\r\n")
		return false
	}
}

func withBody(status int, body string) reply {
	return func(w *bufio.Writer) bool {
		time.Sleep(5 * time.Millisecond)
		fmt.Fprintf(w, "HTTP/1.1 %d X\r\nContent-Length: %d\r\n\r\n%s", status, len(body), body)
		return false
	}
}

func chunked(status int) reply {
	return func(w *bufio.Writer) bool {
		time.Sleep(5 * time.Millisecond)
		fmt.Fprintf(w, "HTTP/1.1 %d X\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\n\r\n", status)
		return false
	}
}

func hang() reply {
	return func(*bufio.Writer) bool { time.Sleep(2 * time.Second); return true }
}

type scripted struct {
	addr     string
	conns    atomic.Int32
	requests atomic.Int32
}

// serve answers the n-th request on a connection with script[n]; the last
// entry repeats.
func serve(t *testing.T, script ...reply) *scripted {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &scripted{addr: ln.Addr().String()}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.conns.Add(1)
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				w := bufio.NewWriter(c)
				for n := 0; ; n++ {
					if _, err := http.ReadRequest(r); err != nil {
						return
					}
					s.requests.Add(1)
					closeAfter := script[min(n, len(script)-1)](w)
					w.Flush()
					if closeAfter {
						return
					}
				}
			}(c)
		}
	}()
	return s
}

func tcpDial(addr string) Dial {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

func targetFor(t *testing.T, s *scripted) target {
	t.Helper()
	tg, err := parseTarget("http://" + s.addr + "/generate_204")
	if err != nil {
		t.Fatal(err)
	}
	return tg
}

func TestMeasureReportsTheFasterShotOverOneConnection(t *testing.T) {
	s := serve(t, status204(30*time.Millisecond), status204(40*time.Millisecond), status204(10*time.Millisecond))
	r := measure(context.Background(), tcpDial(s.addr), targetFor(t, s), fastTiming)
	if r.Failed || r.Ms < 10 || r.Ms >= 40 {
		t.Fatalf("got %+v", r)
	}
	if s.conns.Load() != 1 || s.requests.Load() != 3 {
		t.Fatalf("conns %d requests %d, want 1 and 3", s.conns.Load(), s.requests.Load())
	}
}

func TestMeasureFailsWhenNothingAnswers(t *testing.T) {
	s := serve(t, hang())
	if r := measure(context.Background(), tcpDial(s.addr), targetFor(t, s), fastTiming); !r.Failed {
		t.Fatalf("got %+v", r)
	}
}

func TestMeasureFailsWhenTheDialFails(t *testing.T) {
	dial := func(context.Context) (net.Conn, error) { return nil, fmt.Errorf("refused") }
	tg, _ := parseTarget("http://example.com/generate_204")
	if r := measure(context.Background(), dial, tg, fastTiming); !r.Failed {
		t.Fatalf("got %+v", r)
	}
}

func TestMeasureFallsBackToTheWarmUpWhenTheServerClosesAfterIt(t *testing.T) {
	s := serve(t, func(w *bufio.Writer) bool { status204(20 * time.Millisecond)(w); return true })
	r := measure(context.Background(), tcpDial(s.addr), targetFor(t, s), fastTiming)
	if r.Failed || r.Ms < 20 {
		t.Fatalf("got %+v", r)
	}
	if s.conns.Load() != 1 {
		t.Fatalf("re-dialled: %d connections", s.conns.Load())
	}
}

func TestMeasureATimedOutShotBreaksTheSession(t *testing.T) {
	s := serve(t, status204(10*time.Millisecond), hang())
	r := measure(context.Background(), tcpDial(s.addr), targetFor(t, s), fastTiming)
	if r.Failed || r.Ms < 10 {
		t.Fatalf("got %+v (want the warm-up time)", r)
	}
	if s.requests.Load() != 2 {
		t.Fatalf("server saw %d requests, want 2", s.requests.Load())
	}
}

func TestMeasureNon204WithALengthKeepsTheSession(t *testing.T) {
	s := serve(t, status204(10*time.Millisecond), withBody(http.StatusOK, "blocked"), status204(15*time.Millisecond))
	r := measure(context.Background(), tcpDial(s.addr), targetFor(t, s), fastTiming)
	if r.Failed || r.Ms < 15 || s.requests.Load() != 3 {
		t.Fatalf("got %+v after %d requests", r, s.requests.Load())
	}
}

func TestMeasureChunkedNon204SpendsTheConnection(t *testing.T) {
	s := serve(t, status204(10*time.Millisecond), chunked(http.StatusOK), status204(15*time.Millisecond))
	measure(context.Background(), tcpDial(s.addr), targetFor(t, s), fastTiming)
	if s.requests.Load() != 2 {
		t.Fatalf("server saw %d requests, want 2", s.requests.Load())
	}
}

func TestMeasureABlockPageIsNotAReading(t *testing.T) {
	s := serve(t, withBody(http.StatusOK, "blocked"))
	if r := measure(context.Background(), tcpDial(s.addr), targetFor(t, s), fastTiming); !r.Failed {
		t.Fatalf("got %+v", r)
	}
}

func TestMeasureStopsWhenCancelled(t *testing.T) {
	s := serve(t, hang())
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	began := time.Now()
	measure(ctx, tcpDial(s.addr), targetFor(t, s), Timing{WarmUp: 5 * time.Second, Shot: 5 * time.Second})
	if took := time.Since(began); took > time.Second {
		t.Fatalf("cancel took %v", took)
	}
}

func TestParseTargetKeepsTheHostByName(t *testing.T) {
	tg, err := parseTarget("http://cp.cloudflare.com/generate_204")
	if err != nil {
		t.Fatal(err)
	}
	if !tg.dest.Address.Family().IsDomain() || tg.dest.Address.Domain() != "cp.cloudflare.com" || tg.dest.Port != 80 {
		t.Fatalf("dest %v", tg.dest)
	}
	if !strings.HasPrefix(tg.request, "GET /generate_204 HTTP/1.1\r\nHost: cp.cloudflare.com\r\n") {
		t.Fatalf("request %q", tg.request)
	}
	other, err := parseTarget("http://example.com:8080/x?y=1")
	if err != nil || !strings.HasPrefix(other.request, "GET /x?y=1 HTTP/1.1\r\nHost: example.com:8080\r\n") {
		t.Fatalf("request %q (%v)", other.request, err)
	}
	if _, err := parseTarget("https://cp.cloudflare.com/"); err == nil {
		t.Fatal("https accepted")
	}
}
