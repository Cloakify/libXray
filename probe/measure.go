// Package probe measures server latency inside libXray, on an xray instance
// that is not the tunnel's core, so a sweep needs no second process.
//
// The measurement is the app's old Dart one, unchanged: a warm-up request
// whose time is discarded, then two requests on the SAME connection, 204
// required, the minimum reported.
package probe

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
)

// Timing bounds one server's measurement.
type Timing struct {
	// WarmUp pays the tunnel and endpoint handshakes; its time is discarded.
	// Far looser than a shot: a distant server can spend seconds here while
	// answering promptly once the connection stands.
	WarmUp time.Duration
	// Shot rides the connection the warm-up opened.
	Shot time.Duration
}

var DefaultTiming = Timing{WarmUp: 12 * time.Second, Shot: 3 * time.Second}

// measuredShots: one shot jittered to 750 ms between clean ~104 ms readings in
// a live run; the minimum of two stops one hiccup from colouring a server.
const measuredShots = 2

// maxDrain bounds the non-204 body read that keeps the stream on a boundary.
const maxDrain = 64 << 10

// Bound is the longest one server can take.
func (t Timing) Bound() time.Duration { return t.WarmUp + measuredShots*t.Shot }

// Result is a reading (Ms > 0) or Failed.
type Result struct {
	Ms     int64
	Failed bool
}

// Dial opens the one connection a measurement uses.
type Dial func(ctx context.Context) (net.Conn, error)

type target struct {
	dest    xnet.Destination
	request string
}

// parseTarget turns the probe URL into the destination — by NAME, so the
// server under test resolves it rather than this machine — and the request
// every shot writes.
func parseTarget(raw string) (target, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return target{}, err
	}
	if u.Scheme != "http" || u.Hostname() == "" {
		return target{}, errors.New("probe: url must be plain http with a host")
	}
	port := 80
	if p := u.Port(); p != "" {
		if port, err = strconv.Atoi(p); err != nil {
			return target{}, err
		}
	}
	host := u.Hostname()
	if port != 80 {
		host = net.JoinHostPort(host, strconv.Itoa(port))
	}
	return target{
		dest: xnet.TCPDestination(xnet.ParseAddress(u.Hostname()), xnet.Port(port)),
		// Minimal, and redirects are never followed: the status must be the
		// probed server's own answer, not the end of a chain a captive portal
		// started.
		request: "GET " + u.RequestURI() + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: keep-alive\r\n\r\n",
	}, nil
}

// measure: warm-up, then two shots on the same connection; the minimum
// reading, else the warm-up's own time, else failed.
func measure(ctx context.Context, dial Dial, tg target, t Timing) Result {
	s := &session{ctx: ctx, dial: dial, tg: tg}
	defer s.spend()
	warm := s.shot(t.WarmUp)
	var best int64
	for i := 0; i < measuredShots; i++ {
		if ms := s.shot(t.Shot); ms > 0 && (best == 0 || ms < best) {
			best = ms
		}
	}
	if best > 0 {
		return Result{Ms: best}
	}
	// Reachable but too slow to measure cleanly: an upper bound, so the
	// server shows up slow rather than dead.
	if warm > 0 {
		return Result{Ms: warm}
	}
	return Result{Failed: true}
}

// session is the one connection every shot rides. No http.Transport: it may
// silently re-dial or retry, and a shot would then measure a fresh handshake
// instead of failing. Once broken a session reports nothing more — a shot that
// failed mid-response leaves bytes behind, and the next one would be
// "answered" by them.
type session struct {
	ctx  context.Context
	dial Dial
	tg   target

	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
	broken bool
}

// shot returns the round trip in ms, or 0 when it gave nothing usable. xray's
// connections ignore deadlines, so the budget is enforced by closing the
// connection, which also breaks the session, as a timed-out shot must.
func (s *session) shot(budget time.Duration) int64 {
	if s.isBroken() {
		return 0
	}
	done := make(chan int64, 1)
	go func() { done <- s.exchange() }()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case ms := <-done:
		return ms
	case <-timer.C:
	case <-s.ctx.Done():
	}
	s.spend()
	return 0
}

func (s *session) isBroken() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.broken
}

// spend breaks the session and closes its connection. Idempotent.
func (s *session) spend() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.broken = true
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *session) open() (net.Conn, *bufio.Reader, error) {
	s.mu.Lock()
	if s.broken {
		s.mu.Unlock()
		return nil, nil, errors.New("probe: session spent")
	}
	if s.conn != nil {
		defer s.mu.Unlock()
		return s.conn, s.reader, nil
	}
	s.mu.Unlock()
	conn, err := s.dial(s.ctx)
	if err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken { // the shot that dialled already gave up
		conn.Close()
		return nil, nil, errors.New("probe: session spent")
	}
	s.conn, s.reader = conn, bufio.NewReader(conn)
	return s.conn, s.reader, nil
}

func (s *session) exchange() int64 {
	conn, reader, err := s.open()
	if err != nil {
		s.spend()
		return 0
	}
	if _, err := io.WriteString(conn, s.tg.request); err != nil {
		s.spend()
		return 0
	}
	began := time.Now()
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		s.spend()
		return 0
	}
	elapsed := time.Since(began).Milliseconds()
	if resp.StatusCode == http.StatusNoContent {
		// A 204 has no body: the header block ends the response, and the
		// stream already sits on the next shot's boundary.
		return elapsed
	}
	// A filtered config whose ISP injects a block page answers fast; that is
	// not a reading. Keep the connection only if where the body ends is
	// knowable and small.
	if resp.ContentLength < 0 || resp.ContentLength > maxDrain {
		s.spend()
		return 0
	}
	if _, err := io.CopyN(io.Discard, resp.Body, resp.ContentLength); err != nil {
		s.spend()
	}
	return 0
}
