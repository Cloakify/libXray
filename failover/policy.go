// Package failover keeps Smart mode on the server it is connected to until
// that server is actually down, then moves to the next healthy one: sticky
// failover driven through xray's balancer override. Design:
// cloakify-app/docs/superpowers/specs/2026-09-30-smart-sticky-failover-design.md
package failover

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// Timing holds every clock-facing knob. Production uses DefaultTiming; tests
// shrink it. It lives here rather than in the request so the app and the
// Android service cannot disagree about it.
type Timing struct {
	ProbeInterval   time.Duration // gap between probes of the current server
	StartupInterval time.Duration // gap during the first StartupWindow
	StartupWindow   time.Duration
	FailThreshold   int           // failures in a row that make an outage: the one seen, then the rest confirmed at once
	SearchBackoff   time.Duration // minimum gap between two fallback searches
	DemoteFor       time.Duration // how long a server that just failed sinks to the end
	FreshEvery      time.Duration // how often a probe must open a new connection
	ProbeTimeout    time.Duration
	VerifyBatch     int // fallbacks verified in parallel per round
}

// DefaultTiming leaves a dead server within ~10 s: a probe every 3 s, each
// given 3 s, and once one fails the two confirming it and the first fallbacks
// are probed together — 3 + 3 + 3. A kept-alive connection that outlives a
// server refusing new ones is caught by the fresh probe every 15 s instead.
var DefaultTiming = Timing{
	ProbeInterval:   3 * time.Second,
	StartupInterval: 3 * time.Second,
	StartupWindow:   30 * time.Second,
	FailThreshold:   3,
	SearchBackoff:   30 * time.Second,
	DemoteFor:       10 * time.Minute,
	FreshEvery:      15 * time.Second,
	ProbeTimeout:    3 * time.Second,
	VerifyBatch:     3,
}

// Health is the observatory's last word on a fallback. It only orders the
// candidates; a verification probe decides.
type Health int

const (
	HealthUnknown Health = iota
	HealthAlive
	HealthDead
)

var (
	ErrEmptyOrder = errors.New("failover: empty order")
	ErrUnknownTag = errors.New("failover: unknown tag")
)

// Policy is the decision logic with no I/O: the controller feeds it probe
// results and the time, and it answers when to search and whom to try.
// Not safe for concurrent use — the controller serializes access.
type Policy struct {
	t          Timing
	order      []string
	known      map[string]bool
	current    string
	started    time.Time
	fails      int
	lastSearch time.Time
	lastFresh  time.Time
	demoted    map[string]time.Time
}

func NewPolicy(t Timing, order []string, now time.Time) (*Policy, error) {
	known := make(map[string]bool, len(order))
	dedup := make([]string, 0, len(order))
	for _, tag := range order {
		if tag == "" || known[tag] {
			continue
		}
		known[tag] = true
		dedup = append(dedup, tag)
	}
	if len(dedup) == 0 {
		return nil, ErrEmptyOrder
	}
	if t.VerifyBatch < 1 {
		t.VerifyBatch = 1
	}
	return &Policy{
		t: t, order: dedup, known: known, current: dedup[0],
		started: now, lastFresh: now, demoted: map[string]time.Time{},
	}, nil
}

func (p *Policy) Current() string { return p.current }

// NextProbeIn is the wait before the next probe. The first StartupWindow may
// be probed at its own pace. A failure does not wait for the next probe: the
// controller confirms it at once ([Policy.Confirmations]).
func (p *Policy) NextProbeIn(now time.Time) time.Duration {
	if now.Sub(p.started) < p.t.StartupWindow {
		return p.t.StartupInterval
	}
	return p.t.ProbeInterval
}

// Confirmations is how many probes of the current server must now run at
// once to decide whether the failure just seen is an outage: the rest of
// FailThreshold, or none when there is no failure to confirm or the outage
// is already declared (a search that found nothing).
func (p *Policy) Confirmations() int {
	if p.fails > 0 && p.fails < p.t.FailThreshold {
		return p.t.FailThreshold - p.fails
	}
	return 0
}

// CanSearch reports whether a fallback search would run now — none inside
// SearchBackoff of the last. The controller verifies fallbacks alongside a
// confirmation only then.
func (p *Policy) CanSearch(now time.Time) bool {
	return p.lastSearch.IsZero() || now.Sub(p.lastSearch) >= p.t.SearchBackoff
}

// TakeFresh reports whether this probe must use a new connection, and
// consumes the turn. A kept-alive connection can outlive a server that no
// longer accepts new ones — which is what the user's traffic needs.
//
// Once a probe has failed, every probe is fresh until one succeeds: a
// kept-alive success in between would reset the count, and a server that
// blocks new handshakes (a firewall, a filter keyed on the handshake) would
// never reach FailThreshold while no new connection of the user's got through.
func (p *Policy) TakeFresh(now time.Time) bool {
	if p.fails > 0 {
		p.lastFresh = now
		return true
	}
	if now.Sub(p.lastFresh) < p.t.FreshEvery {
		return false
	}
	p.lastFresh = now
	return true
}

// OnProbe records one probe of the current server and reports whether a
// fallback search should run now: FailThreshold failures in a row, and no
// search inside the last SearchBackoff.
func (p *Policy) OnProbe(now time.Time, ok bool) bool {
	if ok {
		p.fails = 0
		return false
	}
	p.fails++
	if p.fails < p.t.FailThreshold || !p.CanSearch(now) {
		return false
	}
	p.lastSearch = now
	return true
}

// Candidates lists the fallbacks to verify, in batches of VerifyBatch, best
// first: observatory-alive, unknown, dead; within a group, the order; a
// server demoted within DemoteFor after all of them.
func (p *Policy) Candidates(now time.Time, health func(string) Health) [][]string {
	type cand struct {
		tag       string
		rank, idx int
	}
	var cs []cand
	for i, tag := range p.order {
		if tag == p.current {
			continue
		}
		rank := 1
		switch health(tag) {
		case HealthAlive:
			rank = 0
		case HealthDead:
			rank = 2
		}
		if until, ok := p.demoted[tag]; ok {
			if now.Before(until) {
				rank = 3
			} else {
				delete(p.demoted, tag)
			}
		}
		cs = append(cs, cand{tag, rank, i})
	}
	sort.SliceStable(cs, func(a, b int) bool {
		if cs[a].rank != cs[b].rank {
			return cs[a].rank < cs[b].rank
		}
		return cs[a].idx < cs[b].idx
	})
	var batches [][]string
	for i := 0; i < len(cs); i += p.t.VerifyBatch {
		end := min(i+p.t.VerifyBatch, len(cs))
		batch := make([]string, 0, end-i)
		for _, c := range cs[i:end] {
			batch = append(batch, c.tag)
		}
		batches = append(batches, batch)
	}
	return batches
}

// SwitchTo makes tag current. The server it replaces is demoted, so two
// flaky servers cannot ping-pong.
func (p *Policy) SwitchTo(now time.Time, tag string) (from string) {
	from = p.current
	p.demoted[from] = now.Add(p.t.DemoteFor)
	p.current = tag
	p.fails = 0
	p.lastSearch = time.Time{}
	p.lastFresh = now // the verification that chose it was a fresh connection
	return from
}

// SetOrder reorders the fallbacks from live pings. It never moves the current
// server; tags the caller leaves out keep their place at the end.
func (p *Policy) SetOrder(order []string) error {
	seen := make(map[string]bool, len(order))
	next := make([]string, 0, len(p.order))
	for _, tag := range order {
		if !p.known[tag] {
			return fmt.Errorf("%w: %q", ErrUnknownTag, tag)
		}
		if seen[tag] {
			continue
		}
		seen[tag] = true
		next = append(next, tag)
	}
	for _, tag := range p.order {
		if !seen[tag] {
			next = append(next, tag)
		}
	}
	p.order = next
	return nil
}
