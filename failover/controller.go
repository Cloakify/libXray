package failover

import (
	"context"
	"sync"
	"time"
)

// Prober answers whether a server passes the health request through its own
// outbound. fresh asks for a new connection instead of a kept-alive one.
type Prober interface {
	Probe(ctx context.Context, tag string, fresh bool) bool
}

// Pinner moves the balancer override. Unpin hands selection back to the
// balancer's own strategy.
type Pinner interface {
	Pin(tag string) error
	Unpin()
}

type Reason string

const (
	ReasonUnreachable Reason = "unreachable" // current server died, pin moved
	ReasonNoFallback  Reason = "noFallback"  // current server died, nothing healthy to move to
)

type Switch struct {
	Seq       int    `json:"seq"`
	At        int64  `json:"at"` // unix milliseconds
	From      string `json:"from"`
	FromLabel string `json:"fromLabel,omitempty"`
	To        string `json:"to,omitempty"`
	ToLabel   string `json:"toLabel,omitempty"`
	Reason    Reason `json:"reason"`
}

type State struct {
	Running      bool    `json:"running"`
	Current      string  `json:"current,omitempty"`
	CurrentLabel string  `json:"currentLabel,omitempty"`
	LastSwitch   *Switch `json:"lastSwitch,omitempty"`
	Error        string  `json:"error,omitempty"`
}

type Controller struct {
	mu      sync.Mutex
	policy  *Policy
	labels  map[string]string
	prober  Prober
	pinner  Pinner
	health  func(string) Health
	now     func() time.Time
	cancel  context.CancelFunc
	done    chan struct{}
	running bool
	last    *Switch
	seq     int
	stuck   bool // a noFallback event was already recorded for this outage
}

func NewController(t Timing, order []string, labels map[string]string, prober Prober, pinner Pinner, health func(string) Health) (*Controller, error) {
	p, err := NewPolicy(t, order, time.Now())
	if err != nil {
		return nil, err
	}
	if health == nil {
		health = func(string) Health { return HealthUnknown }
	}
	return &Controller{policy: p, labels: labels, prober: prober, pinner: pinner, health: health, now: time.Now}, nil
}

// Start pins the first server and begins probing it.
func (c *Controller) Start() error {
	if err := c.pinner.Pin(c.policy.Current()); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.cancel, c.done, c.running = cancel, make(chan struct{}), true
	c.mu.Unlock()
	go c.loop(ctx)
	return nil
}

// Stop ends the loop and waits for it, in-flight probes included.
func (c *Controller) Stop() {
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

func (c *Controller) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.policy.Current()
	st := State{Running: c.running, Current: cur, CurrentLabel: c.labels[cur]}
	if c.last != nil {
		sw := *c.last
		st.LastSwitch = &sw
	}
	return st
}

func (c *Controller) SetOrder(order []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.policy.SetOrder(order)
}

func (c *Controller) loop(ctx context.Context) {
	defer close(c.done)
	defer func() {
		// Never take the tunnel down with us: hand selection back to the
		// balancer's own strategy and stop.
		if recover() != nil {
			c.pinner.Unpin()
		}
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()
	for {
		c.mu.Lock()
		wait := c.policy.NextProbeIn(c.now())
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		c.mu.Lock()
		tag, fresh := c.policy.Current(), c.policy.TakeFresh(c.now())
		c.mu.Unlock()
		ok := c.prober.Probe(ctx, tag, fresh)
		if ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		search := c.policy.OnProbe(c.now(), ok)
		if ok {
			c.stuck = false
		}
		confirmations := c.policy.Confirmations()
		var first []string
		if confirmations > 0 && c.policy.CanSearch(c.now()) {
			if batches := c.policy.Candidates(c.now(), c.health); len(batches) > 0 {
				first = batches[0]
			}
		}
		c.mu.Unlock()
		var verified map[string]bool
		if confirmations > 0 {
			search, verified = c.confirm(ctx, tag, confirmations, first)
			if ctx.Err() != nil {
				return
			}
		}
		if search {
			c.search(ctx, verified)
		}
	}
}

// confirm decides whether the failure just seen is an outage: n more probes
// of the current server at once, all fresh, and the outage declared when every
// one fails. The first fallbacks are verified alongside, so a confirmed outage
// switches without waiting for them — the whole decision takes one probe's
// time. If the network itself is down they fail too, and nothing switches.
// Returns whether to search, and the fallbacks' results by tag.
func (c *Controller) confirm(ctx context.Context, tag string, n int, first []string) (bool, map[string]bool) {
	confirmed := make([]bool, n)
	results := make([]bool, len(first))
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			confirmed[i] = c.prober.Probe(ctx, tag, true)
		}()
	}
	for i, fallback := range first {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = c.prober.Probe(ctx, fallback, true)
		}()
	}
	wg.Wait()
	verified := make(map[string]bool, len(first))
	for i, fallback := range first {
		verified[fallback] = results[i]
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ok := range confirmed {
		if ok {
			c.policy.OnProbe(c.now(), true)
			c.stuck = false
			return false, nil
		}
	}
	search := false
	for range confirmed {
		search = c.policy.OnProbe(c.now(), false)
	}
	return search, verified
}

// search verifies fallbacks batch by batch and pins the best one that passes.
// verified holds fallbacks already probed for this outage (by [confirm]):
// their result stands, and they are not probed again.
func (c *Controller) search(ctx context.Context, verified map[string]bool) {
	c.mu.Lock()
	batches := c.policy.Candidates(c.now(), c.health)
	c.mu.Unlock()
	for _, batch := range batches {
		results := make([]bool, len(batch))
		var wg sync.WaitGroup
		for i, tag := range batch {
			if ok, seen := verified[tag]; seen {
				results[i] = ok
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = c.prober.Probe(ctx, tag, true)
			}()
		}
		wg.Wait()
		if ctx.Err() != nil {
			return
		}
		for i, ok := range results {
			if !ok || c.pinner.Pin(batch[i]) != nil {
				continue
			}
			c.mu.Lock()
			from := c.policy.SwitchTo(c.now(), batch[i])
			c.record(from, batch[i], ReasonUnreachable)
			c.stuck = false
			c.mu.Unlock()
			return
		}
	}
	c.mu.Lock()
	if !c.stuck {
		c.record(c.policy.Current(), "", ReasonNoFallback)
		c.stuck = true
	}
	c.mu.Unlock()
}

// record needs c.mu held.
func (c *Controller) record(from, to string, reason Reason) {
	c.seq++
	c.last = &Switch{
		Seq: c.seq, At: c.now().UnixMilli(), From: from, FromLabel: c.labels[from],
		To: to, ToLabel: c.labels[to], Reason: reason,
	}
}
