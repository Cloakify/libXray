package failover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
	_ "github.com/xtls/xray-core/transport/internet/tagged/taggedimpl"
)

// Settings arrive with runXray. Timing is not among them — see Timing.
//
// The main fields are Smart's balancer, all or none: absent, no main
// controller runs and none is reported missing. Regions are the per-app
// countries of split tunnelling, each a balancer of its own
// (cloakify-app/docs/superpowers/specs/2026-11-25-per-app-regions-design.md).
type Settings struct {
	BalancerTag string            `json:"balancerTag,omitempty"`
	Order       []string          `json:"order,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	ProbeURL    string            `json:"probeUrl,omitempty"`
	Regions     []RegionSettings  `json:"regions,omitempty"`
}

// RegionSettings is one country's controller. Fallback says where its
// traffic goes while every one of its servers is down: "" nowhere (it stays
// pinned to the dead server, so nothing leaves — the default), "main" the
// main controller's current server, any other value that outbound.
type RegionSettings struct {
	BalancerTag string            `json:"balancerTag"`
	Order       []string          `json:"order"`
	Labels      map[string]string `json:"labels,omitempty"`
	ProbeURL    string            `json:"probeUrl"`
	Fallback    string            `json:"fallback,omitempty"`
}

// FallbackMain is RegionSettings.Fallback's "the main controller's current
// server".
const FallbackMain = "main"

var ErrNotRunning = errors.New("failover: not running")

// run is one controller and its prober — or why it did not start.
type run struct {
	c   *Controller
	p   *xrayProber
	err string
}

func (r run) stop() {
	if r.c != nil {
		r.c.Stop()
	}
	if r.p != nil {
		r.p.close()
	}
}

func (r run) state() State {
	if r.c == nil {
		return State{Error: r.err}
	}
	return r.c.State()
}

var (
	mu      sync.Mutex
	primary run            // Smart's balancer, when the session has one
	regions map[string]run // by balancer tag
)

// Start attaches the controllers to a running core, replacing any previous
// ones. On error a balancer keeps running on its own strategy, and the
// reason is reported by CurrentState. The returned error is the main
// controller's; a region that cannot start leaves the others running.
func Start(instance *core.Instance, s Settings) error { return startWith(instance, s, DefaultTiming) }

func startWith(instance *core.Instance, s Settings, t Timing) error {
	Stop()
	var mainErr error
	var m run
	if s.BalancerTag != "" || len(s.Order) > 0 {
		m, mainErr = startOne(instance, s.BalancerTag, s.Order, s.Labels, s.ProbeURL, t, nil)
	}
	rs := make(map[string]run, len(s.Regions))
	for _, r := range s.Regions {
		// One controller per balancer: a second one would pin against the
		// first, and the first, no longer in the map, would never be stopped.
		if _, dup := rs[r.BalancerTag]; dup {
			continue
		}
		var fallback func() string
		switch r.Fallback {
		case "":
		case FallbackMain:
			fallback = mainCurrent
		default:
			target := r.Fallback
			fallback = func() string { return target }
		}
		rs[r.BalancerTag], _ = startOne(instance, r.BalancerTag, r.Order, r.Labels, r.ProbeURL, t, fallback)
	}
	mu.Lock()
	defer mu.Unlock()
	primary, regions = m, rs
	return mainErr
}

func startOne(instance *core.Instance, balancerTag string, order []string, labels map[string]string, probeURL string, t Timing, fallback func() string) (run, error) {
	c, p, err := build(instance, balancerTag, order, labels, probeURL, t)
	if err == nil {
		c.SetFallback(fallback)
		if err = c.Start(); err != nil {
			p.close()
		}
	}
	if err != nil {
		return run{err: err.Error()}, err
	}
	return run{c: c, p: p}, nil
}

// mainCurrent is the main controller's current server, or "" when none is
// running — a region falling back to "main" then has nowhere to go.
func mainCurrent() string {
	mu.Lock()
	c := primary.c
	mu.Unlock()
	if c == nil {
		return ""
	}
	if st := c.State(); st.Running {
		return st.Current
	}
	return ""
}

// Stop ends every controller. Called before the core closes.
func Stop() {
	mu.Lock()
	m, rs := primary, regions
	primary, regions = run{}, nil
	mu.Unlock()
	m.stop()
	for _, r := range rs {
		r.stop()
	}
}

// CurrentState is the main controller's state (empty when there is none),
// with every region's under Regions. No controller's lock is taken while mu
// is held: a region's fallback takes mu, then the main controller's.
func CurrentState() State {
	mu.Lock()
	m := primary
	rs := make(map[string]run, len(regions))
	for tag, r := range regions {
		rs[tag] = r
	}
	mu.Unlock()
	st := m.state()
	if len(rs) > 0 {
		st.Regions = make(map[string]State, len(rs))
		for tag, r := range rs {
			st.Regions[tag] = r.state()
		}
	}
	return st
}

// SetOrder reorders the main controller's fallbacks (regions are ordered at
// start).
func SetOrder(order []string) error {
	mu.Lock()
	c := primary.c
	mu.Unlock()
	if c == nil {
		return ErrNotRunning
	}
	return c.SetOrder(order)
}

func build(instance *core.Instance, balancerTag string, order []string, labels map[string]string, probeURL string, t Timing) (*Controller, *xrayProber, error) {
	if instance == nil {
		return nil, nil, errors.New("failover: core is not running")
	}
	if balancerTag == "" || probeURL == "" {
		return nil, nil, errors.New("failover: balancerTag and probeUrl are required")
	}
	om, _ := instance.GetFeature(outbound.ManagerType()).(outbound.Manager)
	overrider, _ := instance.GetFeature(routing.RouterType()).(routing.BalancerOverrider)
	dispatcher, _ := instance.GetFeature(routing.DispatcherType()).(routing.Dispatcher)
	if om == nil || overrider == nil || dispatcher == nil {
		return nil, nil, errors.New("failover: core has no outbound manager, router or dispatcher")
	}
	for _, tag := range order {
		if om.GetHandler(tag) == nil {
			return nil, nil, fmt.Errorf("%w: %q", ErrUnknownTag, tag)
		}
	}
	// tagged.Dialer refuses a context without the instance. core.XrayKey is
	// exported for exactly this lookup; xray's own observatory probes the
	// same way from inside the core.
	ctx := context.WithValue(context.Background(), core.XrayKey(1), instance)
	p := &xrayProber{ctx: ctx, dispatcher: dispatcher, url: probeURL, timeout: t.ProbeTimeout, kept: map[string]*http.Client{}}
	c, err := NewController(t, order, labels, p, routerPinner{overrider, balancerTag}, observatoryHealth(ctx, instance))
	if err != nil {
		return nil, nil, err
	}
	return c, p, nil
}

type routerPinner struct {
	r        routing.BalancerOverrider
	balancer string
}

func (p routerPinner) Pin(tag string) error { return p.r.SetOverrideTarget(p.balancer, tag) }
func (p routerPinner) Unpin()               { _ = p.r.SetOverrideTarget(p.balancer, "") }

// xrayProber sends the health request through one outbound. Success is
// exactly 204: a server that answers with a block page is down for the user.
type xrayProber struct {
	ctx        context.Context
	dispatcher routing.Dispatcher
	url        string
	timeout    time.Duration
	mu         sync.Mutex
	kept       map[string]*http.Client
}

func (p *xrayProber) Probe(ctx context.Context, tag string, fresh bool) bool {
	client := p.keptClient(tag)
	if fresh {
		client = p.newClient(tag, true)
		defer client.CloseIdleConnections()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return resp.StatusCode == http.StatusNoContent
}

func (p *xrayProber) keptClient(tag string) *http.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.kept[tag]
	if !ok {
		c = p.newClient(tag, false)
		p.kept[tag] = c
	}
	return c
}

func (p *xrayProber) newClient(tag string, fresh bool) *http.Client {
	tr := &http.Transport{
		DisableKeepAlives:   fresh,
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     90 * time.Second,
		DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			dest, err := xnet.ParseDestination(network + ":" + addr)
			if err != nil {
				return nil, err
			}
			return tagged.Dialer(p.ctx, p.dispatcher, dest, tag)
		},
	}
	return &http.Client{
		Transport:     tr,
		Timeout:       p.timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func (p *xrayProber) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.kept {
		c.CloseIdleConnections()
	}
}

// observatoryHealth reads the burst observatory's verdicts, which only order
// the candidates. nil (HealthUnknown for all) when the config has none.
func observatoryHealth(ctx context.Context, instance *core.Instance) func(string) Health {
	obs, _ := instance.GetFeature(extension.ObservatoryType()).(extension.Observatory)
	if obs == nil {
		return nil
	}
	return func(tag string) Health {
		msg, err := obs.GetObservation(ctx)
		if err != nil {
			return HealthUnknown
		}
		res, ok := msg.(*observatory.ObservationResult)
		if !ok {
			return HealthUnknown
		}
		for _, s := range res.Status {
			if s.OutboundTag == tag {
				if s.Alive {
					return HealthAlive
				}
				return HealthDead
			}
		}
		return HealthUnknown
	}
}
