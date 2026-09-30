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
type Settings struct {
	BalancerTag string            `json:"balancerTag"`
	Order       []string          `json:"order"`
	Labels      map[string]string `json:"labels,omitempty"`
	ProbeURL    string            `json:"probeUrl"`
}

var ErrNotRunning = errors.New("failover: not running")

var (
	mu       sync.Mutex
	active   *Controller
	prober   *xrayProber
	startErr string
)

// Start attaches a controller to a running core, replacing any previous one.
// On error the core keeps running on its balancer's own strategy, and the
// reason is reported by CurrentState.
func Start(instance *core.Instance, s Settings) error { return startWith(instance, s, DefaultTiming) }

func startWith(instance *core.Instance, s Settings, t Timing) error {
	Stop()
	c, p, err := build(instance, s, t)
	if err == nil {
		if err = c.Start(); err != nil {
			p.close()
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		startErr = err.Error()
		return err
	}
	active, prober, startErr = c, p, ""
	return nil
}

// Stop ends the controller. Called before the core closes.
func Stop() {
	mu.Lock()
	c, p := active, prober
	active, prober, startErr = nil, nil, ""
	mu.Unlock()
	if c != nil {
		c.Stop()
	}
	if p != nil {
		p.close()
	}
}

func CurrentState() State {
	mu.Lock()
	c, e := active, startErr
	mu.Unlock()
	if c == nil {
		return State{Error: e}
	}
	return c.State()
}

func SetOrder(order []string) error {
	mu.Lock()
	c := active
	mu.Unlock()
	if c == nil {
		return ErrNotRunning
	}
	return c.SetOrder(order)
}

func build(instance *core.Instance, s Settings, t Timing) (*Controller, *xrayProber, error) {
	if instance == nil {
		return nil, nil, errors.New("failover: core is not running")
	}
	if s.BalancerTag == "" || s.ProbeURL == "" {
		return nil, nil, errors.New("failover: balancerTag and probeUrl are required")
	}
	om, _ := instance.GetFeature(outbound.ManagerType()).(outbound.Manager)
	overrider, _ := instance.GetFeature(routing.RouterType()).(routing.BalancerOverrider)
	dispatcher, _ := instance.GetFeature(routing.DispatcherType()).(routing.Dispatcher)
	if om == nil || overrider == nil || dispatcher == nil {
		return nil, nil, errors.New("failover: core has no outbound manager, router or dispatcher")
	}
	for _, tag := range s.Order {
		if om.GetHandler(tag) == nil {
			return nil, nil, fmt.Errorf("%w: %q", ErrUnknownTag, tag)
		}
	}
	// tagged.Dialer refuses a context without the instance. core.XrayKey is
	// exported for exactly this lookup; xray's own observatory probes the
	// same way from inside the core.
	ctx := context.WithValue(context.Background(), core.XrayKey(1), instance)
	p := &xrayProber{ctx: ctx, dispatcher: dispatcher, url: s.ProbeURL, timeout: t.ProbeTimeout, kept: map[string]*http.Client{}}
	c, err := NewController(t, s.Order, s.Labels, p, routerPinner{overrider, s.BalancerTag}, observatoryHealth(ctx, instance))
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
