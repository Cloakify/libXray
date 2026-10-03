package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xtls/libxray/xray"
	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/transport/internet"
	_ "github.com/xtls/xray-core/transport/internet/tagged/taggedimpl"
)

// Server is one server the app wants held: its id and one xray outbound.
type Server struct {
	ID       string          `json:"id"`
	Outbound json.RawMessage `json:"outbound"`
}

type heldServer struct {
	tag      string
	outbound string // compacted JSON, for change detection
}

// host is the probe's own xray instance, kept for the app's life. Rebuilding
// it per sweep is not an option: the gRPC and xhttp transports cache clients
// in process-global maps keyed by a handler's stream settings and never prune
// them, so fresh handlers every sweep would grow memory without bound (and a
// cached gRPC client keeps reconnecting forever). Reused handlers keep those
// keys stable.
type host struct {
	instance   *core.Instance
	dispatcher routing.Dispatcher
	manager    outbound.Manager
	ctx        context.Context // carries the instance, as tagged.Dialer requires
	held       map[string]heldServer
	next       int
}

func newHost() (*host, error) {
	// Built by hand, not through conf.Config.Build: that always prepends a
	// log app, and the log app registers itself as the PROCESS-global log
	// handler inside core.New — it would take over the tunnel's logging and
	// leave it dead once closed. core.New adds the DNS client, policy, router
	// and stats defaults the dispatcher needs; tagged.Dialer's forced
	// outbound tag bypasses the router.
	instance, err := core.New(&core.Config{App: []*serial.TypedMessage{
		serial.ToTypedMessage(&dispatcher.Config{}),
		serial.ToTypedMessage(&proxyman.OutboundConfig{}),
	}})
	// core.New pointed the process-global dialer (the DNS behind
	// domainStrategy, the dialerProxy lookups) at the new instance. Hand it
	// back to the tunnel at once, error or not.
	if t := xray.Instance(); t != nil && t.IsRunning() {
		pointDialerAt(t)
	}
	if err != nil {
		return nil, err
	}
	if err := instance.Start(); err != nil {
		instance.Close()
		return nil, err
	}
	d, _ := instance.GetFeature(routing.DispatcherType()).(routing.Dispatcher)
	m, _ := instance.GetFeature(outbound.ManagerType()).(outbound.Manager)
	if d == nil || m == nil {
		instance.Close()
		return nil, errors.New("probe: instance has no dispatcher or outbound manager")
	}
	return &host{
		instance:   instance,
		dispatcher: d,
		manager:    m,
		ctx:        context.WithValue(context.Background(), core.XrayKey(1), instance),
		held:       map[string]heldServer{},
	}, nil
}

func pointDialerAt(instance *core.Instance) {
	dc, _ := instance.GetFeature(dns.ClientType()).(dns.Client)
	om, _ := instance.GetFeature(outbound.ManagerType()).(outbound.Manager)
	if dc != nil {
		internet.InitSystemDialer(dc, om)
	}
}

// sync makes the held set exactly servers: unchanged ones keep their handler
// (and so their transport cache keys), changed and gone ones are removed, new
// ones are added one by one so a bad one costs only itself. Returns the ids
// that could not be held.
func (h *host) sync(servers []Server) (dropped []string) {
	want := map[string]string{}
	for _, s := range servers {
		if _, dup := want[s.ID]; dup || s.ID == "" {
			continue
		}
		var buf bytes.Buffer
		if err := json.Compact(&buf, s.Outbound); err != nil {
			dropped = append(dropped, s.ID)
			continue
		}
		want[s.ID] = buf.String()
	}
	for id, have := range h.held {
		if out, ok := want[id]; !ok || out != have.outbound {
			h.remove(id)
		}
	}
	for _, s := range servers {
		out, ok := want[s.ID]
		if !ok {
			continue
		}
		delete(want, s.ID) // a repeated id later in the list is ignored
		if _, ok := h.held[s.ID]; ok {
			continue
		}
		if err := h.add(s.ID, out); err != nil {
			dropped = append(dropped, s.ID)
		}
	}
	return dropped
}

func (h *host) add(id, outboundJSON string) error {
	var c conf.OutboundDetourConfig
	if err := json.Unmarshal([]byte(outboundJSON), &c); err != nil {
		return err
	}
	// Every app outbound arrives tagged "proxy", and the manager refuses a
	// repeated tag, so the tags are ours.
	c.Tag = fmt.Sprintf("probe-%d", h.next)
	h.next++
	hc, err := c.Build()
	if err != nil {
		return err
	}
	if err := core.AddOutboundHandler(h.instance, hc); err != nil {
		return err
	}
	h.held[id] = heldServer{tag: c.Tag, outbound: outboundJSON}
	return nil
}

func (h *host) remove(id string) {
	tag := h.held[id].tag
	delete(h.held, id)
	handler := h.manager.GetHandler(tag)
	// RemoveHandler only forgets the handler; closing it is ours.
	_ = h.manager.RemoveHandler(context.Background(), tag)
	if handler != nil {
		_ = common.Close(handler)
	}
}

func (h *host) close() { _ = h.instance.Close() }
