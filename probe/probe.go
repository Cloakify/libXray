package probe

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/xtls/xray-core/transport/internet/tagged"
)

// Concurrency is how many servers are measured at once. It matches the Smart
// pool's size, so a sweep never opens more sockets than a connect would.
const Concurrency = 8

// timing is a variable only so tests can shrink it.
var timing = DefaultTiming

type StartRequest struct {
	URL     string   `json:"url"`
	Servers []Server `json:"servers"`
	Measure []string `json:"measure"`
}

type StartResponse struct {
	// Dropped: ids that could not be held, or are not held — never measured,
	// which is not the same as failed.
	Dropped []string `json:"dropped"`
	// DeadlineMs is the longest the run can take, so the app has a ceiling
	// without copying the timing.
	DeadlineMs int64 `json:"deadlineMs"`
}

type Reading struct {
	ID     string `json:"id"`
	Ms     int64  `json:"ms,omitempty"`
	Failed bool   `json:"failed,omitempty"`
}

type ResultsResponse struct {
	Results []Reading `json:"results"`
	Done    bool      `json:"done"`
}

// run is one sweep. Its goroutines only ever write into their own run, so a
// replaced run cannot leak readings into the next.
type run struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	pending []Reading
	left    int
}

func (r *run) record(id string, res Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending = append(r.pending, Reading{ID: id, Ms: res.Ms, Failed: res.Failed})
	r.left--
	if r.left == 0 {
		r.cancel()
	}
}

// Package state. Never touches the tunnel's core or its failover controller.
var (
	mu      sync.Mutex
	current *host
	active  *run
)

// Start syncs the held servers to req.Servers, then measures req.Measure in
// the background. A run already going is stopped first.
func Start(req StartRequest) (StartResponse, error) {
	StopSession()
	tg, err := parseTarget(req.URL)
	if err != nil {
		return StartResponse{}, err
	}
	mu.Lock()
	defer mu.Unlock()
	if current == nil {
		h, err := newHost()
		if err != nil {
			return StartResponse{}, err
		}
		current = h
	}
	h := current
	dropped := h.sync(req.Servers)
	unheld := map[string]bool{}
	for _, id := range dropped {
		unheld[id] = true
	}
	type job struct{ id, tag string }
	var jobs []job
	seen := map[string]bool{}
	for _, id := range req.Measure {
		if seen[id] {
			continue
		}
		seen[id] = true
		if held, ok := h.held[id]; ok {
			jobs = append(jobs, job{id, held.tag})
		} else if !unheld[id] {
			dropped = append(dropped, id)
		}
	}
	if dropped == nil {
		dropped = []string{}
	}

	ctx, cancel := context.WithCancel(h.ctx)
	r := &run{cancel: cancel, left: len(jobs)}
	if len(jobs) == 0 {
		cancel()
	}
	active = r
	slots := make(chan struct{}, Concurrency)
	for _, j := range jobs {
		r.wg.Add(1)
		go func(id, tag string) {
			defer r.wg.Done()
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-slots }()
			r.record(id, measureOne(ctx, h, tg, tag))
		}(j.id, j.tag)
	}
	waves := (len(jobs) + Concurrency - 1) / Concurrency
	return StartResponse{Dropped: dropped, DeadlineMs: (time.Duration(waves) * timing.Bound()).Milliseconds()}, nil
}

// measureOne gives each server a link context of its own, torn down when it
// is done, and turns a panic in our own code into a failed reading.
func measureOne(ctx context.Context, h *host, tg target, tag string) (res Result) {
	defer func() {
		if recover() != nil {
			res = Result{Failed: true}
		}
	}()
	link, cancel := context.WithCancel(ctx)
	defer cancel()
	// The dial context is the link's, never a shot's: cancelling a request
	// context would tear the link down under the shots that follow.
	dial := func(c context.Context) (net.Conn, error) { return tagged.Dialer(c, h.dispatcher, tg.dest, tag) }
	return measure(link, dial, tg, timing)
}

// Results returns the readings landed since the previous call.
func Results() ResultsResponse {
	mu.Lock()
	r := active
	mu.Unlock()
	if r == nil {
		return ResultsResponse{Results: []Reading{}, Done: true}
	}
	r.mu.Lock()
	out := r.pending
	r.pending = nil
	done := r.left == 0
	r.mu.Unlock()
	if out == nil {
		out = []Reading{}
	}
	if done {
		mu.Lock()
		if active == r {
			active = nil
		}
		mu.Unlock()
	}
	return ResultsResponse{Results: out, Done: done}
}

// StopSession cancels the running sweep and waits for its workers. The host
// stays. Idempotent.
func StopSession() {
	mu.Lock()
	r := active
	active = nil
	mu.Unlock()
	if r != nil {
		r.cancel()
		r.wg.Wait()
	}
}

// CloseHost stops the sweep and closes the probe instance.
func CloseHost() {
	StopSession()
	mu.Lock()
	h := current
	current = nil
	mu.Unlock()
	if h != nil {
		h.close()
	}
}

// PointDialerAtHost keeps the process-global dialer on a live instance once
// the tunnel's core has closed.
func PointDialerAtHost() {
	mu.Lock()
	defer mu.Unlock()
	if current != nil {
		pointDialerAt(current.instance)
	}
}

// heldTags is for tests: id → tag.
func heldTags() map[string]string {
	mu.Lock()
	defer mu.Unlock()
	out := map[string]string{}
	if current != nil {
		for id, s := range current.held {
			out[id] = s.tag
		}
	}
	return out
}
