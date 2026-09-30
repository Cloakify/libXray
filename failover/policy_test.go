package failover

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

func testTiming() Timing {
	return Timing{
		ProbeInterval: 10 * time.Second, StartupInterval: 3 * time.Second, StartupWindow: 30 * time.Second,
		FailThreshold: 3, SearchBackoff: 30 * time.Second, DemoteFor: 10 * time.Minute,
		FreshEvery: 60 * time.Second, ProbeTimeout: 5 * time.Second, VerifyBatch: 3,
	}
}

func newTestPolicy(t *testing.T, order ...string) *Policy {
	t.Helper()
	p, err := NewPolicy(testTiming(), order, t0)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func unknown(string) Health { return HealthUnknown }

func TestNewPolicyRejectsEmptyOrder(t *testing.T) {
	if _, err := NewPolicy(testTiming(), nil, t0); !errors.Is(err, ErrEmptyOrder) {
		t.Fatalf("got %v, want ErrEmptyOrder", err)
	}
}

func TestStartsOnFirstTagAndDropsDuplicates(t *testing.T) {
	p := newTestPolicy(t, "a", "b", "a")
	if p.Current() != "a" {
		t.Fatalf("current = %q", p.Current())
	}
	if got := p.Candidates(t0, unknown); !reflect.DeepEqual(got, [][]string{{"b"}}) {
		t.Fatalf("candidates = %v", got)
	}
}

func TestThreeConsecutiveFailuresTriggerSearch(t *testing.T) {
	p := newTestPolicy(t, "a", "b")
	if p.OnProbe(t0, false) || p.OnProbe(t0.Add(10*time.Second), false) {
		t.Fatal("searched before the third failure")
	}
	if !p.OnProbe(t0.Add(20*time.Second), false) {
		t.Fatal("third consecutive failure did not search")
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	p := newTestPolicy(t, "a", "b")
	p.OnProbe(t0, false)
	p.OnProbe(t0.Add(10*time.Second), false)
	p.OnProbe(t0.Add(20*time.Second), true)
	if p.OnProbe(t0.Add(30*time.Second), false) || p.OnProbe(t0.Add(40*time.Second), false) {
		t.Fatal("a success did not reset the count")
	}
}

func TestSearchBackoff(t *testing.T) {
	p := newTestPolicy(t, "a", "b")
	for i := 0; i < 3; i++ {
		p.OnProbe(t0.Add(time.Duration(i)*10*time.Second), false)
	}
	// The search above found nothing; failures keep coming.
	if p.OnProbe(t0.Add(30*time.Second), false) {
		t.Fatal("searched again inside the backoff")
	}
	if !p.OnProbe(t0.Add(50*time.Second), false) {
		t.Fatal("did not search again after the backoff")
	}
}

func TestStartupCadence(t *testing.T) {
	p := newTestPolicy(t, "a")
	if got := p.NextProbeIn(t0.Add(29 * time.Second)); got != 3*time.Second {
		t.Fatalf("startup cadence = %v", got)
	}
	if got := p.NextProbeIn(t0.Add(30 * time.Second)); got != 10*time.Second {
		t.Fatalf("steady cadence = %v", got)
	}
}

func TestFreshEveryMinute(t *testing.T) {
	p := newTestPolicy(t, "a")
	if p.TakeFresh(t0.Add(59 * time.Second)) {
		t.Fatal("fresh before a minute")
	}
	if !p.TakeFresh(t0.Add(60 * time.Second)) {
		t.Fatal("no fresh after a minute")
	}
	if p.TakeFresh(t0.Add(61 * time.Second)) {
		t.Fatal("fresh turn not consumed")
	}
}

func TestCandidatesBatchedAndOrdered(t *testing.T) {
	p := newTestPolicy(t, "a", "b", "c", "d", "e")
	want := [][]string{{"b", "c", "d"}, {"e"}}
	if got := p.Candidates(t0, unknown); !reflect.DeepEqual(got, want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
}

func TestCandidatesPreferObservatoryAlive(t *testing.T) {
	p := newTestPolicy(t, "a", "b", "c", "d")
	health := func(tag string) Health {
		switch tag {
		case "b":
			return HealthDead
		case "d":
			return HealthAlive
		}
		return HealthUnknown
	}
	want := [][]string{{"d", "c", "b"}}
	if got := p.Candidates(t0, health); !reflect.DeepEqual(got, want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
}

func TestSwitchDemotesPreviousForTenMinutes(t *testing.T) {
	p := newTestPolicy(t, "a", "b", "c")
	if from := p.SwitchTo(t0, "b"); from != "a" {
		t.Fatalf("from = %q", from)
	}
	if got := p.Candidates(t0.Add(time.Minute), unknown); !reflect.DeepEqual(got, [][]string{{"c", "a"}}) {
		t.Fatalf("demoted candidates = %v", got)
	}
	if got := p.Candidates(t0.Add(11*time.Minute), unknown); !reflect.DeepEqual(got, [][]string{{"a", "c"}}) {
		t.Fatalf("after demotion = %v", got)
	}
}

func TestSwitchResetsFailures(t *testing.T) {
	p := newTestPolicy(t, "a", "b")
	for i := 0; i < 3; i++ {
		p.OnProbe(t0, false)
	}
	p.SwitchTo(t0, "b")
	if p.OnProbe(t0.Add(time.Second), false) || p.OnProbe(t0.Add(2*time.Second), false) {
		t.Fatal("failures carried over to the new server")
	}
}

func TestSetOrderKeepsCurrentAndAppendsMissing(t *testing.T) {
	p := newTestPolicy(t, "a", "b", "c")
	if err := p.SetOrder([]string{"c", "a"}); err != nil {
		t.Fatal(err)
	}
	if p.Current() != "a" {
		t.Fatalf("reorder moved current to %q", p.Current())
	}
	if got := p.Candidates(t0, unknown); !reflect.DeepEqual(got, [][]string{{"c", "b"}}) {
		t.Fatalf("candidates = %v", got)
	}
}

func TestSetOrderRejectsUnknownTag(t *testing.T) {
	p := newTestPolicy(t, "a", "b")
	if err := p.SetOrder([]string{"x"}); !errors.Is(err, ErrUnknownTag) {
		t.Fatalf("got %v, want ErrUnknownTag", err)
	}
}
