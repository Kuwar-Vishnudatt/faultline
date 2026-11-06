package chaos

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Kuwar-Vishnudatt/faultline/pkg/proxy"
)

func newTestProxy(t *testing.T) *proxy.Proxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream := l.Addr().String()
	l.Close()

	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listenAddr := l2.Addr().String()
	l2.Close()

	p := proxy.New(listenAddr, upstream)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestScenarioAppliesPhasesInOrder(t *testing.T) {
	p := newTestProxy(t)
	r := &Runner{
		Proxy: p,
		Phases: []Phase{
			{Name: "baseline", Duration: 20 * time.Millisecond, Faults: proxy.Faults{}},
			{Name: "lossy", Duration: 20 * time.Millisecond, Faults: proxy.Faults{DropProbability: 1.0}},
			{Name: "recovered", Duration: 20 * time.Millisecond, Faults: proxy.Faults{}},
		},
	}

	var seen []string
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r.Run(ctx, func(name string) { seen = append(seen, name) })

	want := []string{"baseline", "lossy", "recovered"}
	if len(seen) != len(want) {
		t.Fatalf("expected phases %v, got %v", want, seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("expected phase order %v, got %v", want, seen)
		}
	}
}

func TestScenarioKillsAndRestartsTargetMidPhase(t *testing.T) {
	p := newTestProxy(t)
	sup := NewSupervisor("sleeper", "/bin/sleep", "30")
	if err := sup.Start(); err != nil {
		t.Fatal(err)
	}
	defer sup.Kill()

	r := &Runner{
		Proxy:      p,
		Supervisor: sup,
		Phases: []Phase{
			{
				Name: "kill-test", Duration: 150 * time.Millisecond,
				KillAt: 30 * time.Millisecond, RestartAfter: 30 * time.Millisecond,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r.Run(ctx, nil)

	if !sup.Running() {
		t.Fatalf("expected the target to have been restarted and be running again after the phase")
	}
}
