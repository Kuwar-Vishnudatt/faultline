// Package loadgen drives a configurable, concurrent stream of requests at a
// target through the chaos proxy, recording per-request outcome and
// latency tagged with whichever experiment phase was active when the
// request was issued.
package loadgen

import (
	"sync"
	"sync/atomic"
	"time"
)

// Outcome classifies how a single operation ended. The Ambiguous category
// exists for the same reason it mattered in the raft-kv project's
// linearizability testing: a connection that fails *after* the request was
// sent is not provably a no-op on the server side (the request may have
// been received and acted on before the connection broke), so counting it
// as a plain "failure" would be a quietly false claim about what actually
// happened server-side. A caller's Op function is expected to distinguish
// this the same way raft-kv's test classified connection-refused (never
// sent) from connection-reset (sent, outcome unknown).
type Outcome int

const (
	Success Outcome = iota
	Failure
	Ambiguous
	// Skip means this attempt wasn't a meaningful unit of work (e.g. a
	// consumer polling an empty queue) and should not be recorded as a
	// sample at all — recording it as Success would understate latency,
	// and as Failure would wrongly tank the success rate on an idle queue.
	Skip
)

type Sample struct {
	Timestamp time.Time
	Phase     string
	Latency   time.Duration
	Outcome   Outcome
	Err       string
}

// Op is one unit of work a worker performs repeatedly. It returns the
// outcome of a single attempt; the generator times it and tags it with the
// phase active at the moment the attempt started.
type Op func() (Outcome, error)

type Generator struct {
	op          Op
	concurrency int
	interval    time.Duration // minimum spacing between a single worker's requests

	mu      sync.Mutex
	samples []Sample
	phase   atomic.Value // string
}

func New(op Op, concurrency int, interval time.Duration) *Generator {
	g := &Generator{op: op, concurrency: concurrency, interval: interval}
	g.phase.Store("(pre-experiment)")
	return g
}

// SetPhase tags all subsequently recorded samples with name, until changed
// again. Call this from the chaos.Runner's onPhaseStart callback.
func (g *Generator) SetPhase(name string) { g.phase.Store(name) }

// Run drives concurrency workers in a loop, each repeatedly calling op with
// at least interval between its own consecutive calls, until stop is
// closed. It blocks until all workers have exited.
func (g *Generator) Run(stop <-chan struct{}) {
	var wg sync.WaitGroup
	for i := 0; i < g.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.worker(stop)
		}()
	}
	wg.Wait()
}

func (g *Generator) worker(stop <-chan struct{}) {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		default:
		}

		phase, _ := g.phase.Load().(string)
		start := time.Now()
		outcome, err := g.op()
		latency := time.Since(start)

		if outcome != Skip {
			s := Sample{Timestamp: start, Phase: phase, Latency: latency, Outcome: outcome}
			if err != nil {
				s.Err = err.Error()
			}
			g.mu.Lock()
			g.samples = append(g.samples, s)
			g.mu.Unlock()
		}

		select {
		case <-stop:
			return
		case <-ticker.C:
		}
	}
}

// Samples returns a copy of every recorded sample so far. Safe to call
// concurrently with Run (e.g. for a live-updating dashboard), though this
// project only calls it after Run returns.
func (g *Generator) Samples() []Sample {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Sample, len(g.samples))
	copy(out, g.samples)
	return out
}
