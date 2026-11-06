package report

import (
	"time"

	"github.com/Kuwar-Vishnudatt/faultline/pkg/loadgen"
)

// PhaseSummary is the SLO rollup for one experiment phase: what a reader
// actually wants to compare before/after — latency percentiles and success
// rate — not the raw sample dump.
type PhaseSummary struct {
	Phase       string
	Total       int
	Successes   int
	Failures    int
	Ambiguous   int
	SuccessRate float64
	P50         time.Duration
	P95         time.Duration
	P99         time.Duration
}

// Summarize groups samples by phase (preserving first-seen order, since
// that's the chronological experiment order) and computes SLO metrics per
// phase.
func Summarize(samples []loadgen.Sample) []PhaseSummary {
	order := []string{}
	byPhase := map[string][]loadgen.Sample{}
	for _, s := range samples {
		if _, ok := byPhase[s.Phase]; !ok {
			order = append(order, s.Phase)
		}
		byPhase[s.Phase] = append(byPhase[s.Phase], s)
	}

	out := make([]PhaseSummary, 0, len(order))
	for _, phase := range order {
		ps := byPhase[phase]
		sum := PhaseSummary{Phase: phase, Total: len(ps)}
		latencies := make([]time.Duration, 0, len(ps))
		for _, s := range ps {
			switch s.Outcome {
			case loadgen.Success:
				sum.Successes++
			case loadgen.Failure:
				sum.Failures++
			case loadgen.Ambiguous:
				sum.Ambiguous++
			}
			latencies = append(latencies, s.Latency)
		}
		if sum.Total > 0 {
			sum.SuccessRate = float64(sum.Successes) / float64(sum.Total)
		}
		sum.P50 = Percentile(latencies, 50)
		sum.P95 = Percentile(latencies, 95)
		sum.P99 = Percentile(latencies, 99)
		out = append(out, sum)
	}
	return out
}
