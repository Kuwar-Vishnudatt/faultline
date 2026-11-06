package report

import (
	"testing"
	"time"

	"github.com/Kuwar-Vishnudatt/faultline/pkg/loadgen"
)

func TestSummarizeGroupsByPhaseInChronologicalOrder(t *testing.T) {
	samples := []loadgen.Sample{
		{Phase: "baseline", Outcome: loadgen.Success, Latency: 10 * time.Millisecond},
		{Phase: "baseline", Outcome: loadgen.Success, Latency: 12 * time.Millisecond},
		{Phase: "chaos", Outcome: loadgen.Failure, Latency: 500 * time.Millisecond},
		{Phase: "chaos", Outcome: loadgen.Success, Latency: 300 * time.Millisecond},
		{Phase: "recovery", Outcome: loadgen.Success, Latency: 11 * time.Millisecond},
	}
	summaries := Summarize(samples)
	if len(summaries) != 3 {
		t.Fatalf("expected 3 phase summaries, got %d", len(summaries))
	}
	order := []string{summaries[0].Phase, summaries[1].Phase, summaries[2].Phase}
	want := []string{"baseline", "chaos", "recovery"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("expected phase order %v, got %v", want, order)
		}
	}

	chaos := summaries[1]
	if chaos.Total != 2 || chaos.Successes != 1 || chaos.Failures != 1 {
		t.Fatalf("unexpected chaos phase summary: %+v", chaos)
	}
	if chaos.SuccessRate != 0.5 {
		t.Fatalf("expected 50%% success rate, got %v", chaos.SuccessRate)
	}
}

func TestSummarizeCountsAmbiguousSeparately(t *testing.T) {
	samples := []loadgen.Sample{
		{Phase: "p", Outcome: loadgen.Ambiguous, Latency: time.Millisecond},
		{Phase: "p", Outcome: loadgen.Success, Latency: time.Millisecond},
	}
	s := Summarize(samples)[0]
	if s.Ambiguous != 1 {
		t.Fatalf("expected 1 ambiguous sample tracked separately, got %+v", s)
	}
	// Ambiguous must not be silently counted as success.
	if s.SuccessRate != 0.5 {
		t.Fatalf("expected success rate to exclude the ambiguous sample from the numerator, got %v", s.SuccessRate)
	}
}
