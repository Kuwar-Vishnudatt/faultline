package report

import (
	"strings"
	"testing"
	"time"
)

func TestRenderSVGProducesValidLookingOutputWithPhaseLabels(t *testing.T) {
	summaries := []PhaseSummary{
		{Phase: "baseline", Total: 100, Successes: 100, SuccessRate: 1.0, P50: 10 * time.Millisecond, P95: 20 * time.Millisecond, P99: 30 * time.Millisecond},
		{Phase: "chaos", Total: 100, Successes: 40, SuccessRate: 0.4, P50: 50 * time.Millisecond, P95: 400 * time.Millisecond, P99: 900 * time.Millisecond},
	}
	svg := RenderSVG(summaries)

	if !strings.HasPrefix(svg, "<svg") || !strings.HasSuffix(svg, "</svg>") {
		t.Fatalf("expected well-formed SVG wrapper, got prefix/suffix of: %.80s...", svg)
	}
	for _, want := range []string{"baseline", "chaos", "p50", "p95", "p99"} {
		if !strings.Contains(svg, want) {
			t.Errorf("expected chart to mention %q", want)
		}
	}
}

func TestRenderSVGHandlesEmptyInputGracefully(t *testing.T) {
	svg := RenderSVG(nil)
	if !strings.Contains(svg, "<svg") || !strings.Contains(svg, "</svg>") {
		t.Fatalf("expected a valid empty-state SVG, got %s", svg)
	}
}

func TestRenderSVGEscapesPhaseNames(t *testing.T) {
	summaries := []PhaseSummary{{Phase: "<script>alert(1)</script>", Total: 1}}
	svg := RenderSVG(summaries)
	if strings.Contains(svg, "<script>") {
		t.Fatalf("expected phase name to be XML-escaped, got raw tag in output")
	}
}

func TestRenderMarkdownIncludesTableAndNotes(t *testing.T) {
	summaries := []PhaseSummary{
		{Phase: "baseline", Total: 10, SuccessRate: 1.0, P50: 5 * time.Millisecond, P95: 8 * time.Millisecond, P99: 9 * time.Millisecond},
	}
	md := RenderMarkdown("Test Experiment", summaries, "all jobs reconciled successfully", "chart.svg")

	for _, want := range []string{"# Test Experiment", "baseline", "chart.svg", "all jobs reconciled successfully"} {
		if !strings.Contains(md, want) {
			t.Errorf("expected markdown to contain %q", want)
		}
	}
}
