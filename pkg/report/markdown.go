package report

import (
	"fmt"
	"strings"
)

// RenderMarkdown produces a human-readable SLO report: a summary table plus
// any free-form notes (e.g. correctness-reconciliation results) the caller
// wants to attach.
func RenderMarkdown(title string, summaries []PhaseSummary, notes string, chartFilename string) string {
	var b strings.Builder
	if title != "" {
		fmt.Fprintf(&b, "# %s\n\n", title)
	}
	if chartFilename != "" {
		fmt.Fprintf(&b, "![SLO chart](%s)\n\n", chartFilename)
	}

	b.WriteString("| Phase | Requests | Success Rate | p50 | p95 | p99 | Failures | Ambiguous |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, s := range summaries {
		fmt.Fprintf(&b, "| %s | %d | %.1f%% | %s | %s | %s | %d | %d |\n",
			s.Phase, s.Total, s.SuccessRate*100, formatMs(s.P50), formatMs(s.P95), formatMs(s.P99), s.Failures, s.Ambiguous)
	}

	if notes != "" {
		b.WriteString("\n## Notes\n\n")
		b.WriteString(notes)
		b.WriteString("\n")
	}
	return b.String()
}
