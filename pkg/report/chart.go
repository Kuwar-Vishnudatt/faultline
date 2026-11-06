package report

import (
	"fmt"
	"strings"
	"time"
)

// RenderSVG renders a dependency-free grouped bar chart: one group of three
// bars (p50/p95/p99 latency) per phase, with the phase's success rate
// printed above its group. Hand-rolled rather than pulling in a charting
// library — the output is a handful of <rect>/<text> elements, which is all
// a grouped bar chart actually is, and it keeps this project's dependency
// footprint at zero.
func RenderSVG(summaries []PhaseSummary) string {
	const (
		width       = 900
		height      = 420
		marginLeft  = 70
		marginRight = 30
		marginTop   = 60
		marginBot   = 70
		barGroupGap = 40
	)
	plotW := width - marginLeft - marginRight
	plotH := height - marginTop - marginBot

	maxLatency := time.Duration(1)
	for _, s := range summaries {
		if s.P99 > maxLatency {
			maxLatency = s.P99
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %d %d" xmlns="http://www.w3.org/2000/svg" font-family="monospace">`, width, height)
	fmt.Fprintf(&b, `<rect x="0" y="0" width="%d" height="%d" fill="#ffffff"/>`, width, height)
	fmt.Fprintf(&b, `<text x="%d" y="24" font-size="18" fill="#111">Latency percentiles and success rate by experiment phase</text>`, marginLeft)

	// Y axis gridlines + labels, scaled to maxLatency.
	for i := 0; i <= 4; i++ {
		frac := float64(i) / 4
		y := marginTop + plotH - int(frac*float64(plotH))
		val := time.Duration(frac * float64(maxLatency))
		fmt.Fprintf(&b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#ddd" stroke-width="1"/>`, marginLeft, y, width-marginRight, y)
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="11" fill="#666" text-anchor="end">%s</text>`, marginLeft-8, y+4, formatMs(val))
	}
	fmt.Fprintf(&b, `<text x="20" y="%d" font-size="11" fill="#666" transform="rotate(-90 20 %d)">latency (ms)</text>`, marginTop+plotH/2, marginTop+plotH/2)

	if len(summaries) == 0 {
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="14" fill="#999">no data</text>`, marginLeft, marginTop+plotH/2)
		b.WriteString(`</svg>`)
		return b.String()
	}

	groupW := plotW / len(summaries)
	barW := (groupW - barGroupGap) / 3
	colors := map[string]string{"p50": "#4e79a7", "p95": "#f28e2b", "p99": "#e15759"}

	for i, s := range summaries {
		groupX := marginLeft + i*groupW + barGroupGap/2
		bars := []struct {
			label string
			val   time.Duration
		}{{"p50", s.P50}, {"p95", s.P95}, {"p99", s.P99}}

		for j, bar := range bars {
			frac := float64(bar.val) / float64(maxLatency)
			barH := int(frac * float64(plotH))
			x := groupX + j*barW
			y := marginTop + plotH - barH
			fmt.Fprintf(&b, `<rect x="%d" y="%d" width="%d" height="%d" fill="%s"/>`, x, y, barW-4, barH, colors[bar.label])
		}

		centerX := groupX + (barW*3)/2
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="12" fill="#111" text-anchor="middle">%s</text>`,
			centerX, marginTop+plotH+20, escapeXML(s.Phase))
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="11" fill="#333" text-anchor="middle">n=%d, success=%.0f%%</text>`,
			centerX, marginTop+plotH+36, s.Total, s.SuccessRate*100)
	}

	legendY := 40
	legendX := width - marginRight - 260
	for i, label := range []string{"p50", "p95", "p99"} {
		x := legendX + i*85
		fmt.Fprintf(&b, `<rect x="%d" y="%d" width="12" height="12" fill="%s"/>`, x, legendY-10, colors[label])
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="12" fill="#111">%s</text>`, x+16, legendY, label)
	}

	b.WriteString(`</svg>`)
	return b.String()
}

func formatMs(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}

func escapeXML(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}
