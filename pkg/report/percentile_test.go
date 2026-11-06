package report

import (
	"testing"
	"time"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestPercentileOnKnownDataset(t *testing.T) {
	// 1..100 ms, so p50 should land near 50ms and p99 near the top.
	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = ms(i + 1)
	}

	if p50 := Percentile(samples, 50); p50 < ms(45) || p50 > ms(55) {
		t.Fatalf("expected p50 near 50ms, got %v", p50)
	}
	if p99 := Percentile(samples, 99); p99 < ms(95) {
		t.Fatalf("expected p99 near the top of the range, got %v", p99)
	}
	if p0 := Percentile(samples, 0); p0 != ms(1) {
		t.Fatalf("expected p0 to be the minimum, got %v", p0)
	}
	if p100 := Percentile(samples, 100); p100 != ms(100) {
		t.Fatalf("expected p100 to be the maximum, got %v", p100)
	}
}

func TestPercentileEmptyInput(t *testing.T) {
	if p := Percentile(nil, 50); p != 0 {
		t.Fatalf("expected 0 for empty input, got %v", p)
	}
}

func TestPercentileUnsortedInputDoesNotMutateCaller(t *testing.T) {
	samples := []time.Duration{ms(5), ms(1), ms(3), ms(2), ms(4)}
	orig := append([]time.Duration(nil), samples...)
	Percentile(samples, 50)
	for i := range samples {
		if samples[i] != orig[i] {
			t.Fatalf("Percentile must not mutate its input slice, got %v want %v", samples, orig)
		}
	}
}

func TestPercentileSingleSample(t *testing.T) {
	if p := Percentile([]time.Duration{ms(42)}, 99); p != ms(42) {
		t.Fatalf("expected the single sample for any percentile, got %v", p)
	}
}
