package loadgen

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestGeneratorRecordsSamplesFromAllWorkers(t *testing.T) {
	var calls atomic.Int64
	op := func() (Outcome, error) {
		calls.Add(1)
		return Success, nil
	}
	g := New(op, 4, time.Millisecond)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { g.Run(stop); close(done) }()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	<-done

	samples := g.Samples()
	if len(samples) == 0 {
		t.Fatal("expected some samples to have been recorded")
	}
	if int64(len(samples)) != calls.Load() {
		t.Fatalf("expected one sample per op call: %d samples vs %d calls", len(samples), calls.Load())
	}
	for _, s := range samples {
		if s.Outcome != Success {
			t.Fatalf("expected all recorded outcomes to be Success, got %v", s.Outcome)
		}
	}
}

func TestGeneratorTagsSamplesWithCurrentPhase(t *testing.T) {
	op := func() (Outcome, error) { return Success, nil }
	g := New(op, 1, time.Millisecond)

	stop := make(chan struct{})
	done := make(chan struct{})
	g.SetPhase("before")
	go func() { g.Run(stop); close(done) }()

	time.Sleep(20 * time.Millisecond)
	g.SetPhase("after")
	time.Sleep(20 * time.Millisecond)
	close(stop)
	<-done

	var sawBefore, sawAfter bool
	for _, s := range g.Samples() {
		if s.Phase == "before" {
			sawBefore = true
		}
		if s.Phase == "after" {
			sawAfter = true
		}
	}
	if !sawBefore || !sawAfter {
		t.Fatalf("expected samples tagged with both phases, sawBefore=%v sawAfter=%v", sawBefore, sawAfter)
	}
}

func TestGeneratorRecordsErrorMessageOnFailure(t *testing.T) {
	op := func() (Outcome, error) { return Failure, errStub{} }
	g := New(op, 1, time.Millisecond)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { g.Run(stop); close(done) }()
	time.Sleep(20 * time.Millisecond)
	close(stop)
	<-done

	samples := g.Samples()
	if len(samples) == 0 {
		t.Fatal("expected at least one sample")
	}
	for _, s := range samples {
		if s.Outcome != Failure || s.Err == "" {
			t.Fatalf("expected a Failure outcome with a recorded error message, got %+v", s)
		}
	}
}

type errStub struct{}

func (errStub) Error() string { return "stub failure" }

func TestGeneratorDoesNotRecordSkippedAttempts(t *testing.T) {
	var calls atomic.Int64
	op := func() (Outcome, error) {
		n := calls.Add(1)
		if n%2 == 0 {
			return Skip, nil
		}
		return Success, nil
	}
	g := New(op, 1, time.Millisecond)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { g.Run(stop); close(done) }()
	time.Sleep(30 * time.Millisecond)
	close(stop)
	<-done

	samples := g.Samples()
	if len(samples) == 0 {
		t.Fatal("expected some non-skipped samples")
	}
	for _, s := range samples {
		if s.Outcome == Skip {
			t.Fatalf("expected Skip outcomes to never be recorded as a sample")
		}
	}
	if int64(len(samples)) >= calls.Load() {
		t.Fatalf("expected fewer recorded samples (%d) than total calls (%d) since half are skipped", len(samples), calls.Load())
	}
}
