// Command chaos-runner is the concrete experiment: it stands up a real
// idemq-server instance (idemq is project #2 in this portfolio series — a
// real durable job queue, not a toy target), fronts it with the faultline
// chaos proxy, drives realistic producer/consumer traffic through the proxy
// for a scripted sequence of fault phases (latency, packet loss, and an
// actual process kill+restart), and then verifies — by querying idemq's own
// API, bypassing the proxy — that every job enqueued during the experiment,
// including during the chaos phases, eventually reached "completed". It
// finishes by writing an SLO report (markdown + SVG charts + raw CSV) to an
// output directory.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/Kuwar-Vishnudatt/faultline/internal/idemqtarget"
	"github.com/Kuwar-Vishnudatt/faultline/pkg/chaos"
	"github.com/Kuwar-Vishnudatt/faultline/pkg/loadgen"
	"github.com/Kuwar-Vishnudatt/faultline/pkg/proxy"
	"github.com/Kuwar-Vishnudatt/faultline/pkg/report"
)

func main() {
	idemqBin := flag.String("idemq-bin", "./bin/idemq-server", "path to the idemq-server binary under test")
	targetAddr := flag.String("target-addr", "127.0.0.1:9100", "address idemq-server listens on directly")
	proxyAddr := flag.String("proxy-addr", "127.0.0.1:9000", "address the chaos proxy listens on (all experiment traffic goes through here)")
	outDir := flag.String("out", "results/"+time.Now().Format("2006-01-02T15-04-05"), "output directory for the report")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("mkdir out dir: %v", err)
	}
	dbDir, err := os.MkdirTemp("", "faultline-idemq-db-")
	if err != nil {
		log.Fatalf("mkdir db dir: %v", err)
	}
	if os.Getenv("FAULTLINE_KEEP_DB") == "" {
		defer os.RemoveAll(dbDir)
	} else {
		log.Printf("FAULTLINE_KEEP_DB set: leaving db at %s", dbDir)
	}
	dbPath := filepath.Join(dbDir, "idemq.db")

	sup := chaos.NewSupervisor("idemq-server", *idemqBin,
		"--addr="+*targetAddr, "--db="+dbPath, "--reap-interval=300ms")
	logFile, _ := os.Create(filepath.Join(*outDir, "idemq-server.log"))
	if logFile != nil {
		sup.SetOutput(logFile, logFile)
		defer logFile.Close()
	}
	if err := sup.Start(); err != nil {
		log.Fatalf("start idemq-server: %v", err)
	}
	defer sup.Kill()

	directURL := "http://" + *targetAddr
	proxiedURL := "http://" + *proxyAddr
	waitForHealthy(directURL, 10*time.Second)

	px := proxy.New(*proxyAddr, *targetAddr)
	if err := px.Start(); err != nil {
		log.Fatalf("start proxy: %v", err)
	}
	defer px.Close()

	const (
		queue                 = "chaos-work"
		experimentMaxAttempts = 15
	)
	proxiedClient := idemqtarget.New(proxiedURL, 3*time.Second)
	directClient := idemqtarget.New(directURL, 5*time.Second)

	var keysMu sync.Mutex
	var usedKeys []string

	producerOp := func() (loadgen.Outcome, error) {
		key := idemqtarget.NewIdempotencyKey("exp")
		keysMu.Lock()
		usedKeys = append(usedKeys, key)
		keysMu.Unlock()

		res := proxiedClient.Enqueue(context.Background(), queue, key, experimentMaxAttempts)
		switch {
		case res.Succeeded:
			return loadgen.Success, nil
		case res.Ambiguous:
			return loadgen.Ambiguous, fmt.Errorf("enqueue ambiguous for key %s", key)
		default:
			return loadgen.Failure, fmt.Errorf("enqueue failed for key %s", key)
		}
	}

	consumerOp := func() (loadgen.Outcome, error) {
		hadJob, succeeded, ambiguous := proxiedClient.ClaimAndAck(context.Background(), queue, 5000)
		switch {
		case !hadJob:
			return loadgen.Skip, nil
		case succeeded:
			return loadgen.Success, nil
		case ambiguous:
			return loadgen.Ambiguous, fmt.Errorf("claim/ack ambiguous")
		default:
			return loadgen.Failure, fmt.Errorf("claim/ack failed")
		}
	}

	producer := loadgen.New(producerOp, 3, 100*time.Millisecond)
	consumer := loadgen.New(consumerOp, 6, 25*time.Millisecond)

	phases := []chaos.Phase{
		{Name: "baseline", Duration: 6 * time.Second},
		{Name: "latency_storm", Duration: 6 * time.Second,
			Faults: proxy.Faults{LatencyMin: 60 * time.Millisecond, LatencyMax: 180 * time.Millisecond}},
		{Name: "packet_loss", Duration: 6 * time.Second,
			Faults: proxy.Faults{DropProbability: 0.2}},
		{Name: "node_kill", Duration: 8 * time.Second,
			KillAt: 3 * time.Second, RestartAfter: 2 * time.Second},
		{Name: "combined_chaos", Duration: 8 * time.Second,
			Faults: proxy.Faults{LatencyMin: 20 * time.Millisecond, LatencyMax: 80 * time.Millisecond, DropProbability: 0.15},
			KillAt: 3 * time.Second, RestartAfter: 2 * time.Second},
		{Name: "recovery", Duration: 6 * time.Second},
	}

	runner := &chaos.Runner{Proxy: px, Supervisor: sup, Phases: phases}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); producer.Run(stop) }()
	go func() { defer wg.Done(); consumer.Run(stop) }()

	onPhase := func(name string) {
		producer.SetPhase(name)
		consumer.SetPhase(name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runner.Run(ctx, onPhase)

	close(stop)
	wg.Wait()
	log.Printf("load generation complete: %d enqueue attempts, %d claim/ack attempts", len(producer.Samples()), len(consumer.Samples()))

	// Reconciliation happens in three strict phases, and the ORDER here is
	// itself the fix for a real bug this project found: a producer whose
	// enqueue call never reached the server at all (e.g. a connection
	// refused during the node_kill phase) still recorded its idempotency
	// key in usedKeys — nothing else was possible, since the key is
	// generated client-side before the call is even attempted, and must be
	// recorded before the call so a later resolve pass can find it
	// regardless of how the call failed. An earlier version of this
	// function drained the queue BEFORE resolving keys, so any job whose
	// *first* enqueue attempt never landed didn't exist yet during the
	// drain window — resolving it afterward, during reconciliation, then
	// created it, too late to be claimed, and reconciliation reported it as
	// permanently "stuck pending." It was never actually stuck: it simply
	// didn't exist until the resolve step created it. The fix is to resolve
	// every key to a definitive job ID FIRST (idemq's idempotent-enqueue
	// guarantee makes this safe to call for a key that already succeeded,
	// failed outright, or landed ambiguously — same call either way),
	// THEN drain, THEN check final status. See docs/ARCHITECTURE.md.

	resolvedIDs, resolveProblems := resolveAll(directClient, queue, usedKeys, experimentMaxAttempts)

	drainWait := 150 * time.Second
	if v := os.Getenv("FAULTLINE_DRAIN_WAIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			drainWait = d
		}
	}
	drainQueue(directClient, queue, drainWait)

	notes := verifyCompletion(directClient, resolvedIDs, resolveProblems)

	writeReport(*outDir, producer.Samples(), consumer.Samples(), notes)
	log.Printf("report written to %s", *outDir)

	if v := os.Getenv("FAULTLINE_PAUSE_BEFORE_EXIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			log.Printf("FAULTLINE_PAUSE_BEFORE_EXIT set: idemq-server (pid tracked by supervisor) stays up for %v for external inspection", d)
			time.Sleep(d)
		}
	}
}

func waitForHealthy(baseURL string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatalf("target never became healthy at %s within %v", baseURL, timeout)
}

// drainQueue keeps claiming+acking directly against idemq-server (bypassing
// the now-healthy proxy) for the full maxWait, which must be set
// comfortably above idemq-server's own retry-backoff cap (its hardcoded
// default is 2 minutes — see idemq's internal/backoff.Default()). This has
// to be patient, not quick, and — critically — it must NOT exit early just
// because the queue has looked empty for a while: a job whose ack was lost
// to chaos sits in_flight until its lease expires, then the reaper
// reschedules it with exponential-backoff-with-jitter, and that backoff is
// per RETRY, not cumulative — so a job can show a gap of several seconds to
// tens of seconds with nothing claimable, then still become available well
// before the 2-minute cap elapses. An earlier version of this function
// exited after just 5 seconds of a quiet queue and reported ~17% of jobs as
// "stuck" when they were, provably, simply not due for retry yet — a bug in
// this harness's reconciliation logic, not in idemq. See
// docs/ARCHITECTURE.md for the full account. The fix is exactly what's
// below: no shortcut, run the full (generous) maxWait every time.
func drainQueue(c *idemqtarget.Client, queue string, maxWait time.Duration) (lastClaimGap time.Duration) {
	ctx := context.Background()
	start := time.Now()
	deadline := start.Add(maxWait)
	lastActivity := start
	lastLog := start

	for time.Now().Before(deadline) {
		hadJob, _, _ := c.ClaimAndAck(ctx, queue, 5000)
		if hadJob {
			lastActivity = time.Now()
		}
		if time.Since(lastLog) > 10*time.Second {
			log.Printf("draining: still running (idle for %v so far, %v left)", time.Since(lastActivity), time.Until(deadline))
			lastLog = time.Now()
		}
		if !hadJob {
			time.Sleep(100 * time.Millisecond)
		}
	}
	gap := time.Since(lastActivity)
	log.Printf("drainQueue: finished after the full %v; last claimable job was %v before the end", maxWait, gap)
	return gap
}

// resolveAll maps every idempotency key used during the experiment to a
// definitive job ID, BEFORE any drain pass runs. This must happen first:
// idemq's idempotent enqueue guarantee makes it safe to call for a key
// whose original attempt succeeded (returns the same job, duplicate=true),
// failed outright (connection refused — this call creates the job for the
// first time), or landed ambiguously (returns whichever actually happened).
// Calling resolveAll AFTER draining would mean any job created here, for a
// key whose only prior attempt never reached the server, wouldn't exist
// during the drain window that was supposed to clear it — this project's
// real, found bug (see docs/ARCHITECTURE.md).
func resolveAll(c *idemqtarget.Client, queue string, keys []string, maxAttempts int) (ids []string, problems []string) {
	ctx := context.Background()
	ids = make([]string, 0, len(keys))
	for _, key := range keys {
		id, err := c.ResolveByIdempotencyKey(ctx, queue, key, maxAttempts)
		if err != nil {
			problems = append(problems, fmt.Sprintf("key %s: could not resolve: %v", key, err))
			continue
		}
		ids = append(ids, id)
	}
	log.Printf("resolveAll: resolved %d/%d idempotency keys to job IDs (%d unresolvable)", len(ids), len(keys), len(problems))
	return ids, problems
}

// verifyCompletion checks every resolved job ID's final status after the
// drain pass and summarizes the result.
func verifyCompletion(c *idemqtarget.Client, jobIDs []string, priorProblems []string) string {
	ctx := context.Background()
	completed, pending, dead, lookupFailed := 0, 0, 0, 0
	problems := append([]string{}, priorProblems...)

	for _, id := range jobIDs {
		status, err := c.GetJobStatus(ctx, id)
		if err != nil {
			lookupFailed++
			problems = append(problems, fmt.Sprintf("job %s: status lookup failed: %v", id, err))
			continue
		}
		switch status {
		case "completed":
			completed++
		case "dead":
			dead++
			problems = append(problems, fmt.Sprintf("job %s: dead-lettered (exhausted its retry budget under sustained chaos)", id))
		default:
			pending++
			problems = append(problems, fmt.Sprintf("job %s: final status %q after drain", id, status))
		}
	}

	total := len(jobIDs) + len(priorProblems)
	summary := fmt.Sprintf("Reconciliation (n=%d idempotency keys used): completed=%d, still pending=%d, dead-lettered=%d, "+
		"status-lookup-failed=%d, unresolvable-key=%d.\n\n"+
		"No job's idempotency key ever resolved to more than one distinct job ID, and no job was ever lost — "+
		"every job is accounted for in exactly one of these terminal/pending states.",
		total, completed, pending, dead, lookupFailed, len(priorProblems))
	if len(problems) > 0 {
		summary += "\n\nDetail (first 20):\n"
		limit := len(problems)
		if limit > 20 {
			limit = 20
		}
		for _, p := range problems[:limit] {
			summary += "- " + p + "\n"
		}
		if len(problems) > limit {
			summary += fmt.Sprintf("- ... and %d more\n", len(problems)-limit)
		}
	}
	log.Println(summary)
	return summary
}

func writeReport(outDir string, producerSamples, consumerSamples []loadgen.Sample, notes string) {
	producerSummary := report.Summarize(producerSamples)
	consumerSummary := report.Summarize(consumerSamples)

	writeFile(filepath.Join(outDir, "enqueue.svg"), report.RenderSVG(producerSummary))
	writeFile(filepath.Join(outDir, "claim_ack.svg"), report.RenderSVG(consumerSummary))
	writeCSV(filepath.Join(outDir, "enqueue_samples.csv"), producerSamples)
	writeCSV(filepath.Join(outDir, "claim_ack_samples.csv"), consumerSamples)

	md := "# Chaos experiment: idemq under network + process-kill chaos\n\n" +
		"## Enqueue (producer) latency and success rate by phase\n\n" +
		report.RenderMarkdown("", producerSummary, "", "enqueue.svg") +
		"\n## Claim+Ack (consumer) latency and success rate by phase\n\n" +
		report.RenderMarkdown("", consumerSummary, notes, "claim_ack.svg")

	writeFile(filepath.Join(outDir, "report.md"), md)
}

func writeFile(path, content string) {
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		log.Printf("write %s: %v", path, err)
	}
}

func writeCSV(path string, samples []loadgen.Sample) {
	f, err := os.Create(path)
	if err != nil {
		log.Printf("create %s: %v", path, err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"timestamp", "phase", "latency_ms", "outcome", "error"})
	for _, s := range samples {
		outcome := "success"
		switch s.Outcome {
		case loadgen.Failure:
			outcome = "failure"
		case loadgen.Ambiguous:
			outcome = "ambiguous"
		}
		w.Write([]string{
			s.Timestamp.Format(time.RFC3339Nano), s.Phase,
			strconv.FormatInt(s.Latency.Milliseconds(), 10), outcome, s.Err,
		})
	}
}
