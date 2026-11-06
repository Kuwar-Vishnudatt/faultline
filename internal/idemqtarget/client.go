// Package idemqtarget is the one piece of this project that's specific to
// the particular "real service" this experiment targets — the idemq job
// queue (project #2 in this portfolio series) — as opposed to pkg/proxy,
// pkg/chaos, pkg/loadgen, and pkg/report, which know nothing about idemq
// and could drive a chaos experiment against any HTTP service. It talks to
// idemq purely over its public HTTP API, as an external black-box client —
// it does not import idemq's source at all, which is exactly how a chaos
// harness is used against a real service in practice (you don't get to
// assume source-level access to what you're testing).
package idemqtarget

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// debugClaim, enabled via FAULTLINE_DEBUG_CLAIM=1, prints every claim
// call's raw outcome — the exact instrumentation used to track down this
// project's reconciliation-ordering bug (see docs/ARCHITECTURE.md): it's
// what made it possible to see, concretely, that the server was genuinely
// returning "no job" rather than the client silently misreporting it.
var debugClaim = os.Getenv("FAULTLINE_DEBUG_CLAIM") != ""

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string, timeout time.Duration) *Client {
	return &Client{BaseURL: baseURL, HTTP: &http.Client{Timeout: timeout}}
}

// NewIdempotencyKey returns a fresh, client-generated key. Generating it
// client-side (rather than letting the server assign an ID) is what makes
// the producer side of this experiment resilient to its own chaos: if the
// connection breaks before the enqueue response arrives, the caller still
// knows the key it used and can safely resubmit the identical enqueue call
// later — idemq's own idempotent-enqueue guarantee (see idemq's
// docs/ARCHITECTURE.md) means that resubmission either creates the job (if
// it never actually landed) or returns the original job's ID (if it did),
// with no ambiguity and no duplicate, which is exactly the property this
// chaos experiment's correctness check relies on.
func NewIdempotencyKey(prefix string) string {
	var b [8]byte
	rand.Read(b[:])
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b[:]))
}

// classify turns a raw HTTP attempt's outcome into (succeeded, ambiguous).
// "Ambiguous" means the request may have reached and been acted on by the
// server even though this caller didn't get to see that — the same
// connection-refused-vs-everything-else distinction that mattered for
// raft-kv's linearizability classification.
func classify(err error, status int) (succeeded, ambiguous bool) {
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			return false, false // provably never reached the server
		}
		return false, true // timeout, reset, EOF mid-response — unknown
	}
	return status >= 200 && status < 300, false
}

type EnqueueResult struct {
	JobID     string
	Succeeded bool
	Ambiguous bool
}

func (c *Client) Enqueue(ctx context.Context, queue, idemKey string, maxAttempts int) EnqueueResult {
	body, _ := json.Marshal(map[string]any{
		"payload":         map[string]string{"task": "chaos-experiment"},
		"idempotency_key": idemKey,
		"max_attempts":    maxAttempts,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/queues/"+queue+"/enqueue", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	status := 0
	var respBody []byte
	if err == nil {
		status = resp.StatusCode
		respBody, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	succeeded, ambiguous := classify(err, status)

	var out EnqueueResult
	out.Succeeded, out.Ambiguous = succeeded, ambiguous
	if succeeded {
		var parsed struct {
			ID string `json:"id"`
		}
		if jerr := json.Unmarshal(respBody, &parsed); jerr == nil {
			out.JobID = parsed.ID
		}
	}
	return out
}

type ClaimedJob struct {
	ID         string `json:"id"`
	LeaseToken int64  `json:"lease_token"`
}

// ClaimAndAck claims one job from queue and immediately acks it. Returns
// (hadJob, succeeded, ambiguous): hadJob is false when the queue was simply
// empty (not a failure — see loadgen.Skip), succeeded is true only if both
// the claim and the ack completed cleanly.
func (c *Client) ClaimAndAck(ctx context.Context, queue string, leaseMs int64) (hadJob, succeeded, ambiguous bool) {
	body, _ := json.Marshal(map[string]any{"lease_ms": leaseMs})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/queues/"+queue+"/claim", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		if debugClaim {
			fmt.Printf("DEBUGCLAIM err=%v\n", err)
		}
		_, amb := classify(err, 0)
		return true, false, amb // we don't know if a job existed; treat as an attempted op
	}
	defer resp.Body.Close()
	if debugClaim {
		fmt.Printf("DEBUGCLAIM status=%d remoteAddr=%s\n", resp.StatusCode, req.URL.String())
	}

	if resp.StatusCode == http.StatusNoContent {
		return false, false, false // genuinely empty queue, not a failure
	}
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return true, false, false
	}

	var job ClaimedJob
	if jerr := json.NewDecoder(resp.Body).Decode(&job); jerr != nil {
		return true, false, false
	}

	ackBody, _ := json.Marshal(map[string]any{"token": job.LeaseToken})
	ackReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/jobs/"+job.ID+"/ack", bytes.NewReader(ackBody))
	ackReq.Header.Set("Content-Type", "application/json")
	ackResp, ackErr := c.HTTP.Do(ackReq)
	if ackErr != nil {
		_, amb := classify(ackErr, 0)
		return true, false, amb
	}
	defer ackResp.Body.Close()
	return true, ackResp.StatusCode == http.StatusOK, false
}

// GetJobStatus fetches a job's current status for post-experiment
// reconciliation.
func (c *Client) GetJobStatus(ctx context.Context, jobID string) (status string, err error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/jobs/"+jobID, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	var parsed struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", err
	}
	return parsed.Status, nil
}

// ResolveByIdempotencyKey re-submits the identical enqueue call for a key
// used earlier in the experiment, returning the definitive job ID it maps
// to regardless of whether the original attempt is known to have
// succeeded. Used in post-chaos reconciliation.
func (c *Client) ResolveByIdempotencyKey(ctx context.Context, queue, idemKey string, maxAttempts int) (string, error) {
	res := c.Enqueue(ctx, queue, idemKey, maxAttempts)
	if !res.Succeeded {
		return "", fmt.Errorf("could not resolve idempotency key %q after chaos ended", idemKey)
	}
	return res.JobID, nil
}
