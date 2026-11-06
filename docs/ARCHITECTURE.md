# Architecture

## What problem this solves

Every project in this portfolio series up to this point has tested
resilience claims *in-process*: idemq's own chaos test abandons claims at
the application layer to simulate a crashed worker; llmgate's tests flip a
fake backend's failure mode directly. Those are real, valuable tests, but
they all share one limitation — the "failure" is simulated by code that
knows it's a test. This project exists to remove that limitation: inject
faults at the actual network layer (real packet loss, real latency) and
actual process layer (a genuine `SIGKILL`, not a flag), against a real,
separately-built, already-published service, treated as an opaque black
box reachable only over HTTP. If idemq's resilience claims only held up
against failures the test code was specifically designed to produce,
putting a real chaos proxy and a real kill signal between it and its
clients would be where that gap shows up.

## Why a userspace proxy instead of tc/netem

Linux's `tc qdisc ... netem` is the standard way to inject network-level
latency and loss, and it is a genuinely more complete solution — but it
needs `CAP_NET_ADMIN` (effectively root) and a Linux network namespace,
neither of which is available in a sandboxed environment, on macOS, or in
most CI runners. A chaos tool that only works with root on Linux is a tool
most engineers can't run locally to reproduce an issue. `pkg/proxy` instead
takes Shopify's Toxiproxy approach: a plain TCP proxy, in userspace, that
delays or severs connections per a live-updatable fault profile. This loses
some fidelity (it can't simulate, say, asymmetric routing or realistic
packet reordering) but gains universality — it runs anywhere Go runs, with
no privileges, which is why it's the right tradeoff for a *portfolio*
project meant to be clonable and runnable by anyone reviewing it.

## Bug #1: the proxy's half-close bug (found immediately, by the proxy's own first test run)

`pkg/proxy`'s first version copied bytes between client and upstream with
two goroutines per connection, each independently reading from one side and
writing to the other, returning (but not closing anything) on EOF or a
read/write error. The very first test —
`TestTransparentPassthroughWithNoFaults`, the simplest possible case,
sending one line and reading the echoed reply back — hung forever.

The bug: when the client closes its side of the connection (ordinary EOF),
only *that* goroutine's `pipe()` call returns; the *other* goroutine,
copying the upstream's responses back to the client, is left blocked
reading from a connection to the upstream that was never told to close.
Nothing ever closes the upstream connection, so that goroutine blocks
forever, and `handleConn`'s `wg.Wait()` — waiting for both directions to
finish — never returns. The fix: every return path in `pipe()` (EOF,
read error, write error, or a fault-injected drop) now closes *both* the
client and upstream connections, not just lets its own direction end
quietly. This proxy deliberately doesn't support TCP half-close (one
direction finishing ends the whole connection pair) — a fine simplification
for the request/response HTTP traffic it fronts in practice, and it's
documented as a deliberate scope decision in the code, not a leftover bug.

## Bug #2: the reconciliation-ordering bug (the harder one — found only through sustained investigation)

The first several full experiment runs consistently reported roughly
15-20% of jobs stuck in `pending` after the chaos scenario, reaper sweeps,
and a very generous (up to 150-second) drain window — with zero of them
dead-lettered, and every one at `attempts=0` (never claimed even once).
That specific combination — never claimed, not backed off, not dead,
simply absent from the claimable set — didn't fit "idemq's backoff is
slower than expected," which was the first hypothesis and the one the
drain loop's design explicitly guards against (see `drainQueue`'s doc
comment for that first fix). Ruling that out took real investigation,
because every attempt to reproduce the apparent symptom in isolation
*succeeded cleanly*:

- A fresh `curl` against the *same, still-running* idemq-server process,
  immediately after a "stuck" experiment run finished, found and claimed a
  job on the very first try — repeatedly, with zero issue.
- A small standalone Go program using the exact same `idemqtarget.Client`
  code, run in a tight loop against the same populated database, claimed
  and acked job after job without a single failure — including variants
  that deliberately added concurrent background HTTP load on a shared
  `http.Transport`, and variants that killed and restarted the target
  process mid-loop.
- Direct `sqlite3` inspection of a "stuck" database confirmed the supposedly
  stuck rows had `available_at` timestamps safely in the past and
  `attempts = 0` — there was no backoff delay to wait out at all.

Every one of those rules out idemq's server-side logic, the database
contents, and `idemqtarget.Client`'s HTTP handling as the cause — which
left only the one piece of code none of those reproductions exercised: the
*experiment's own reconciliation logic*. The actual bug, once found, was
simple: `cmd/chaos-runner/main.go`'s producer generates an idempotency key
and records it in `usedKeys` **before** attempting the enqueue call — which
it must, since the whole point of the key is to let a *failed* enqueue
attempt be resolved later. But the original code ran `drainQueue` (claim
everything claimable) **before** `resolveAll` (resolve every used key to a
definitive job ID, creating the job for the first time if its original
enqueue attempt never actually reached the server — e.g. a connection
refused during the `node_kill` phase). Any key whose *only* enqueue attempt
failed outright didn't correspond to a real job yet when the drain window
ran — resolution, later, created it *after* the drain had already finished,
with nothing left to claim it. It was never stuck; it simply didn't exist
yet during the only window that could have claimed it.

The fix is the order the comment above describes and the code now
enforces: **resolve every key to a job ID first** (safe to do for a key
that already succeeded, failed outright, or landed ambiguously — idemq's
idempotent enqueue returns the same answer either way), **then drain**,
**then** check final status. After this fix, reconciliation across
multiple runs consistently shows "every job accounted for" at 100%, with
the drain loop typically clearing the entire backlog within the first
couple of seconds of a deliberately generous window.

This is worth narrating in full, including the dead ends, because the
investigation itself is the actual engineering content here — the fix,
once found, is four lines reordered. Diagnosing it required ruling out
the server, the database, the client library, and (seriously considered,
and only reluctantly dismissed after a dedicated clean-system rerun) this
dev machine's own CPU load as the cause, before the actual bug — specific
to this harness's own test logic, not to idemq or to anything in `pkg/`
— became the only remaining explanation.

## Why reaped-and-ambiguous failures are reported separately from provable ones

`internal/idemqtarget/client.go`'s `classify()` applies the same discipline
that mattered in raft-kv's linearizability test and in llmgate's retry
classification: a `connection refused` error means the request provably
never reached the server (nothing to be ambiguous about — the request
simply failed), while anything else (a reset, a timeout, an EOF
mid-response) could have reached the server and been acted on before the
connection broke. `loadgen.Outcome` keeps these as two distinct categories
(`Failure` vs. `Ambiguous`) rather than collapsing them, which is why
`results/final/report.md`'s tables show chaos-phase rows with many
"Ambiguous" outcomes and essentially zero "Failures" — the proxy's fault
injection (packet drops mid-connection, a killed server process) produces
exactly the "request may have landed, client just can't tell" condition,
not a clean "never sent" one, and the report says so honestly instead of
lumping both into one generic error count.

## Testing strategy

- `pkg/proxy`: transparent passthrough with no faults, latency injection
  (measured, not just configured), connection severing under
  `DropProbability=1.0`, and faults updated live between connections on the
  same proxy instance.
- `pkg/chaos`: the scenario runner's phase ordering and timing in
  isolation; the supervisor's kill/restart cycle verified against a real
  subprocess with an *independent* liveness check (`syscall.Kill(pid, 0)`
  from outside the supervisor's own bookkeeping, so the test can't pass
  just because the supervisor's internal state says so).
- `pkg/report`: percentile correctness against a known dataset, phase
  grouping and ambiguous/failure/success counting, and the hand-rolled SVG
  renderer (checked for well-formed output, correct escaping of untrusted
  phase-name text, and graceful handling of empty input).
- The full `cmd/chaos-runner` experiment against real idemq-server is the
  slow, end-to-end proof, not run as part of the fast unit-test suite —
  see `results/final/` for a checked-in real run's output.

## What's deliberately out of scope (v1)

See the README — CPU/memory/disk-pressure injection, multi-instance target
topologies, and automated SLO-threshold gating are all reasonable next
steps this project doesn't attempt.
