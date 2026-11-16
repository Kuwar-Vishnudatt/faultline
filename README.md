# faultline

A chaos-engineering harness, written in Go with zero external dependencies,
that injects *real* network and process failures into a *real* running
service and measures exactly what happens. It isn't a simulator: the
latency it adds really delays bytes on a real TCP connection, the
connections it severs are really closed out from under a real HTTP client,
and the process it kills really receives a `SIGKILL` from the operating
system and has to restart from scratch. The one service it has actually
been pointed at — [idemq](https://github.com/Kuwar-Vishnudatt/idemq), a
durable job queue built earlier in this same portfolio — is treated the
whole time as a black box: faultline talks to it only over plain HTTP,
the same way any real client would, with no special hooks and no access
to idemq's source.

```
   load generator              chaos proxy                idemq-server
  (producer+consumer) ──────▶ (latency, connection ──────▶  (the real
   via idemqtarget.Client      severing injection)            service
                                    │                          under test)
                                    │
                          chaos.Runner drives both
                          the proxy's fault profile
                          AND actual SIGKILL+restart
                          of the idemq-server process,
                          phase by phase
```

## Why this exists

"Our system handles failures gracefully" is a sentence every engineer has
written in a design doc, and on its own it is worth exactly nothing. It's
an untested claim until someone actually breaks something and checks what
happened afterward. Most "resilience tests" don't do that — they call a
function that pretends to fail, inside the same process, with the test
code fully aware it's a test. That's a legitimate first pass (idemq's own
test suite does exactly this at the application layer), but it leaves a
gap: it only proves the system survives the failures its own test author
thought to simulate.

faultline exists to close that gap for one specific, concrete case. It
puts a real proxy on the wire between a client and a real server, it
really cuts connections and really delays bytes, and it really kills the
server process with `SIGKILL` — the same signal an OOM-killer or a
rebooted machine would send, giving the process zero chance to clean up,
flush a buffer, or finish a write. Then it counts, honestly, what the
client actually saw happen and what the server-side truth actually was
afterward. The rest of this document is the story of building that tool
and the one real target it was pointed at.

## How it works

### 1. A fault-injecting proxy sits in the middle

`pkg/proxy` is a plain TCP proxy. A client connects to it instead of to
the real service; the proxy opens its own connection to the real service
and copies bytes back and forth in both directions, chunk by chunk. As
long as no fault is active, this is invisible — the client is talking to
the real service, just through one extra hop.

The interesting part is what the proxy can do to each chunk before
forwarding it, controlled by a `Faults` value that can be swapped out at
any moment, mid-experiment, without restarting the proxy or dropping any
connection that's already open:

- **Latency injection** — delay a chunk by a random duration drawn from a
  configured range before forwarding it. This simulates a slow or
  congested network: the bytes all arrive eventually, just later than
  they should.
- **Connection severing** — instead of forwarding a chunk, close the
  connection outright, with some configured probability per chunk. This
  is deliberately *not* byte-level corruption. A real lossy network
  doesn't hand an application corrupted bytes — TCP's own checksums and
  retransmission logic hide that entirely from anything above the socket
  layer. What a real client actually experiences during packet loss is
  resets and timeouts, i.e. connections that just stop working. So
  "severing the connection" is a more faithful simulation of packet-loss
  symptoms than it might sound like at first — it reproduces the actual
  failure mode an application sees, not a mechanism no application ever
  observes directly.

This is the same basic architecture as Shopify's Toxiproxy, and it was a
deliberate choice over the "more real" alternative: Linux's `tc`/`netem`,
which manipulates the kernel's own network stack to inject latency and
loss. `tc`/`netem` is more faithful in some ways (it can do things a
userspace proxy can't, like realistic packet reordering), but it needs
`CAP_NET_ADMIN` — effectively root — and a Linux kernel. That means it
doesn't work on macOS, doesn't work in most sandboxed environments, and
doesn't work in a lot of CI runners. A chaos tool that only runs with root
on Linux is a tool most engineers can't actually run on their own laptop
to reproduce a problem. faultline trades away some of that fidelity for
something it considers more valuable for a tool meant to be cloned and
run by anyone: it needs no privileges at all, and it runs anywhere Go
runs.

### 2. A chaos phase is a named, timed segment with a fault profile and, optionally, a real kill

A "chaos scenario" (`pkg/chaos`) is just an ordered list of **phases**.
Each phase has a name, a duration, a `Faults` profile to apply to the
proxy for that duration, and — optionally — a scheduled process kill
partway through, with a scheduled restart some time after that. Naming
and timing the phases explicitly (rather than just ramping chaos up
continuously) is what makes the results attributable afterward: the final
report can say "here's what p99 latency and success rate looked like
specifically during `packet_loss`," rather than just "something got worse
at some point."

Process kill is handled by a completely separate mechanism from the proxy,
on purpose: `pkg/chaos.Supervisor` owns the target as a real OS
subprocess, and when a phase schedules a kill, the supervisor sends it an
actual `SIGKILL` — the same signal the OS sends when something is forcibly
terminated, with no opportunity for graceful shutdown, no chance to finish
a write or release a lock cleanly. After the scheduled delay, the
supervisor restarts a fresh instance of the process. This genuinely
exercises whatever the target does for crash recovery — reading its state
back off disk, re-establishing its own internal bookkeeping — rather than
assuming it works because a unit test called a `Shutdown()` method.

The concrete experiment in this repo (`cmd/chaos-runner`) runs idemq's
server through six phases in order: `baseline` (no faults, to establish a
clean comparison point), `latency_storm` (every chunk delayed 60-180ms),
`packet_loss` (20% chance per chunk of a severed connection),
`node_kill` (a real `SIGKILL` partway through, restarted two seconds
later), `combined_chaos` (latency *and* packet loss *and* another real
kill, all at once — the worst phase), and `recovery` (faults removed
again, to see if things return to baseline).

### 3. A load generator drives traffic and classifies every outcome honestly

While the chaos phases run, `pkg/loadgen` drives two independent, ongoing
streams of real HTTP traffic through the proxy: a **producer**
continuously enqueuing new jobs, and a **consumer** continuously claiming
and acknowledging ("acking") them. Every single attempt — not a sample,
every one — is timed and recorded with the phase that was active when it
started.

The important design decision here is how an attempt's outcome gets
classified, and it's a three-way split, not a simple success/fail binary:

- **Success** — the request completed and got back exactly the response
  it should have.
- **Failure** — the request *provably* never reached the server at all
  (the harness treats a `connection refused` error this way — there's
  nothing ambiguous about a connection that was never established).
- **Ambiguous** — anything else that didn't cleanly succeed: a timeout, a
  connection reset mid-response, an EOF partway through reading a reply.
  These are deliberately *not* counted as failures, because the request
  may well have reached the server and been fully acted on — the client
  just lost visibility into the response. Lumping this into "failure"
  would be a quietly false claim about what the server actually did.
  (This exact failure-vs-ambiguous distinction is a theme that recurs
  elsewhere in this portfolio — raft-kv's linearizability checker and
  llmgate's retry logic both draw the same line for the same reason.)
- There's also a fourth bucket, **Skip**, for the consumer polling an
  empty queue — that's not a meaningful unit of work at all, so it isn't
  recorded as a sample; counting it as a success would understate
  latency, and counting it as a failure would wrongly tank the success
  rate just because the queue happened to be empty at that instant.

This classification is what makes it meaningful to later say "claim/ack
success fell to 14% during combined_chaos" without that number being
inflated or deflated by connections that were never really failures at
all, or by idle polling.

### 4. The report turns raw samples into something a human can actually read

Once a scenario finishes, `pkg/report` groups every recorded sample by
phase and computes, per phase: total requests, success count, failure
count, ambiguous count, success rate, and latency percentiles (p50, p95,
p99). It then renders two things, both hand-rolled with no charting
library:

- A **Markdown table**, one row per phase, with all of the above.
- An **SVG bar chart** (`pkg/report/chart.go`) of success rate and latency
  by phase, embedded directly into the Markdown report so it renders
  inline on GitHub or in any Markdown viewer — no separate image hosting,
  no JavaScript, just plain SVG markup written out by hand.

`cmd/chaos-runner` writes the finished report, the raw per-request CSVs
(timestamp, phase, latency, outcome, error, for every single attempt),
and the target's own log output to a timestamped `results/<timestamp>/`
directory. A real run's output is checked into [results/final](results/final)
in this repo, so the actual numbers below can be inspected directly rather
than taken on faith.

## The real test: applying this to idemq

The one concrete thing faultline has been run against is
[idemq](https://github.com/Kuwar-Vishnudatt/idemq) — project #2 in this
portfolio, a real durable job queue with its own published binary
(`idemq-server`), installed here as an ordinary Go module dependency
(`go install github.com/Kuwar-Vishnudatt/idemq/cmd/idemq-server@latest`)
and run as a genuine separate OS process, not linked in or mocked. idemq's
own design makes two promises worth testing for real: that a job
enqueued with an idempotency key will never be silently lost or silently
duplicated, even if the enqueue call itself fails partway through, and
that its crash recovery (lease expiry, retry backoff, a "reaper" that
reclaims abandoned work) actually works when the process genuinely dies
mid-job, not just when it's told to shut down cleanly.

The experiment: run idemq's server through the six chaos phases described
above, with a producer and consumer hammering it through the proxy the
whole time, then — once the chaos stops — run a careful **reconciliation
pass** to find out, definitively, what actually happened to every job that
was ever attempted.

**Reconciliation** means: for every idempotency key the producer ever
generated during the whole run (1,098 of them, in the checked-in real
run), resolve it to its one true, final job ID by calling idemq's enqueue
endpoint again with that same key — idemq's idempotent-enqueue guarantee
means this is always safe to do, because it returns the *existing* job if
one was already created for that key, or creates it for the first time if
the original attempt never actually reached the server. Then keep claiming
and acking against the now-healthy server for a long, patient window,
giving any job that's sitting in a retry backoff time to come back around.
Then check every single resolved job's final status.

The real numbers, from [results/final/report.md](results/final/report.md):

| Phase | Consumer claim/ack success rate |
|---|---:|
| baseline | 100.0% |
| latency_storm | 97.3% |
| packet_loss | 12.3% |
| node_kill | 30.7% |
| **combined_chaos** | **14.4%** |
| recovery | 100.0% |

During `combined_chaos` — packet loss, injected latency, and a real
process kill, all running at once — the consumer's directly observed
claim/ack success rate collapsed to **14.4%**. Four out of every five
attempts the client made either failed outright or came back ambiguous.
By any naive reading, that looks like a system falling over.

And yet: **all 1,098 of the 1,098 jobs enqueued during the entire
experiment ended up with status `completed`** after reconciliation. Zero
stuck pending, zero dead-lettered, zero lost. That gap — a client-visible
success rate of 14% against a server-side completion rate of 100% — is
the entire point of running this experiment instead of just asserting
idemq is resilient. A client *seeing* a request fail during chaos is not
the same fact as a job actually being lost. idemq's idempotent enqueue
and its lease/reaper/backoff crash-recovery machinery mean that what the
client experienced as chaos — timeouts, resets, a dead connection — was,
from the job's point of view, just a delay. Every job that looked
"attempted and inconclusive" in the moment was still tracked, still
retried, and still finished. This experiment measures that distinction
with real numbers produced by real failures, instead of taking anyone's
word for it.

## Real bugs found while building this

Two real, non-obvious bugs turned up while building faultline itself —
not in idemq, the thing under test, but in the test harness. Both are
worth telling in full, because the investigation is most of the
engineering value; each fix, once found, was tiny.

### Bug #1: the proxy's half-close hang

The very first time the proxy's very first test ran —
`TestTransparentPassthroughWithNoFaults`, about as simple as a test gets:
send one line through the proxy with no faults active, read the echo
back — it hung forever. `Close()` never returned.

The cause: each proxied connection is handled by two goroutines, one
copying bytes client-to-upstream, the other copying upstream-to-client.
The first version of that copying loop (`pipe()`) returned on a normal EOF
without closing anything. So when the client side of the test closed its
connection normally (an ordinary, successful EOF — nothing had gone
wrong), only the goroutine reading *from* the client noticed and returned.
The other goroutine — the one reading the upstream's responses, to send
back to a client that had already hung up — was left blocked inside a
`Read()` call on a connection nobody had told it to close. Nothing was
ever going to arrive on that connection again, and nothing was ever going
to close it either, so that goroutine, and the `wg.Wait()` waiting on both
goroutines to finish, blocked forever.

The fix: every return path out of `pipe()` — a clean EOF, a read error, a
write error, or a deliberately injected drop — now closes *both* ends of
the connection pair before returning, not just its own side. This proxy
deliberately does not support TCP half-close (one direction finishing
doesn't leave the other half open); for the request/response HTTP traffic
it fronts in this project, that's a reasonable simplification and is
documented as a conscious scope decision in the code, not a leftover bug.

### Bug #2: the reconciliation-ordering bug

This one was much harder, and it lived in the harness's own
reconciliation logic, not in the proxy and not in idemq.

The symptom: the first several complete experiment runs all reported
roughly 15-20% of jobs stuck in `pending` status after the chaos finished,
after the reaper had had plenty of chances to sweep them, and after a very
generous (up to 150-second) drain window specifically meant to let any
job in a retry backoff come back around. Every one of those "stuck" jobs
had `attempts=0` — meaning they had never even been claimed once — and
none of them were dead-lettered. That specific signature — never claimed,
not backed off, not dead, just absent — didn't match the first and most
obvious hypothesis (idemq's backoff timing being slower than the harness
expected), and ruling that hypothesis out took genuine investigation,
because every attempt to reproduce the symptom in isolation came back
clean:

- A plain `curl` against the exact same, still-running idemq-server
  process, run immediately after a run that had reported "stuck" jobs,
  found and claimed a job on the very first try, repeatedly.
- A small standalone Go program, using the exact same `idemqtarget.Client`
  code this harness itself uses, run in a tight loop against the same
  populated database, claimed and acked job after job without a single
  failure — including variants that added concurrent background HTTP load
  and variants that killed and restarted the target mid-loop.
- Direct inspection of a "stuck" database with `sqlite3` showed the
  supposedly-stuck rows had `available_at` timestamps safely in the past
  and `attempts = 0` — there was no backoff delay left to wait out. They
  looked, from the database's own point of view, completely claimable.

Taken together, those three results ruled out idemq's server logic, the
database's actual contents, and the HTTP client library, one at a time —
which left exactly one piece of code none of those isolated reproductions
had exercised at all: the experiment's own reconciliation logic in
`cmd/chaos-runner/main.go`.

The actual bug: the producer generates a fresh idempotency key and records
it (in `usedKeys`) *before* attempting the enqueue call — which it has to,
since the entire reason the key exists is so that a *failed* attempt can
still be resolved later. The original code then ran the drain pass (claim
and ack everything claimable) *before* the resolve pass (which maps every
used key to its definitive job ID, creating that job for the first time if
its original enqueue attempt never actually reached the server — for
example, a connection refused during `node_kill`). Any key whose only
enqueue attempt failed outright simply had no corresponding job yet at the
moment the drain window ran. The resolve pass, running afterward during
reconciliation, created that job — but by then the drain window was
already over, so nothing was left to claim it. The job was never actually
stuck or lost; it just didn't exist yet during the one window that could
have claimed it.

The fix is the reordering `resolveAll` and `drainQueue`'s doc comments in
`cmd/chaos-runner/main.go` now describe explicitly: **resolve every key to
a job ID first** (safe for a key whose original attempt succeeded, failed
outright, or landed ambiguously — idemq's idempotent enqueue returns the
same definitive answer regardless), **then drain**, **then** check final
status. After that one reordering, reconciliation across every subsequent
run has consistently shown every job accounted for, at 100% completion.

This is worth telling in full, dead ends included, because the
investigation — ruling out the server, the database, and the client
library one at a time, and seriously (if reluctantly) considering whether
an overloaded dev machine's own CPU contention was interfering with
timing before dismissing that too — was the actual engineering work here.
The fix itself, once the real cause was found, was four lines reordered.

## Architecture at a glance

- **`pkg/proxy`** — the generic TCP fault-injecting proxy: latency
  injection and connection severing, with a live-swappable fault profile
  and clean shutdown semantics (including the half-close fix from Bug #1
  above). Knows nothing about idemq or HTTP; it proxies raw TCP.
- **`pkg/chaos`** — `Supervisor` (owns a real subprocess, can genuinely
  `SIGKILL` and restart it) and `Runner`/`Phase` (sequences the proxy's
  fault profile and the supervisor's kill/restart into named, timed
  phases). Also generic — no idemq-specific knowledge.
- **`pkg/loadgen`** — the concurrent load generator: runs any caller-
  supplied operation on a schedule, times every attempt, tags it with the
  current phase, and records it using the Success/Failure/Ambiguous/Skip
  classification described above.
- **`pkg/report`** — turns raw samples into percentiles
  (`percentile.go`), per-phase SLO summaries (`summary.go`), and the final
  human-readable output: a Markdown table (`markdown.go`) and a hand-rolled
  SVG bar chart (`chart.go`), with no external charting dependency.
- **`internal/idemqtarget`** — the *one* package in this repo that knows
  idemq exists: a plain HTTP client for idemq's public API (enqueue,
  claim, ack, get-status, resolve-by-idempotency-key), used strictly as an
  external black box with no access to idemq's source. This is the only
  piece that would need to change to point faultline at a different HTTP
  service.
- **`cmd/chaos-runner`** — the concrete experiment: wires all of the
  above together, stands up idemq-server as a real subprocess, runs the
  six-phase scenario against it through the proxy, runs the
  resolve-then-drain-then-verify reconciliation pass, and writes the
  final report.
- **`docs/ARCHITECTURE.md`** — the fuller design rationale and both bug
  writeups, for anyone who wants more detail than this README's summary.

Everything under `pkg/` is intentionally generic: none of it imports or
even knows about idemq. Pointing faultline at a different real HTTP
service would mean writing one new `internal/<target>` client package,
not touching `pkg/` at all.

## Try it yourself

```bash
make build       # builds chaos-runner, and go-installs idemq-server as the target
make experiment  # runs the full six-phase chaos experiment, writes a report
```

A finished run's `results/<timestamp>/` directory contains `report.md`
(with the embedded SVG charts), the raw per-request CSVs for both the
producer and the consumer, and idemq-server's own log output for that run.
The real run referenced throughout this README is checked in at
[results/final](results/final) — open `results/final/report.md` to see it
rendered with its charts.

```bash
make test-all    # unit + integration tests across all pkg/ packages
make test-race   # same, with the race detector enabled
```

The unit/integration tests are fast and hermetic — they don't need idemq
built at all. `pkg/proxy` is tested for transparent passthrough, measured
latency injection, connection severing, and live fault updates between
connections. `pkg/chaos` tests the scenario runner's phase ordering and
proves the kill/restart cycle against a real `/bin/sleep` subprocess,
checked by an independent liveness probe (`syscall.Kill(pid, 0)` from
outside the supervisor's own bookkeeping) so the test can't pass just
because the supervisor's internal state claims it did. `pkg/report`'s
percentile math and SVG rendering are unit tested directly. `make
experiment` is the separate, slower, genuinely end-to-end proof against
the real service.

A few environment variables exist specifically for investigating a run
while it's happening, not just producing one — these are, concretely,
what this project's own bug investigations above were built on:

| Variable | Effect |
|---|---|
| `FAULTLINE_DRAIN_WAIT` | Override the post-chaos drain window (default `150s`) |
| `FAULTLINE_KEEP_DB` | Don't delete idemq's SQLite file on exit — inspect it with `sqlite3` afterward |
| `FAULTLINE_PAUSE_BEFORE_EXIT=<duration>` | Keep the target process alive after the report is written, for live `curl`/inspection |
| `FAULTLINE_DEBUG_CLAIM` | Print every claim call's raw outcome from the client's point of view |

## What this deliberately doesn't do (yet)

- **CPU/memory/disk-pressure injection.** This harness only injects
  network-level and process-level faults — it has no way to simulate a
  noisy neighbor stealing CPU cycles or saturating disk I/O. (In a nice
  bit of irony, this project's own debugging process genuinely ran into
  that kind of interference from an overloaded dev machine while chasing
  Bug #2 above, and had to rule it out by hand before finding the real,
  deterministic cause.)
- **Distributed target topologies.** The proxy fronts exactly one target
  address. Fanning out to several instances behind a load balancer would
  need a second proxy mode that doesn't exist yet.
- **Automated regression gating.** The report is built for a human to
  read, not for CI to act on. Turning `report.Summarize`'s per-phase
  output into automatic pass/fail thresholds (e.g. "fail the build if p99
  exceeds X during any phase") would be a straightforward addition on top
  of what's here — just not one this version builds.

## Project layout

```
faultline/
├── cmd/
│   └── chaos-runner/      wires everything into the concrete idemq experiment
├── internal/
│   └── idemqtarget/       the one idemq-specific piece: a black-box HTTP client
├── pkg/
│   ├── proxy/             generic TCP fault-injecting proxy
│   ├── chaos/             Supervisor (real kill/restart) + Scenario/Phase runner
│   ├── loadgen/           concurrent load generator + outcome classification
│   └── report/            percentiles, SLO summaries, markdown + SVG charts
├── docs/
│   └── ARCHITECTURE.md    design rationale + both bug writeups, in more depth
├── results/
│   └── final/             a real, checked-in experiment run (report, CSVs, logs)
├── Makefile
├── go.mod
├── LICENSE                MIT
└── README.md
```

Licensed under the MIT License — see [LICENSE](LICENSE). Source:
[github.com/Kuwar-Vishnudatt/faultline](https://github.com/Kuwar-Vishnudatt/faultline).
