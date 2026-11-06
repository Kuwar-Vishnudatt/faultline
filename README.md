# faultline

A chaos-engineering harness: a userspace fault-injecting network proxy, a
phased chaos scenario runner with real process kill/restart, a load
generator, and an SLO report generator (latency percentiles + success rate,
rendered as charts) — applied, concretely, against [idemq](https://github.com/Kuwar-Vishnudatt/idemq),
a real durable job queue built earlier in this same portfolio series, to
prove its crash-recovery and idempotency guarantees hold under actual
network chaos and actual process kills, not just simulated ones.

```
   load generator              chaos proxy                idemq-server
  (producer+consumer) ──────▶ (latency, packet   ──────▶  (the real
   via idemqtarget.Client      loss injection)              service
                                    │                        under test)
                                    │
                          chaos.Runner drives both
                          the proxy's fault profile
                          AND actual SIGKILL+restart
                          of the idemq-server process,
                          phase by phase
```

## Why a userspace proxy, not tc/netem

Linux's `tc`/`netem` (the "real" way to inject network-level faults) needs
root and a Linux kernel — unavailable in most sandboxes, on macOS, and in
many CI runners. `pkg/proxy` instead implements the same architecture as
Shopify's Toxiproxy: a TCP proxy that delays or severs connections in
userspace, with live-updatable fault profiles, no privileges required. Node
kill/restart chaos is handled separately and for real — `pkg/chaos.Supervisor`
owns an actual OS subprocess and sends it a genuine `SIGKILL`.

## Quickstart

```bash
make build       # builds chaos-runner, and go-installs idemq-server as the target
make experiment  # runs the full phased chaos experiment, writes a report
```

A finished experiment's `results/<timestamp>/` directory contains `report.md`
(with embedded SVG charts), raw per-request CSVs, and the target's own log.
A real run's results are checked into [results/final](results/final) in
this repo — see that report for the actual before/after numbers.

## What the experiment proves

The scenario runs idemq's server through six phases — `baseline`,
`latency_storm`, `packet_loss`, `node_kill` (an actual `SIGKILL` + restart
mid-phase), `combined_chaos` (latency + packet loss + another kill), and
`recovery` — while a producer continuously enqueues jobs and a consumer
continuously claims and acks them, all traffic routed through the chaos
proxy. After the scenario ends, a **reconciliation pass** resolves every
idempotency key used during the experiment to its definitive job ID
(bypassing the proxy, talking straight to idemq-server) and confirms every
single one reached `completed` — i.e., **no job was permanently lost**,
despite real packet loss, real latency spikes, and the server process
being killed outright, twice, mid-experiment.

From [results/final/report.md](results/final/report.md) (a real run):
during `combined_chaos` (packet loss + latency + a process kill), the
consumer's claim/ack success rate as directly observed by clients fell as
low as **~14%** — and **100% of jobs still completed** once the dust
settled. That gap is the entire point: idemq's fencing tokens and
idempotent enqueue mean a client-visible failure during chaos is not the
same thing as data loss, and this experiment measures that distinction
with real numbers instead of asserting it.

## Debugging knobs

A few environment variables exist for investigating a run, not just
producing one (the kind of thing you want *while* debugging an experiment,
not scaffolding to delete afterward — these are what this project's own
investigation into real bugs was built on; see docs/ARCHITECTURE.md):

| Variable | Effect |
|---|---|
| `FAULTLINE_DRAIN_WAIT` | Override the post-chaos drain window (default `150s`) |
| `FAULTLINE_KEEP_DB` | Don't delete idemq's SQLite file on exit — inspect it with `sqlite3` afterward |
| `FAULTLINE_PAUSE_BEFORE_EXIT=<duration>` | Keep the target process alive after the report is written, for live `curl`/inspection |
| `FAULTLINE_DEBUG_CLAIM` | Print every claim call's raw outcome from the client's point of view |

## Package layout

```
pkg/proxy/      generic TCP fault-injecting proxy (latency, connection severing)
pkg/chaos/      Supervisor (real process kill/restart) + Scenario/Phase runner
pkg/loadgen/    concurrent load generator, phase-tagged samples, outcome classification
pkg/report/     percentiles, phase SLO summaries, markdown + hand-rolled SVG charts
internal/idemqtarget/  the one idemq-specific piece: a black-box HTTP client
                        for idemq's public API (no source-level coupling)
cmd/chaos-runner/       wires it all into the concrete idemq experiment
docs/ARCHITECTURE.md    design rationale + two real bugs found building this
```

Everything under `pkg/` is generic — it doesn't know idemq exists, and
could drive a chaos experiment against any HTTP service by writing a new
`internal/<target>` client package.

## Testing

```bash
make test-all    # unit + integration tests across all pkg/ packages
make test-race   # same, with the race detector
```

`pkg/proxy` is tested for transparent passthrough, latency injection, and
connection-severing (and for the real half-close bug found while building
it — see ARCHITECTURE.md). `pkg/chaos` verifies the scenario runner's phase
ordering and proves process kill/restart against a real `/bin/sleep`
subprocess, checked via an independent `syscall.Kill(pid, 0)` liveness
probe. `pkg/report`'s percentile and SVG-rendering logic is unit tested
directly. These are fast, hermetic tests with no dependency on idemq being
built; the full end-to-end experiment (`make experiment`) is the separate,
slower, "real service" proof.

## What's deliberately out of scope (v1)

- **CPU/memory/disk-pressure injection.** This harness only injects network
  and process-level faults — it doesn't simulate a noisy neighbor stealing
  CPU or disk I/O. (Ironically, this project's own debugging process ran
  into exactly that kind of interference from an overloaded dev machine —
  see ARCHITECTURE.md's account of the investigation it took to rule that
  out as the actual cause of what turned out to be a real, deterministic
  bug.)
- **Distributed target topologies.** The proxy fronts one target address;
  fanning out to N instances behind a load balancer would need a second
  proxy mode, not implemented here.
- **Automated regression gating** (failing a CI run if an SLO crosses a
  threshold). The report is generated for a human to read; wiring
  `report.Summarize`'s output into pass/fail thresholds would be a
  straightforward addition on top of what's here, not built in v1.
