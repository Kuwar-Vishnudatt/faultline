package chaos

import (
	"context"
	"log"
	"time"

	"github.com/Kuwar-Vishnudatt/faultline/pkg/proxy"
)

// Phase is one named, timed segment of an experiment: a fault profile
// applied to the proxy for Duration, with an optional kill of the target
// process partway through. Naming and timing phases explicitly (rather than
// just "ramp faults up over time") is what makes an experiment's results
// attributable: a report can say "p99 latency during the packet_loss phase
// was X" instead of just "p99 latency went up at some point."
type Phase struct {
	Name     string
	Duration time.Duration
	Faults   proxy.Faults
	// KillAt, if nonzero, kills the supervised target process this far into
	// the phase, then restarts it KillAt+RestartAfter into the phase (both
	// relative to the phase's own start, so phases remain independently
	// composable regardless of order).
	KillAt       time.Duration
	RestartAfter time.Duration
}

// Runner executes a sequence of Phases against a Proxy and (optionally) a
// Supervisor, invoking onPhase at the start of each phase so a caller
// (typically the load generator) can tag in-flight samples with the current
// phase name.
type Runner struct {
	Proxy      *proxy.Proxy
	Supervisor *Supervisor // nil if this experiment doesn't exercise process kill
	Phases     []Phase
}

// Run blocks until every phase has completed or ctx is cancelled, calling
// onPhaseStart(name) at the beginning of each phase.
func (r *Runner) Run(ctx context.Context, onPhaseStart func(name string)) {
	for _, phase := range r.Phases {
		if ctx.Err() != nil {
			return
		}
		log.Printf("[chaos] phase %q starting (duration=%v, faults=%+v)", phase.Name, phase.Duration, phase.Faults)
		r.Proxy.SetFaults(phase.Faults)
		if onPhaseStart != nil {
			onPhaseStart(phase.Name)
		}

		phaseDeadline := time.NewTimer(phase.Duration)
		var killTimer, restartTimer *time.Timer
		if r.Supervisor != nil && phase.KillAt > 0 {
			killTimer = time.NewTimer(phase.KillAt)
		}

	phaseLoop:
		for {
			select {
			case <-ctx.Done():
				phaseDeadline.Stop()
				return
			case <-phaseDeadline.C:
				break phaseLoop
			case <-timerC(killTimer):
				log.Printf("[chaos] phase %q: killing target process %q", phase.Name, r.Supervisor.Name())
				if err := r.Supervisor.Kill(); err != nil {
					log.Printf("[chaos] kill failed: %v", err)
				}
				restartTimer = time.NewTimer(phase.RestartAfter)
				killTimer = nil
			case <-timerC(restartTimer):
				log.Printf("[chaos] phase %q: restarting target process %q", phase.Name, r.Supervisor.Name())
				if err := r.Supervisor.Restart(); err != nil {
					log.Printf("[chaos] restart failed: %v", err)
				}
				restartTimer = nil
			}
		}
	}
}

// timerC returns t.C, or a nil channel (which blocks forever in a select)
// if t is nil — lets the select above treat "no kill/restart scheduled"
// uniformly without a bunch of extra branching.
func timerC(t *time.Timer) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}
