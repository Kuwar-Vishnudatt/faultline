// Package chaos drives fault-injection over time: a Supervisor owns a real
// OS subprocess and can genuinely kill and restart it (SIGKILL, not a
// simulated flag), and a Scenario sequences the proxy's network faults and
// process kills into named, timed phases so an experiment is reproducible
// and its results are attributable to a specific phase.
package chaos

import (
	"fmt"
	"io"
	"os/exec"
	"sync"
)

// Supervisor manages one long-lived subprocess, restartable after a real
// kill. This is "node kill" chaos in the original Chaos Monkey sense: the
// target process is actually terminated by the OS, not told to pretend to
// be down — so whatever the target does for crash recovery (on-disk state,
// reconnect logic, etc.) is genuinely exercised, not merely assumed.
type Supervisor struct {
	name string
	path string
	args []string

	stdout, stderr io.Writer

	mu  sync.Mutex
	cmd *exec.Cmd
}

func NewSupervisor(name, path string, args ...string) *Supervisor {
	return &Supervisor{name: name, path: path, args: args}
}

// SetOutput directs the subprocess's stdout/stderr somewhere other than
// /dev/null (useful for debugging an experiment run); takes effect on the
// next Start/Restart.
func (s *Supervisor) SetOutput(stdout, stderr io.Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stdout, s.stderr = stdout, stderr
}

// Start launches the subprocess if it is not already running.
func (s *Supervisor) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd != nil {
		return fmt.Errorf("supervisor %s: already running", s.name)
	}
	cmd := exec.Command(s.path, s.args...)
	cmd.Stdout = s.stdout
	cmd.Stderr = s.stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("supervisor %s: start: %w", s.name, err)
	}
	s.cmd = cmd
	go func() {
		cmd.Wait() // reap; prevents a zombie process after Kill
	}()
	return nil
}

// Kill sends SIGKILL to the running subprocess — an actual, unrecoverable
// termination, not graceful shutdown, which is the point: real chaos
// engineering tests what happens when a process disappears with no chance
// to clean up (mid-write, mid-connection, lock held, whatever state it was
// in at that instant).
func (s *Supervisor) Kill() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cmd == nil {
		return fmt.Errorf("supervisor %s: not running", s.name)
	}
	err := s.cmd.Process.Kill()
	s.cmd = nil
	return err
}

// Restart starts a fresh instance after a Kill. Safe to call whether or not
// a previous instance is still tracked (it is not, after Kill clears cmd).
func (s *Supervisor) Restart() error {
	return s.Start()
}

func (s *Supervisor) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cmd != nil
}

func (s *Supervisor) Name() string { return s.name }
