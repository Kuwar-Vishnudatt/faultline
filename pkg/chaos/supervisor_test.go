package chaos

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestSupervisorStartKillRestartCycle(t *testing.T) {
	s := NewSupervisor("sleeper", "/bin/sleep", "30")
	if err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !s.Running() {
		t.Fatal("expected Running() true after Start")
	}

	// Capture the real OS PID so we can independently verify the kill
	// actually terminated the process, not just that our bookkeeping says so.
	s.mu.Lock()
	pid := s.cmd.Process.Pid
	s.mu.Unlock()

	if err := s.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if s.Running() {
		t.Fatal("expected Running() false after Kill")
	}

	time.Sleep(50 * time.Millisecond) // let the OS actually reap/terminate it
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatalf("expected process %d to be gone after Kill, but it still responds to signal 0", pid)
	}

	if err := s.Restart(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if !s.Running() {
		t.Fatal("expected Running() true after Restart")
	}
	s.mu.Lock()
	newPID := s.cmd.Process.Pid
	s.mu.Unlock()
	if newPID == pid {
		t.Fatal("expected a genuinely new process with a different PID after restart")
	}

	s.Kill() // cleanup
}

func TestKillingNotRunningProcessErrors(t *testing.T) {
	s := NewSupervisor("never-started", "/bin/sleep", "30")
	if err := s.Kill(); err == nil {
		t.Fatal("expected an error killing a process that was never started")
	}
}

func TestDoubleStartErrors(t *testing.T) {
	s := NewSupervisor("sleeper", "/bin/sleep", "30")
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Kill()
	if err := s.Start(); err == nil {
		t.Fatal("expected starting an already-running supervisor to error")
	}
}

func TestMain(m *testing.M) {
	// /bin/sleep must exist for these tests to mean anything; skip loudly
	// rather than silently passing on a system without it.
	if _, err := os.Stat("/bin/sleep"); err != nil {
		panic("these tests require /bin/sleep to exist: " + err.Error())
	}
	os.Exit(m.Run())
}
