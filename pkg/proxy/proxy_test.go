package proxy

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"
)

// startEchoServer starts a trivial line-echo TCP server for the proxy to
// sit in front of, and returns its address.
func startEchoServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					fmt.Fprintf(c, "%s\n", scanner.Text())
				}
			}(conn)
		}
	}()
	return l.Addr().String()
}

func startProxy(t *testing.T, upstream string) *Proxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close() // release it; Proxy.Start will re-bind the same port

	p := New(addr, upstream)
	if err := p.Start(); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestTransparentPassthroughWithNoFaults(t *testing.T) {
	upstream := startEchoServer(t)
	p := startProxy(t, upstream)

	conn, err := net.Dial("tcp", p.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "hello\n")
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "hello\n" {
		t.Fatalf("expected transparent echo, got %q", line)
	}
}

func TestLatencyInjectionDelaysForwarding(t *testing.T) {
	upstream := startEchoServer(t)
	p := startProxy(t, upstream)
	p.SetFaults(Faults{LatencyMin: 80 * time.Millisecond, LatencyMax: 100 * time.Millisecond})

	conn, err := net.Dial("tcp", p.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	start := time.Now()
	fmt.Fprintf(conn, "hi\n")
	reader := bufio.NewReader(conn)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("read: %v", err)
	}
	elapsed := time.Since(start)

	// Latency applies to both directions (request to upstream, response
	// back), so expect at least roughly 2x the minimum.
	if elapsed < 150*time.Millisecond {
		t.Fatalf("expected injected latency to delay the round trip, got %v", elapsed)
	}
}

func TestDropProbabilityOneSeversEveryConnection(t *testing.T) {
	upstream := startEchoServer(t)
	p := startProxy(t, upstream)
	p.SetFaults(Faults{DropProbability: 1.0})

	conn, err := net.Dial("tcp", p.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "hi\n")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)
	_, err = reader.ReadString('\n')
	if err == nil {
		t.Fatal("expected the connection to be severed (EOF or reset), got a successful echo")
	}
}

func TestNoFaultsNeverDropsConnections(t *testing.T) {
	upstream := startEchoServer(t)
	p := startProxy(t, upstream)

	for i := 0; i < 20; i++ {
		conn, err := net.Dial("tcp", p.ListenAddr)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(conn, "msg-%d\n", i)
		reader := bufio.NewReader(conn)
		line, err := reader.ReadString('\n')
		conn.Close()
		if err != nil {
			t.Fatalf("iteration %d: expected success with no faults configured: %v", i, err)
		}
		if line != fmt.Sprintf("msg-%d\n", i) {
			t.Fatalf("iteration %d: unexpected echo %q", i, line)
		}
	}
	if p.ConnsDropped() != 0 {
		t.Fatalf("expected zero drops with DropProbability=0, got %d", p.ConnsDropped())
	}
}

// TestCloseReturnsPromptlyWithAnIdleOpenConnection is a regression test for
// a real hang this project hit: an idle keep-alive connection through the
// proxy (no faults, no more data exchanged, just left open — exactly what
// an HTTP client's connection pool does) has its proxying goroutines
// blocked in Read() waiting for bytes that will never arrive. Closing only
// the listener (as an earlier version of Close did) never touches that
// already-accepted connection, so Close hung forever. Close must forcibly
// close every open connection, not just stop accepting new ones.
func TestCloseReturnsPromptlyWithAnIdleOpenConnection(t *testing.T) {
	upstream := startEchoServer(t)
	p := New(mustFreeAddr(t), upstream)
	if err := p.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	conn, err := net.Dial("tcp", p.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "hello\n")
	if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
		t.Fatalf("initial exchange should succeed: %v", err)
	}
	// Connection now sits open and idle — exactly the hang-triggering state.

	done := make(chan error, 1)
	go func() { done <- p.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close returned an error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung with an idle open connection present")
	}
}

func mustFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestFaultsCanBeUpdatedLiveBetweenConnections(t *testing.T) {
	upstream := startEchoServer(t)
	p := startProxy(t, upstream)

	// Healthy first.
	conn1, _ := net.Dial("tcp", p.ListenAddr)
	fmt.Fprintf(conn1, "a\n")
	if _, err := bufio.NewReader(conn1).ReadString('\n'); err != nil {
		t.Fatalf("expected healthy connection to succeed: %v", err)
	}
	conn1.Close()

	// Now go fully lossy.
	p.SetFaults(Faults{DropProbability: 1.0})
	conn2, _ := net.Dial("tcp", p.ListenAddr)
	fmt.Fprintf(conn2, "b\n")
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := bufio.NewReader(conn2).ReadString('\n'); err == nil {
		t.Fatal("expected the connection to be severed after faults were updated")
	}
	conn2.Close()

	// And back to healthy.
	p.SetFaults(Faults{})
	conn3, _ := net.Dial("tcp", p.ListenAddr)
	fmt.Fprintf(conn3, "c\n")
	if _, err := bufio.NewReader(conn3).ReadString('\n'); err != nil {
		t.Fatalf("expected recovery to healthy behavior: %v", err)
	}
	conn3.Close()
}
