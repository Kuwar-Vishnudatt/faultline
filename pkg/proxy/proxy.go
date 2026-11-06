// Package proxy implements a userspace TCP fault-injecting proxy: it sits
// between a client and a real upstream service and can add latency or
// abruptly sever connections, on live-updatable probabilities.
//
// This is deliberately a userspace proxy (the same architecture as
// Shopify's Toxiproxy) rather than OS-level fault injection (Linux tc/netem
// + network namespaces, or iptables). That's not a simplification made to
// dodge the "real" way of doing this — tc/netem requires CAP_NET_ADMIN and
// a Linux kernel, so it's unavailable in a sandboxed environment, on macOS,
// or in most CI runners, and a chaos tool that only works with root on
// Linux is a chaos tool most engineers can't actually run locally. A
// userspace proxy is portable, requires no privileges, and is precise
// (per-connection, per-byte-chunk control) in a way that's genuinely useful
// for testing a specific service's resilience in isolation — which is
// exactly what Toxiproxy is used for in real incident-readiness testing at
// companies that can't or don't want to touch kernel-level network state in
// their test environment.
package proxy

import (
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Faults describes the fault behavior currently in effect. It is swapped
// atomically, so a controller can change it mid-experiment without
// restarting the proxy or racing with in-flight connections.
type Faults struct {
	// LatencyMin/LatencyMax: each forwarded chunk is delayed by a random
	// duration uniformly drawn from [LatencyMin, LatencyMax].
	LatencyMin time.Duration
	LatencyMax time.Duration
	// DropProbability: probability, checked before forwarding each chunk,
	// that this connection is abruptly severed instead. This is how
	// packet-loss-grade flakiness actually manifests to a TCP client in
	// practice — not corrupted bytes, but resets and timeouts — so
	// modeling it as "sometimes just kill the connection" is a more
	// realistic simulation of lossy-network behavior than dropping
	// individual bytes would be (which TCP's own retransmission would mask
	// completely at the byte-stream level the application sees anyway).
	DropProbability float64
}

func (f Faults) latency() time.Duration {
	if f.LatencyMax <= f.LatencyMin {
		return f.LatencyMin
	}
	spread := f.LatencyMax - f.LatencyMin
	return f.LatencyMin + time.Duration(rand.Int63n(int64(spread)))
}

// Proxy forwards TCP connections from ListenAddr to UpstreamAddr, applying
// whatever Faults are currently set.
type Proxy struct {
	ListenAddr   string
	UpstreamAddr string

	faults   atomic.Pointer[Faults]
	listener net.Listener
	wg       sync.WaitGroup

	connsMu sync.Mutex
	conns   map[net.Conn]struct{} // open client-side connections, for forced shutdown

	connsTotal   atomic.Int64
	connsDropped atomic.Int64
}

func New(listenAddr, upstreamAddr string) *Proxy {
	p := &Proxy{ListenAddr: listenAddr, UpstreamAddr: upstreamAddr, conns: make(map[net.Conn]struct{})}
	p.faults.Store(&Faults{})
	return p
}

// SetFaults atomically replaces the fault behavior; takes effect for new
// chunks on both new and already-open connections immediately.
func (p *Proxy) SetFaults(f Faults) { p.faults.Store(&f) }

func (p *Proxy) currentFaults() Faults { return *p.faults.Load() }

func (p *Proxy) ConnsTotal() int64   { return p.connsTotal.Load() }
func (p *Proxy) ConnsDropped() int64 { return p.connsDropped.Load() }

// Start begins accepting connections in the background. Call Close to stop.
func (p *Proxy) Start() error {
	l, err := net.Listen("tcp", p.ListenAddr)
	if err != nil {
		return err
	}
	p.listener = l
	p.wg.Add(1)
	go p.acceptLoop()
	return nil
}

// Close stops accepting new connections AND forcibly closes every
// currently-open one, then waits for all proxying goroutines to exit.
// Closing just the listener is not enough: an idle keep-alive connection
// (the common case for an HTTP client reusing the proxy) has its pipe()
// goroutines blocked in Read(), waiting for more bytes that will never
// come and with no error to react to — the listener closing doesn't touch
// already-accepted connections at all. Without this, Close can hang
// forever on exactly the kind of persistent connection a real HTTP client
// naturally leaves open. This project's own experiment runner hit this
// hang in practice before the fix: see docs/ARCHITECTURE.md.
func (p *Proxy) Close() error {
	err := p.listener.Close()

	p.connsMu.Lock()
	for c := range p.conns {
		c.Close()
	}
	p.connsMu.Unlock()

	p.wg.Wait()
	return err
}

func (p *Proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return // listener closed
		}
		p.wg.Add(1)
		go p.handleConn(conn)
	}
}

func (p *Proxy) handleConn(client net.Conn) {
	defer p.wg.Done()
	defer client.Close()
	p.connsTotal.Add(1)

	p.connsMu.Lock()
	p.conns[client] = struct{}{}
	p.connsMu.Unlock()
	defer func() {
		p.connsMu.Lock()
		delete(p.conns, client)
		p.connsMu.Unlock()
	}()

	upstream, err := net.Dial("tcp", p.UpstreamAddr)
	if err != nil {
		log.Printf("proxy: upstream dial failed: %v", err)
		return
	}
	defer upstream.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.pipe(client, upstream, client, upstream) }()
	go func() { defer wg.Done(); p.pipe(upstream, client, client, upstream) }()
	wg.Wait()
}

// pipe copies from src to dst a chunk at a time, applying latency and
// drop-probability faults per chunk. It always closes both closeA and
// closeB (always client and upstream, regardless of which direction this
// pipe instance is copying) before returning — on a simulated drop, but
// also on an ordinary EOF or any read/write error. Without that, when one
// side finishes normally (e.g. the client closes its connection), only
// this one direction's goroutine would return; the *other* direction would
// stay blocked forever reading from a peer that will never send anything
// else, since nothing ever closed its connection. This proxy intentionally
// does not support TCP half-close (one direction finishing ends the whole
// connection pair) — a deliberate scope simplification, fine for the
// request/response HTTP traffic this proxy fronts in practice.
func (p *Proxy) pipe(dst io.Writer, src io.Reader, closeA, closeB net.Conn) {
	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			f := p.currentFaults()
			if f.DropProbability > 0 && rand.Float64() < f.DropProbability {
				p.connsDropped.Add(1)
				closeA.Close()
				closeB.Close()
				return
			}
			if d := f.latency(); d > 0 {
				time.Sleep(d)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				closeA.Close()
				closeB.Close()
				return
			}
		}
		if err != nil {
			closeA.Close()
			closeB.Close()
			return
		}
	}
}
