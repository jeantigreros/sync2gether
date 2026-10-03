// Command syncpoc is a minimal proof-of-concept for synchronizing
// playback between two PCs. Both sides send a heartbeat about once
// per second (see internal/syncmsg) and log each received message
// with its apparent latency from SentAtUnixMilli.
//
// Usage:
//
//	syncpoc serve <addr>                  listen on plain TCP (offline default)
//	syncpoc dial <addr>                   connect over plain TCP
//	syncpoc serve-tailcat <port>          listen over tailcat (P2P WireGuard + DERP)
//	syncpoc dial-tailcat <tc-addr> <port> connect over tailcat
//
// The plain-TCP modes exercise framing/heartbeat logic offline. The
// tailcat modes are the real P2P transport (serve prints an address
// that dial consumes). Playback is driven from stdin, on either side:
//
//	play        resume from the current position
//	pause       freeze at the current position
//	seek <secs> jump (preserves playing/paused state)
//	status      print local playback state
//	quit        hang up (Ctrl+C and Ctrl+D do the same)
//
// The message layer (runPeer) is
// transport-agnostic: switching fully to tailcat only means calling
// listenTailcat/dialTailcat (the marked swap points) instead of
// listenPlain/dialPlain.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tailscale/tailcat"

	"sync2gether/internal/syncmsg"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "syncpoc:", err)
		os.Exit(1)
	}
}

func usage() error {
	return fmt.Errorf("usage:\n" +
		"  syncpoc serve <addr>\n" +
		"  syncpoc dial <addr>\n" +
		"  syncpoc serve-tailcat <port>\n" +
		"  syncpoc dial-tailcat <tc-addr> <port>")
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	hub := &cmdHub{}
	go readStdin(hub, stop)

	switch args[0] {
	case "serve":
		if len(args) != 2 {
			return usage()
		}
		printHelp()
		ln, err := listenPlain(args[1]) // TAILCAT SWAP POINT: use listenTailcat for P2P.
		if err != nil {
			return err
		}
		defer ln.Close()
		closeOnDone(ctx, ln) // Ctrl+C unblocks Accept.
		return serveLoop(ctx, ln, hub)
	case "dial":
		if len(args) != 2 {
			return usage()
		}
		printHelp()
		conn, err := dialPlain(ctx, args[1]) // TAILCAT SWAP POINT: use dialTailcat for P2P.
		if err != nil {
			return err
		}
		runPeer(ctx, conn, "dial", hub)
		return nil
	case "serve-tailcat":
		if len(args) != 2 {
			return usage()
		}
		printHelp()
		port, err := parsePort(args[1])
		if err != nil {
			return err
		}
		srv, ln, err := listenTailcat(ctx, port)
		if err != nil {
			return err
		}
		defer srv.Close()
		defer ln.Close()
		closeOnDone(ctx, ln) // Ctrl+C unblocks Accept.
		return serveLoop(ctx, ln, hub)
	case "dial-tailcat":
		if len(args) != 3 {
			return usage()
		}
		printHelp()
		port, err := parsePort(args[2])
		if err != nil {
			return err
		}
		conn, err := dialTailcat(ctx, args[1], port)
		if err != nil {
			return err
		}
		runPeer(ctx, conn, "dial", hub)
		return nil
	default:
		return usage()
	}
}

// listenPlain opens a plain TCP listener. Offline-friendly; this is the
// tailcat swap point on the serving side (see listenTailcat).
func listenPlain(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	fmt.Println("listening on", ln.Addr())
	return ln, nil
}

// dialPlain connects over plain TCP. Offline-friendly; this is the
// tailcat swap point on the dialing side (see dialTailcat).
func dialPlain(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return conn, nil
}

// listenTailcat starts a tailcat server and listens for P2P TCP streams
// on the given port. It prints the tailcat address the peer must pass
// to dial-tailcat. No Tailscale account is needed (control-plane-free),
// but both sides need DERP bootstrap connectivity for peer discovery.
func listenTailcat(ctx context.Context, port uint16) (*tailcat.Server, net.Listener, error) {
	srv := &tailcat.Server{}
	if err := srv.Start(); err != nil {
		return nil, nil, fmt.Errorf("tailcat server start: %w", err)
	}
	ln, err := srv.Listen(ctx, "tcp", net.JoinHostPort("", strconv.Itoa(int(port))))
	if err != nil {
		srv.Close()
		return nil, nil, fmt.Errorf("tailcat listen: %w", err)
	}
	fmt.Println("tailcat address (share with peer):", srv.TailcatAddr())
	return srv, ln, nil
}

// dialTailcat connects to a tailcat server's TCP listener on the given
// port. The client is intentionally process-lifetime: closing it would
// drop the connection, so it lives until the POC exits.
func dialTailcat(ctx context.Context, tcAddr string, port uint16) (net.Conn, error) {
	if _, err := tailcat.ParseAddr(tailcat.Addr(tcAddr)); err != nil {
		return nil, fmt.Errorf("invalid tailcat address: %w", err)
	}
	cl := tailcat.NewClient(tailcat.Addr(tcAddr))
	conn, err := cl.DialTCPPort(ctx, port)
	if err != nil {
		cl.Close()
		return nil, fmt.Errorf("tailcat dial: %w", err)
	}
	return conn, nil
}

// closeOnDone closes c once ctx is cancelled so that a blocking Accept or
// Read unblocks and the program exits promptly on Ctrl+C. Closing twice is
// safe: the redundant error is ignored.
func closeOnDone(ctx context.Context, c io.Closer) {
	go func() {
		<-ctx.Done()
		fmt.Fprintln(os.Stderr, "\ninterrupted, closing...")
		_ = c.Close()
	}()
}

// serveLoop accepts connections until interrupted, running the sync
// exchange on each one.
func serveLoop(ctx context.Context, ln net.Listener, hub *cmdHub) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return fmt.Errorf("accept: %w", err)
			}
		}
		go runPeer(ctx, conn, "serve", hub)
	}
}

// runPeer runs the sync exchange on conn: a sendLoop owns the local playback
// state and writes heartbeats plus stdin-driven events, while this loop reads
// and keeps a mirror of the remote playback state. Both sides run the same
// symmetric logic, so either side can drive playback.
func runPeer(ctx context.Context, conn net.Conn, role string, hub *cmdHub) {
	defer conn.Close()
	fmt.Printf("[%s] connected: local=%s remote=%s\n", role, conn.LocalAddr(), conn.RemoteAddr())
	closeOnDone(ctx, conn) // Ctrl+C unblocks the Scan loop below.

	cmds := hub.subscribe()
	defer hub.unsubscribe(cmds)
	go sendLoop(ctx, conn, role, cmds)

	var rPos float64
	var rPlaying bool
	sc := syncmsg.NewScanner(conn)
	for sc.Scan() {
		m := sc.Message()
		latency := time.Now().UnixMilli() - m.SentAtUnixMilli
		switch m.Type {
		case syncmsg.TypePlay:
			rPlaying, rPos = true, m.PositionSeconds
			fmt.Printf("[%s] remote event: play from %.1fs (latency=%dms)\n", role, rPos, latency)
		case syncmsg.TypePause:
			rPlaying, rPos = false, m.PositionSeconds
			fmt.Printf("[%s] remote event: pause at %.1fs (latency=%dms)\n", role, rPos, latency)
		case syncmsg.TypeSeek:
			rPos = m.PositionSeconds
			fmt.Printf("[%s] remote event: seek to %.1fs (latency=%dms)\n", role, rPos, latency)
		case syncmsg.TypeHeartbeat:
			rPos = m.PositionSeconds
			state := "paused"
			if rPlaying {
				state = "playing"
			}
			fmt.Printf("[%s] remote hb: pos=%.1fs %s (latency=%dms)\n", role, rPos, state, latency)
		default:
			fmt.Printf("[%s] remote: unknown type %q\n", role, m.Type)
		}
	}
	if err := sc.Err(); err != nil {
		if ctx.Err() != nil {
			fmt.Printf("[%s] stopped\n", role)
			return
		}
		fmt.Printf("[%s] stream ended with error: %v\n", role, err)
		return
	}
	fmt.Printf("[%s] stream closed by peer\n", role)
}

// peerCmd is one local playback command parsed from stdin.
type peerCmd struct {
	name string
	pos  float64 // only for seek.
}

// cmdHub broadcasts stdin commands to every active peer's sendLoop. Delivery
// is best-effort: a slow or gone peer drops commands instead of stalling the
// rest. Good enough for a 1:1 POC.
type cmdHub struct {
	mu    sync.Mutex
	peers map[chan peerCmd]struct{}
}

func (h *cmdHub) subscribe() chan peerCmd {
	ch := make(chan peerCmd, 16)
	h.mu.Lock()
	if h.peers == nil {
		h.peers = make(map[chan peerCmd]struct{})
	}
	h.peers[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *cmdHub) unsubscribe(ch chan peerCmd) {
	h.mu.Lock()
	delete(h.peers, ch)
	h.mu.Unlock()
}

func (h *cmdHub) broadcast(c peerCmd) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.peers {
		select {
		case ch <- c:
		default:
		}
	}
}

func printHelp() {
	fmt.Println("commands: play | pause | seek <secs> | status | quit  (Ctrl+C / Ctrl+D also stop)")
}

// readStdin parses playback commands from stdin and broadcasts them to all
// peers. quit (or EOF: Ctrl+D, or piped input running out) cancels the
// context, stopping everything through the same path as Ctrl+C.
func readStdin(hub *cmdHub, stop context.CancelFunc) {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 4096), 4096)
	for sc.Scan() {
		fields := strings.Fields(strings.TrimSpace(sc.Text()))
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "play":
			hub.broadcast(peerCmd{name: "play"})
		case "pause":
			hub.broadcast(peerCmd{name: "pause"})
		case "seek":
			if len(fields) != 2 {
				fmt.Fprintln(os.Stderr, "usage: seek <secs>")
				continue
			}
			n, err := strconv.ParseFloat(fields[1], 64)
			if err != nil || n < 0 {
				fmt.Fprintln(os.Stderr, "invalid seek position")
				continue
			}
			hub.broadcast(peerCmd{name: "seek", pos: n})
		case "status":
			hub.broadcast(peerCmd{name: "status"})
		case "quit":
			stop()
			return
		case "help":
			printHelp()
		default:
			fmt.Fprintln(os.Stderr, "unknown command (try: play pause seek status quit)")
		}
	}
	stop()
}

// sendLoop owns this peer's local playback state and is the only writer to
// conn: a 1s ticker emits heartbeats while stdin commands arrive as events.
// The position is clock-based (elapsed wall time while playing), so pause
// freezes it without drift and commands landing between ticks stay accurate.
func sendLoop(ctx context.Context, conn net.Conn, role string, cmds <-chan peerCmd) {
	var pos float64
	playing := false
	last := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	emit := func(typ string) bool {
		m := syncmsg.Message{Type: typ, PositionSeconds: pos, SentAtUnixMilli: time.Now().UnixMilli()}
		if err := syncmsg.Encode(conn, m); err != nil {
			return false
		}
		return true
	}
	advance := func() {
		now := time.Now()
		if playing {
			pos += now.Sub(last).Seconds()
		}
		last = now
	}
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if playing {
				pos += now.Sub(last).Seconds()
			}
			last = now
			if !emit(syncmsg.TypeHeartbeat) {
				return
			}
		case c := <-cmds:
			advance()
			switch c.name {
			case "play":
				playing = true
				fmt.Printf("[%s] local: play from %.1fs\n", role, pos)
				if !emit(syncmsg.TypePlay) {
					return
				}
			case "pause":
				playing = false
				fmt.Printf("[%s] local: pause at %.1fs\n", role, pos)
				if !emit(syncmsg.TypePause) {
					return
				}
			case "seek":
				pos = c.pos // playing/paused state preserved.
				fmt.Printf("[%s] local: seek to %.1fs\n", role, pos)
				if !emit(syncmsg.TypeSeek) {
					return
				}
			case "status":
				state := "paused"
				if playing {
					state = "playing"
				}
				fmt.Printf("[%s] local: %s at %.1fs\n", role, state, pos)
			}
		}
	}
}

func parsePort(s string) (uint16, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return uint16(n), nil
}
