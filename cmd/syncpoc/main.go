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
// that dial consumes). The message layer (runPeer) is
// transport-agnostic: switching fully to tailcat only means calling
// listenTailcat/dialTailcat (the marked swap points) instead of
// listenPlain/dialPlain.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
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

	switch args[0] {
	case "serve":
		if len(args) != 2 {
			return usage()
		}
		ln, err := listenPlain(args[1]) // TAILCAT SWAP POINT: use listenTailcat for P2P.
		if err != nil {
			return err
		}
		defer ln.Close()
		return serveLoop(ctx, ln)
	case "dial":
		if len(args) != 2 {
			return usage()
		}
		conn, err := dialPlain(ctx, args[1]) // TAILCAT SWAP POINT: use dialTailcat for P2P.
		if err != nil {
			return err
		}
		runPeer(ctx, conn, "dial")
		return nil
	case "serve-tailcat":
		if len(args) != 2 {
			return usage()
		}
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
		return serveLoop(ctx, ln)
	case "dial-tailcat":
		if len(args) != 3 {
			return usage()
		}
		port, err := parsePort(args[2])
		if err != nil {
			return err
		}
		conn, err := dialTailcat(ctx, args[1], port)
		if err != nil {
			return err
		}
		runPeer(ctx, conn, "dial")
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

// serveLoop accepts connections until interrupted, running the sync
// exchange on each one.
func serveLoop(ctx context.Context, ln net.Listener) error {
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
		go runPeer(ctx, conn, "serve")
	}
}

// runPeer runs the sync exchange on conn: it sends a heartbeat about
// once per second and logs every received message with its apparent
// latency. Both sides run the same symmetric logic.
func runPeer(ctx context.Context, conn net.Conn, role string) {
	defer conn.Close()
	fmt.Printf("[%s] connected: local=%s remote=%s\n", role, conn.LocalAddr(), conn.RemoteAddr())
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var pos float64
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				pos++
				m := syncmsg.Message{Type: syncmsg.TypeHeartbeat, PositionSeconds: pos, SentAtUnixMilli: now.UnixMilli()}
				if err := syncmsg.Encode(conn, m); err != nil {
					return
				}
			}
		}
	}()
	sc := syncmsg.NewScanner(conn)
	for sc.Scan() {
		m := sc.Message()
		latency := time.Now().UnixMilli() - m.SentAtUnixMilli
		fmt.Printf("[%s] recv type=%s pos=%.1fs latency=%dms\n", role, m.Type, m.PositionSeconds, latency)
	}
	if err := sc.Err(); err != nil {
		fmt.Printf("[%s] stream ended with error: %v\n", role, err)
		return
	}
	fmt.Printf("[%s] stream closed by peer\n", role)
}

func parsePort(s string) (uint16, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return uint16(n), nil
}
