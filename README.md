# sync2gether

Watch the same video on two computers, in sync. Press play/pause/seek on
either side and the other side follows, transported over a Tailscale-style
P2P tunnel ([tailcat](https://github.com/tailscale/tailcat)) with plain TCP
available for offline testing.

## Prerequisites

- Go 1.27.1+ (the toolchain resolves itself via `go.mod`)
- `mpv` (only for player mode; keyboard mode needs nothing else)
- For the `*-tailcat` modes: UDP/DERP connectivity for peer discovery
  (no Tailscale account needed). First connect takes a few seconds.

## Build

```bash
go build ./...
go test ./...
```

This produces no installed binary; run from source or
`go build -o syncpoc ./cmd/syncpoc`.

## Usage

One side serves, the other dials. Both sides are symmetric afterwards:
either one can drive playback.

### Keyboard mode (no player)

```bash
# PC1
syncpoc serve 0.0.0.0:4433
# PC2
syncpoc dial <pc1-ip>:4433
```

Type commands on either side (stdin):

```
play        resume from the current position
pause       freeze at the current position
seek <secs> jump (keeps playing/paused state)
status      print local playback state
quit        hang up (Ctrl+C and Ctrl+D do the same)
```

A 1 Hz heartbeat carries the position each way; every received message is
logged with its apparent latency.

### Player mode (mpv follows mpv)

```bash
# PC1 (same video file on both machines)
mpv --input-ipc-server=/tmp/mpv.sock movie.mp4
syncpoc serve 0.0.0.0:4433 --mpv-socket /tmp/mpv.sock

# PC2
mpv --input-ipc-server=/tmp/mpv.sock movie.mp4
syncpoc dial <pc1-ip>:4433 --mpv-socket /tmp/mpv.sock
```

Anything you do in mpv (play/pause/seek) is broadcast; remote events are
applied to your mpv. Echo suppression keeps applied commands from bouncing
back. Heartbeats are mirror-only (logged, never applied to mpv).

### P2P mode (tailcat, no LAN needed)

Same as above but over an end-to-end encrypted P2P tunnel instead of raw
TCP. The server prints a one-time address you share out of band:

```bash
# PC1
syncpoc serve-tailcat 4433 [--mpv-socket /tmp/mpv.sock]
# prints: tailcat address (share with peer): <addr>

# PC2
syncpoc dial-tailcat <addr> 4433 [--mpv-socket /tmp/mpv.sock]
```

Direct UDP when NAT traversal succeeds, DERP relay as fallback. Expect
~1–5 ms direct, ~20–80 ms relayed.

## Protocol

One JSON object per line over the stream (`internal/syncmsg`):

```json
{"type":"heartbeat","position_seconds":12.5,"sent_at_unix_milli":1721234567890}
```

Types: `play` (resume from position), `pause` (freeze at position),
`seek` (jump, preserves playing/paused), `heartbeat` (position keepalive).

## Layout

```
cmd/syncpoc/        the POC binary (transport-agnostic message layer)
internal/syncmsg/   framing codec (JSON + newline) with tests
internal/mpv/       mpv IPC client (observe/apply, echo suppression) with tests
third_party/tailcat read-only upstream reference, never committed (see .gitignore)
odd/                local working docs, never committed (see .gitignore)
```

## Notes

- `third_party/tailcat` is a reference clone. The real dependency comes
  from the module proxy (`github.com/tailscale/tailcat v0.7.0` in `go.mod`).
- If both PCs already share a Tailscale tailnet, plain `serve`/`dial` over
  tailnet IPs is the simplest option; tailcat is for when they don't.
- Conflicts (both sides act at once) resolve last-writer-wins by sender
  timestamp. Good enough for two people watching a movie.
