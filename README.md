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

Each side spawns its own mpv and bridges it. One side hosts, the other
joins with the printed address. Both sides are symmetric afterwards:
either one can drive playback.

```bash
# PC1 (same video file on both machines)
syncpoc host movie.mp4 [--port N]
# prints: tailcat address (share with peer): <addr>

# PC2
syncpoc join <addr> movie.mp4 [--port N]
```
Both sides must use the same port (default 8080).

Anything you do in mpv (play/pause/seek) is broadcast to the peer; remote
events are applied to your mpv. A 1 Hz heartbeat carries the position each
way, and every received message is logged with its apparent latency.

`syncpoc movie.mp4` alone just plays locally, without sync.

## Shell completion

TAB-completion for subcommands, file arguments and `--port` lives in
`completions/` (bash, zsh, fish):

```bash
# bash, this shell only (or append to ~/.bashrc)
source completions/syncpoc.bash
# zsh: cp completions/syncpoc.zsh ~/.zsh/completions/_syncpoc  ($fpath)
# fish: cp completions/syncpoc.fish ~/.config/fish/completions/
```

The scripts complete the `syncpoc` name. If you run the binary as
`./main`, either rename it (`go build -o syncpoc ./cmd/syncpoc`) or, in
bash, register it too: `complete -F _syncpoc ./main`.

Playback can also be driven from the terminal on either side:
`play`, `pause`, `seek <secs>`, `status`, `quit`
(`Ctrl+C` and `Ctrl+D` stop too).

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
- Transport is always the tailcat P2P tunnel: direct UDP when NAT
  traversal succeeds, DERP relay as fallback. Expect ~1–5 ms direct,
  ~20–80 ms relayed.
- Conflicts (both sides act at once) resolve last-writer-wins by sender
  timestamp. Good enough for two people watching a movie.
