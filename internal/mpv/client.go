// Package mpv bridges a local mpv player with the sync peer hub.
//
// mpv exposes a JSON IPC protocol over a unix socket (see
// --input-ipc-server): commands and events are newline-delimited JSON
// objects. This client has two roles:
//
//   - Observe watches the "pause" and "time-pos" properties and translates
//     property-change events into local playback commands (play, pause and
//     seek with position). Smooth playback progress between observations is
//     intentionally not reported (only jumps larger than SeekEmitThreshold
//     count as seeks); the periodic heartbeat already carries the smooth
//     position.
//   - Apply renders a remote playback command on the local mpv. A seek
//     never flips the pause state, matching the sync protocol semantics
//     (seek preserves playing/paused).
//
// Echo suppression: applying a remote command moves the local mpv, which
// the observer would otherwise pick up and re-broadcast forever
// (ping-pong). Apply arms a short suppression window (SuppressWindow)
// during which Observe still tracks state but drops emissions, so the
// baseline stays exact and the loop dies after one hop.
package mpv

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Local playback command names. They intentionally match the sync wire
// vocabulary (play/pause/seek) so the caller can forward them 1:1.
const (
	CmdPlay  = "play"
	CmdPause = "pause"
	CmdSeek  = "seek"
)

// Numeric ids used in observe_property requests. mpv echoes them back on
// every property-change event; the property names are matched as well.
const (
	observePauseID   = 1
	observeTimePosID = 2
)

const (
	// SeekEmitThreshold is the minimum jump, in seconds, between two
	// consecutive time-pos observations that counts as a user seek.
	// Normal playback advances far less between observations; the
	// heartbeat covers that smooth progress.
	SeekEmitThreshold = 1.5
	// PauseSeekEpsilon bounds the "already there" check when applying a
	// remote pause: if the local position is within this many seconds of
	// the target, the pre-seek is skipped and mpv just pauses, avoiding a
	// visible stutter on an already-correct pause.
	PauseSeekEpsilon = 0.5
	// SuppressWindow is how long Observe drops events after Apply. It only
	// needs to cover the mpv round-trip of our own applied commands.
	SuppressWindow = 300 * time.Millisecond
	// maxLineBytes bounds a single IPC line. mpv events are tiny; 64 KiB
	// keeps a misbehaving peer from forcing unbounded buffering.
	maxLineBytes = 64 * 1024
)

// Cmd is a local playback command, either observed from mpv or to apply
// to it. Pos is the mpv time-pos in seconds: the current position for
// play/pause, the destination for seek.
type Cmd struct {
	Name string
	Pos  float64
}

// wireRequest is one JSON IPC command line.
type wireRequest struct {
	Command   []any  `json:"command"`
	RequestID uint64 `json:"request_id,omitempty"`
}

// wireResponse is one JSON IPC reply line.
type wireResponse struct {
	Error     string          `json:"error"`
	Data      json.RawMessage `json:"data,omitempty"`
	RequestID uint64          `json:"request_id,omitempty"`
}

// wireEvent is one JSON IPC async event line.
type wireEvent struct {
	Event string          `json:"event"`
	ID    int             `json:"id"`
	Name  string          `json:"name"`
	Data  json.RawMessage `json:"data"`
}

// Client is an mpv IPC connection pair: one connection streams
// property-change events (Observe) while the other runs synchronous
// request/response commands (Apply). Two connections keep command replies
// from interleaving with the event stream, so Apply never has to
// demultiplex.
type Client struct {
	obsConn   net.Conn
	cmdConn   net.Conn
	cmdReader *bufio.Reader

	mu     sync.Mutex // Serializes all writes and cmd request/response pairs.
	nextID atomic.Uint64

	suppressFlag  atomic.Bool
	suppressUntil atomic.Int64 // UnixNano deadline of the drop window.
}

// Dial connects to the mpv IPC socket at path.
func Dial(path string) (*Client, error) {
	obs, err := net.Dial("unix", path)
	if err != nil {
		return nil, fmt.Errorf("mpv: dial %s: %w", path, err)
	}
	cmd, err := net.Dial("unix", path)
	if err != nil {
		obs.Close()
		return nil, fmt.Errorf("mpv: dial %s: %w", path, err)
	}
	return &Client{obsConn: obs, cmdConn: cmd, cmdReader: bufio.NewReader(cmd)}, nil
}

// Close releases both IPC connections.
func (c *Client) Close() error {
	if err := c.obsConn.Close(); err != nil {
		_ = c.cmdConn.Close()
		return fmt.Errorf("mpv: close: %w", err)
	}
	if err := c.cmdConn.Close(); err != nil {
		return fmt.Errorf("mpv: close: %w", err)
	}
	return nil
}

// Observe subscribes to the pause and time-pos properties and translates
// property-change events into commands on the returned channel. The
// channel closes when ctx is done or the socket breaks. Emissions stop
// while echo suppression is armed (see Apply), but state tracking does
// not, so the baseline stays exact across the window.
func (c *Client) Observe(ctx context.Context) (<-chan Cmd, error) {
	if err := c.sendObserve(observePauseID, "pause"); err != nil {
		return nil, err
	}
	if err := c.sendObserve(observeTimePosID, "time-pos"); err != nil {
		return nil, err
	}
	out := make(chan Cmd, 16)
	go c.watch(ctx, out)
	return out, nil
}

func (c *Client) sendObserve(id int, name string) error {
	req := wireRequest{Command: []any{"observe_property", id, name}, RequestID: c.nextID.Add(1)}
	raw, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("mpv: marshal observe %s: %w", name, err)
	}
	raw = append(raw, '\n')
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.obsConn.Write(raw); err != nil {
		return fmt.Errorf("mpv: observe %s: %w", name, err)
	}
	return nil
}

func (c *Client) watch(ctx context.Context, out chan<- Cmd) {
	defer close(out)
	// Unblock the scanner below on cancel; Close is idempotent enough
	// that the later Client.Close stays safe.
	stop := context.AfterFunc(ctx, func() { c.obsConn.Close() })
	defer stop()
	var tr tracker
	sc := bufio.NewScanner(c.obsConn)
	sc.Buffer(make([]byte, maxLineBytes), maxLineBytes)
	for sc.Scan() {
		var ev wireEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			continue // Replies and malformed lines carry no state.
		}
		if ev.Event != "property-change" {
			continue // Replies to observe_property land here too.
		}
		var cmd Cmd
		var emit bool
		switch ev.Name {
		case "pause":
			var paused bool
			if err := json.Unmarshal(ev.Data, &paused); err != nil {
				continue
			}
			cmd, emit = tr.pauseEvent(paused)
		case "time-pos":
			var pos *float64
			if len(ev.Data) > 0 {
				if err := json.Unmarshal(ev.Data, &pos); err != nil {
					continue
				}
			}
			if pos == nil {
				continue // Idle mpv reports null: no position to track.
			}
			cmd, emit = tr.posEvent(*pos)
		default:
			continue
		}
		if !emit || c.suppressed() {
			continue // State above is tracked even while suppressed.
		}
		select {
		case out <- cmd:
		case <-ctx.Done():
			return
		}
	}
}

// Apply renders cmd on the local mpv. Suppression is armed first, so the
// observer drops the property changes our own commands are about to
// cause instead of re-broadcasting them.
//
//   - play: absolute seek to the event position, then unpause.
//   - pause: absolute seek first, but only when the local position is
//     further than PauseSeekEpsilon from the target, then pause.
//   - seek: absolute seek; mpv keeps the pause state untouched, which
//     matches the sync semantics (seek preserves playing/paused).
func (c *Client) Apply(cmd Cmd) error {
	c.suppress()
	switch cmd.Name {
	case CmdPlay:
		if _, err := c.roundTrip("seek", cmd.Pos, "absolute"); err != nil {
			return err
		}
		_, err := c.roundTrip("set_property", "pause", false)
		return err
	case CmdPause:
		if math.Abs(c.timePos()-cmd.Pos) > PauseSeekEpsilon {
			if _, err := c.roundTrip("seek", cmd.Pos, "absolute"); err != nil {
				return err
			}
		}
		_, err := c.roundTrip("set_property", "pause", true)
		return err
	case CmdSeek:
		_, err := c.roundTrip("seek", cmd.Pos, "absolute")
		return err
	default:
		return fmt.Errorf("mpv: unknown command %q", cmd.Name)
	}
}

// roundTrip sends one command and reads its reply. The mutex is held for
// the whole pair, so concurrent Apply calls stay sequential and replies
// always match the command just sent.
//
// mpv broadcasts async property-change events to EVERY IPC client,
// including this command connection, so event lines can arrive between the
// command and its reply. The observer owns a separate connection and sees
// every event, so non-reply lines are skipped here instead of being
// mistaken for the reply (which used to surface as bizarre failures with
// an empty error string).
func (c *Client) roundTrip(command ...any) (wireResponse, error) {
	var res wireResponse
	req := wireRequest{Command: command, RequestID: c.nextID.Add(1)}
	raw, err := json.Marshal(req)
	if err != nil {
		return res, fmt.Errorf("mpv: marshal %v: %w", command, err)
	}
	raw = append(raw, '\n')
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.cmdConn.Write(raw); err != nil {
		return res, fmt.Errorf("mpv: write %v: %w", command, err)
	}
	for {
		line, err := c.cmdReader.ReadBytes('\n')
		if err != nil {
			return res, fmt.Errorf("mpv: read reply to %v: %w", command, err)
		}
		var probe struct {
			Event string `json:"event"`
		}
		if err := json.Unmarshal(line, &probe); err != nil {
			return res, fmt.Errorf("mpv: decode reply to %v: %w", command, err)
		}
		if probe.Event != "" {
			continue // Async event for the observer's connection to handle.
		}
		if err := json.Unmarshal(line, &res); err != nil {
			return res, fmt.Errorf("mpv: decode reply to %v: %w", command, err)
		}
		if res.Error != "success" {
			return res, fmt.Errorf("mpv: command %v failed: %s", command, res.Error)
		}
		return res, nil
	}
}

// timePos queries the current mpv position as a best-effort anchor for
// the pause already-there check. A missing value (idle, no file) or any
// read failure reads as 0: the pause itself is applied right after and
// surfaces real transport errors there.
func (c *Client) timePos() float64 {
	res, err := c.roundTrip("get_property", "time-pos")
	if err != nil || len(res.Data) == 0 {
		return 0
	}
	var pos *float64
	if err := json.Unmarshal(res.Data, &pos); err != nil || pos == nil {
		return 0
	}
	return *pos
}

// suppress arms the echo-suppression window.
func (c *Client) suppress() {
	c.suppressFlag.Store(true)
	c.suppressUntil.Store(time.Now().Add(SuppressWindow).UnixNano())
}

// suppressed reports whether emissions are currently dropped. Once the
// deadline passes the flag clears itself.
func (c *Client) suppressed() bool {
	if !c.suppressFlag.Load() {
		return false
	}
	if time.Now().UnixNano() < c.suppressUntil.Load() {
		return true
	}
	c.suppressFlag.Store(false)
	return false
}

// tracker folds raw property observations into playback commands. It is
// pure state (no I/O) so the mapping is unit-testable on its own.
type tracker struct {
	havePause bool
	paused    bool
	havePos   bool
	lastPos   float64
}

func (t *tracker) pos() float64 {
	if t.havePos {
		return t.lastPos
	}
	return 0
}

// pauseEvent folds one pause observation. Repeats are silent; flips emit
// play/pause with the current time-pos. The very first observation is
// emitted too: a fresh bridge must converge the peer (a missed initial
// play would leave it out of sync until the next local action), while a
// redundant pause is a no-op on the other side.
func (t *tracker) pauseEvent(paused bool) (Cmd, bool) {
	first := !t.havePause
	changed := t.havePause && t.paused != paused
	t.paused, t.havePause = paused, true
	if !first && !changed {
		return Cmd{}, false
	}
	if paused {
		return Cmd{Name: CmdPause, Pos: t.pos()}, true
	}
	return Cmd{Name: CmdPlay, Pos: t.pos()}, true
}

// posEvent folds one time-pos observation. Only jumps larger than
// SeekEmitThreshold emit a seek; smooth progress stays silent because the
// heartbeat already carries it. The first observation converges a
// mid-playback connect, but stays quiet around zero so every fresh start
// does not broadcast a seek-to-zero.
func (t *tracker) posEvent(pos float64) (Cmd, bool) {
	if !t.havePos {
		t.lastPos, t.havePos = pos, true
		if math.Abs(pos) > SeekEmitThreshold {
			return Cmd{Name: CmdSeek, Pos: pos}, true
		}
		return Cmd{}, false
	}
	jump := math.Abs(pos - t.lastPos)
	t.lastPos = pos
	if jump > SeekEmitThreshold {
		return Cmd{Name: CmdSeek, Pos: pos}, true
	}
	return Cmd{}, false
}
