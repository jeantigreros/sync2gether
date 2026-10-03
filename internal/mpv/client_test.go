// Tests for the mpv IPC bridge: the pure observer mapping (table-driven),
// the Apply wire JSON against a fake mpv socket, and echo suppression.
package mpv

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestTrackerMapping(t *testing.T) {
	type step struct {
		kind   string // "pause", "pos" or "null"
		paused bool
		pos    float64
	}
	pause := func(p bool) step { return step{kind: "pause", paused: p} }
	pos := func(f float64) step { return step{kind: "pos", pos: f} }
	tests := []struct {
		name  string
		steps []step
		want  []Cmd
	}{
		{
			name:  "initial pause emits with zero pos",
			steps: []step{pause(true)},
			want:  []Cmd{{Name: CmdPause, Pos: 0}},
		},
		{
			name:  "initial play emits with zero pos",
			steps: []step{pause(false)},
			want:  []Cmd{{Name: CmdPlay, Pos: 0}},
		},
		{
			name:  "repeated pause is silent",
			steps: []step{pause(true), pause(true)},
			want:  []Cmd{{Name: CmdPause, Pos: 0}},
		},
		{
			name:  "pause flip carries current pos",
			steps: []step{pos(3), pause(true), pause(false)},
			want:  []Cmd{{Name: CmdSeek, Pos: 3}, {Name: CmdPause, Pos: 3}, {Name: CmdPlay, Pos: 3}},
		},
		{
			name:  "small steps are silent",
			steps: []step{pos(0.2), pos(0.5), pos(1.0)},
			want:  nil,
		},
		{
			name:  "jump emits seek",
			steps: []step{pos(0), pos(5)},
			want:  []Cmd{{Name: CmdSeek, Pos: 5}},
		},
		{
			name:  "exact threshold is silent",
			steps: []step{pos(0), pos(SeekEmitThreshold)},
			want:  nil,
		},
		{
			name:  "initial pos near zero converges silently",
			steps: []step{pos(0.3)},
			want:  nil,
		},
		{
			name:  "initial pos far from zero converges with seek",
			steps: []step{pos(30)},
			want:  []Cmd{{Name: CmdSeek, Pos: 30}},
		},
		{
			name:  "null pos is ignored",
			steps: []step{{kind: "null"}, pause(true)},
			want:  []Cmd{{Name: CmdPause, Pos: 0}},
		},
		{
			name:  "backward jump emits seek",
			steps: []step{pos(30), pos(10)},
			want:  []Cmd{{Name: CmdSeek, Pos: 30}, {Name: CmdSeek, Pos: 10}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tr tracker
			var got []Cmd
			for _, s := range tt.steps {
				var cmd Cmd
				var emit bool
				switch s.kind {
				case "pause":
					cmd, emit = tr.pauseEvent(s.paused)
				case "pos":
					cmd, emit = tr.posEvent(s.pos)
				case "null":
					continue // Idle mpv reports null: nothing to fold.
				}
				if emit {
					got = append(got, cmd)
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// fakeMpv is a scriptable mpv IPC stand-in over a temp unix socket. It
// answers every command with {"error":"success"} (get_property echoes
// pos), records received command lines, and can push property-change
// events to whichever connections subscribed via observe_property.
type fakeMpv struct {
	t   *testing.T
	ln  net.Listener
	pos float64

	mu        sync.Mutex
	lines     []string
	eventSubs []net.Conn
}

func newFakeMpv(t *testing.T, pos float64) *fakeMpv {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "mpv.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen fake mpv: %v", err)
	}
	f := &fakeMpv{t: t, ln: ln, pos: pos}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, c := range f.eventSubs {
			c.Close()
		}
	})
	f.t.Logf("fake mpv on %s", sock)
	_ = sock
	return f
}

func (f *fakeMpv) sock() string { return f.ln.Addr().String() }

func (f *fakeMpv) serve(conn net.Conn) {
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, maxLineBytes), maxLineBytes)
	sawObserve := false // This connection asked for events explicitly.
	for sc.Scan() {
		line := sc.Text()
		var req struct {
			Command   []json.RawMessage `json:"command"`
			RequestID uint64            `json:"request_id"`
		}
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue
		}
		name := ""
		if len(req.Command) > 0 {
			_ = json.Unmarshal(req.Command[0], &name)
		}
		f.mu.Lock()
		f.lines = append(f.lines, line)
		f.mu.Unlock()
		if name == "observe_property" {
			sawObserve = true
		} else if !sawObserve {
			// Like real mpv, broadcast a stray property-change event
			// to this command connection ahead of the reply. The
			// client must skip it instead of reading it as the reply
			// (regression: "command failed: " with an empty error).
			fmt.Fprintln(conn, `{"event":"property-change","id":1,"name":"pause","data":true}`)
		}
		var resp string
		if name == "get_property" {
			resp = fmt.Sprintf(`{"error":"success","data":%v,"request_id":%d}`, f.pos, req.RequestID)
		} else {
			resp = fmt.Sprintf(`{"error":"success","request_id":%d}`, req.RequestID)
		}
		fmt.Fprintln(conn, resp)
		if name == "observe_property" {
			f.mu.Lock()
			f.eventSubs = append(f.eventSubs, conn)
			f.mu.Unlock()
		}
	}
}

// blast pushes one event line to every subscribed connection.
func (f *fakeMpv) blast(line string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.eventSubs {
		fmt.Fprintln(c, line)
	}
}

// received decodes the "command" array of every line seen so far.
func (f *fakeMpv) received(t *testing.T) [][]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	var out [][]any
	for _, line := range f.lines {
		var req struct {
			Command []any `json:"command"`
		}
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			t.Fatalf("decode command line %q: %v", line, err)
		}
		out = append(out, req.Command)
	}
	return out
}

func waitForLines(t *testing.T, f *fakeMpv, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		got := len(f.lines)
		f.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d command lines", n)
}

func TestApplyWire(t *testing.T) {
	tests := []struct {
		name string
		cmd  Cmd
		pos  float64 // Fake mpv time-pos answer.
		want [][]any
	}{
		{
			name: "play seeks then unpauses",
			cmd:  Cmd{Name: CmdPlay, Pos: 5},
			pos:  0,
			want: [][]any{{"seek", 5.0, "absolute"}, {"set_property", "pause", false}},
		},
		{
			name: "seek is a bare absolute seek",
			cmd:  Cmd{Name: CmdSeek, Pos: 7},
			pos:  0,
			want: [][]any{{"seek", 7.0, "absolute"}},
		},
		{
			name: "pause near target skips the seek",
			cmd:  Cmd{Name: CmdPause, Pos: 10.2},
			pos:  10.0,
			want: [][]any{{"get_property", "time-pos"}, {"set_property", "pause", true}},
		},
		{
			name: "pause far from target seeks first",
			cmd:  Cmd{Name: CmdPause, Pos: 20},
			pos:  10.0,
			want: [][]any{{"get_property", "time-pos"}, {"seek", 20.0, "absolute"}, {"set_property", "pause", true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeMpv(t, tt.pos)
			cl, err := Dial(f.sock())
			if err != nil {
				t.Fatalf("dial fake mpv: %v", err)
			}
			defer cl.Close()
			if err := cl.Apply(tt.cmd); err != nil {
				t.Fatalf("Apply(%v): %v", tt.cmd, err)
			}
			waitForLines(t, f, len(tt.want))
			if got := f.received(t); !reflect.DeepEqual(got, tt.want) {
				raw, _ := json.Marshal(got)
				wantRaw, _ := json.Marshal(tt.want)
				t.Fatalf("wire mismatch:\n got %s\nwant %s", raw, wantRaw)
			}
		})
	}
}

func TestApplyUnknown(t *testing.T) {
	f := newFakeMpv(t, 0)
	cl, err := Dial(f.sock())
	if err != nil {
		t.Fatalf("dial fake mpv: %v", err)
	}
	defer cl.Close()
	if err := cl.Apply(Cmd{Name: "rewind", Pos: 1}); err == nil {
		t.Fatal("Apply(unknown) should fail")
	}
}

// TestCommandConnSkipsEvents is the regression test for the empty-error
// failure seen against real mpv: every reply on the command connection is
// preceded by a stray property-change event (see fakeMpv.serve), and Apply
// must skip it instead of mistaking it for the reply.
func TestCommandConnSkipsEvents(t *testing.T) {
	f := newFakeMpv(t, 20.0)
	cl, err := Dial(f.sock())
	if err != nil {
		t.Fatalf("dial fake mpv: %v", err)
	}
	defer cl.Close()
	for _, cmd := range []Cmd{
		{Name: CmdSeek, Pos: 20},
		{Name: CmdPause, Pos: 20},
		{Name: CmdPlay, Pos: 20},
	} {
		if err := cl.Apply(cmd); err != nil {
			t.Fatalf("Apply(%v) tripped over a stray event: %v", cmd, err)
		}
	}
}

func TestObserveMapsEvents(t *testing.T) {
	f := newFakeMpv(t, 0)
	cl, err := Dial(f.sock())
	if err != nil {
		t.Fatalf("dial fake mpv: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmds, err := cl.Observe(ctx)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	waitForLines(t, f, 2) // Both observe_property requests registered.
	f.blast(`{"event":"property-change","id":1,"name":"pause","data":true}`)
	f.blast(`{"event":"property-change","id":2,"name":"time-pos","data":10.0}`)
	f.blast(`{"event":"property-change","id":2,"name":"time-pos","data":10.4}`)
	f.blast(`{"event":"property-change","id":2,"name":"time-pos","data":30.0}`)
	f.blast(`{"event":"property-change","id":1,"name":"pause","data":false}`)
	f.blast(`{"event":"property-change","id":1,"name":"pause","data":false}`)
	f.blast(`{"event":"property-change","id":2,"name":"time-pos","data":null}`)
	want := []Cmd{
		{Name: CmdPause, Pos: 0},
		{Name: CmdSeek, Pos: 10.0},
		{Name: CmdSeek, Pos: 30.0},
		{Name: CmdPlay, Pos: 30.0},
	}
	var got []Cmd
	timeout := time.After(5 * time.Second)
	for len(got) < len(want) {
		select {
		case c, ok := <-cmds:
			if !ok {
				t.Fatalf("observer closed early with %v", got)
			}
			got = append(got, c)
		case <-timeout:
			t.Fatalf("timed out waiting for commands, got %v", got)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	select {
	case c := <-cmds:
		t.Fatalf("spurious extra command: %v", c)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestEchoSuppression(t *testing.T) {
	f := newFakeMpv(t, 0)
	cl, err := Dial(f.sock())
	if err != nil {
		t.Fatalf("dial fake mpv: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmds, err := cl.Observe(ctx)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	waitForLines(t, f, 2)
	// Applying arms suppression: the property change our own seek causes
	// on the real mpv must not come back as an emission.
	if err := cl.Apply(Cmd{Name: CmdSeek, Pos: 30}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	f.blast(`{"event":"property-change","id":2,"name":"time-pos","data":30.5}`)
	select {
	case c := <-cmds:
		t.Fatalf("suppression failed, re-emitted own apply: %v", c)
	case <-time.After(150 * time.Millisecond):
	}
	// After the window the observer reports again, with the baseline
	// tracked through the silence (30.5), so 60.0 is a jump, not a drift.
	time.Sleep(SuppressWindow)
	f.blast(`{"event":"property-change","id":2,"name":"time-pos","data":60.0}`)
	select {
	case c := <-cmds:
		if !reflect.DeepEqual(c, Cmd{Name: CmdSeek, Pos: 60.0}) {
			t.Fatalf("got %v, want seek 60", c)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("observer stayed silent after the suppression window")
	}
}
