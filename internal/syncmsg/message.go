// Package syncmsg defines the playback synchronization message framing
// used by the syncpoc prototype.
//
// Every message is a single JSON object terminated by "\n", so a plain TCP
// stream (such as a tailcat TCP connection) can carry a sequence of messages
// without any additional framing:
//
//	{"type":"heartbeat","position_seconds":12.5,"sent_at_unix_milli":1721234567890}\n
//
// Message types:
//
//	play      - resume playback from PositionSeconds.
//	pause     - pause playback at PositionSeconds.
//	seek      - jump to PositionSeconds (the destination).
//	heartbeat - periodic keepalive carrying the current PositionSeconds.
package syncmsg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// Playback event types carried in Message.Type.
const (
	TypePlay      = "play"
	TypePause     = "pause"
	TypeSeek      = "seek"
	TypeHeartbeat = "heartbeat"
)

// MaxLineBytes bounds a single framed line. Sync messages are ~100 bytes,
// so 64 KiB leaves ample headroom while keeping a malformed peer from
// forcing unbounded buffering.
const MaxLineBytes = 64 * 1024

// Message is a single playback synchronization event.
type Message struct {
	// Type is one of play, pause, seek or heartbeat.
	Type string `json:"type"`
	// PositionSeconds is the playback position in seconds: the current
	// position for play, pause and heartbeat, the seek destination for seek.
	PositionSeconds float64 `json:"position_seconds"`
	// SentAtUnixMilli is the sender-side timestamp in Unix milliseconds,
	// used by the receiver to estimate apparent latency.
	SentAtUnixMilli int64 `json:"sent_at_unix_milli"`
}

// newScanner returns a line scanner with an enlarged buffer so that a
// single oversized (but bounded) message does not fail the scan.
func newScanner(r io.Reader) *bufio.Scanner {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, MaxLineBytes), MaxLineBytes)
	return s
}

// Encode writes m as one JSON document followed by "\n".
func Encode(w io.Writer, m Message) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("syncmsg: encode: %w", err)
	}
	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("syncmsg: encode: write: %w", err)
	}
	return nil
}

// Decode reads a single newline-delimited JSON message from r.
// It is a one-shot helper; to consume a stream of glued messages on one
// connection, use Scanner instead so buffered bytes are not lost between
// calls.
func Decode(r io.Reader) (Message, error) {
	var m Message
	s := newScanner(r)
	if !s.Scan() {
		if err := s.Err(); err != nil {
			return m, fmt.Errorf("syncmsg: decode: scan: %w", err)
		}
		return m, fmt.Errorf("syncmsg: decode: no message: %w", io.ErrUnexpectedEOF)
	}
	if err := json.Unmarshal(s.Bytes(), &m); err != nil {
		return m, fmt.Errorf("syncmsg: decode: invalid JSON: %w", err)
	}
	return m, nil
}

// Scanner incrementally decodes newline-delimited messages from a stream,
// preserving buffered bytes across messages. It mirrors bufio.Scanner usage:
//
//	sc := syncmsg.NewScanner(conn)
//	for sc.Scan() {
//		msg := sc.Message()
//		...
//	}
//	if err := sc.Err(); err != nil { ... }
type Scanner struct {
	s   *bufio.Scanner
	msg Message
	err error
}

// NewScanner returns a Scanner reading newline-delimited JSON messages
// from r.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{s: newScanner(r)}
}

// Scan advances to the next message, reporting false at end of stream or
// on error. A JSON syntax error on one line also stops the scan; details
// are available via Err.
func (sc *Scanner) Scan() bool {
	if sc.err != nil {
		return false
	}
	if !sc.s.Scan() {
		return false
	}
	var m Message
	if err := json.Unmarshal(sc.s.Bytes(), &m); err != nil {
		sc.err = err
		return false
	}
	sc.msg = m
	return true
}

// Message returns the most recent message decoded by Scan.
func (sc *Scanner) Message() Message {
	return sc.msg
}

// Err returns the first non-EOF error encountered by the scan, if any.
// This includes JSON syntax errors on individual lines, not just
// transport errors from the underlying stream.
func (sc *Scanner) Err() error {
	if sc.err != nil {
		return sc.err
	}
	return sc.s.Err()
}
