package syncmsg

import (
	"bytes"
	"strings"
	"testing"
)

// TestEncodeDecodeRoundTrip verifies every message type survives an
// Encode->Decode cycle unchanged.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		msg  Message
	}{
		{"play", Message{Type: TypePlay, PositionSeconds: 12.5, SentAtUnixMilli: 1721234567890}},
		{"pause", Message{Type: TypePause, PositionSeconds: 0, SentAtUnixMilli: 1721234567891}},
		{"seek", Message{Type: TypeSeek, PositionSeconds: 300.25, SentAtUnixMilli: 1721234567892}},
		{"heartbeat", Message{Type: TypeHeartbeat, PositionSeconds: 61, SentAtUnixMilli: 1721234567893}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Encode(&buf, tt.msg); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !strings.HasSuffix(buf.String(), "\n") {
				t.Fatalf("encoded message %q is not newline-terminated", buf.String())
			}
			got, err := Decode(&buf)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got != tt.msg {
				t.Errorf("round trip mismatch: got %+v, want %+v", got, tt.msg)
			}
		})
	}
}

// TestScannerGluedMessages encodes several messages into one buffer and
// checks the Scanner reads all of them back in order.
func TestScannerGluedMessages(t *testing.T) {
	want := []Message{
		{Type: TypePlay, PositionSeconds: 1, SentAtUnixMilli: 1000},
		{Type: TypeHeartbeat, PositionSeconds: 2, SentAtUnixMilli: 2000},
		{Type: TypePause, PositionSeconds: 3, SentAtUnixMilli: 3000},
		{Type: TypeSeek, PositionSeconds: 120, SentAtUnixMilli: 4000},
		{Type: TypeHeartbeat, PositionSeconds: 121, SentAtUnixMilli: 5000},
	}
	var buf bytes.Buffer
	for _, m := range want {
		if err := Encode(&buf, m); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	sc := NewScanner(&buf)
	var got []Message
	for sc.Scan() {
		got = append(got, sc.Message())
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("Scanner.Err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("decoded %d messages, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("message %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestScannerMalformedLine checks that an invalid JSON line stops the
// scan and is reported via Err, and that Decode rejects it too.
func TestScannerMalformedLine(t *testing.T) {
	first := Message{Type: TypePlay, PositionSeconds: 7, SentAtUnixMilli: 7000}
	var buf bytes.Buffer
	if err := Encode(&buf, first); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	buf.WriteString("{not valid json\n")
	if err := Encode(&buf, Message{Type: TypePause, PositionSeconds: 8, SentAtUnixMilli: 8000}); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	sc := NewScanner(&buf)
	if !sc.Scan() {
		t.Fatalf("first Scan = false, want true for the valid leading message")
	}
	if got := sc.Message(); got != first {
		t.Fatalf("first message = %+v, want %+v", got, first)
	}
	if sc.Scan() {
		t.Fatalf("Scan after malformed line = true, want false (scan must stop)")
	}
	if err := sc.Err(); err == nil {
		t.Fatalf("Scanner.Err = nil after malformed line, want JSON syntax error")
	}

	if _, err := Decode(strings.NewReader("{not valid json\n")); err == nil {
		t.Fatalf("Decode of invalid JSON = nil error, want error")
	}
}

// TestEmptyInput covers the degenerate stream: Decode must fail and the
// Scanner must report no messages without an error.
func TestEmptyInput(t *testing.T) {
	if _, err := Decode(strings.NewReader("")); err == nil {
		t.Fatalf("Decode of empty input = nil error, want error")
	}
	sc := NewScanner(strings.NewReader(""))
	if sc.Scan() {
		t.Fatalf("Scan on empty input = true, want false")
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("Scanner.Err on empty input = %v, want nil (clean EOF)", err)
	}
}
