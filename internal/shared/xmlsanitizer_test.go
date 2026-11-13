package shared

import (
	"io"
	"strings"
	"testing"
)

func TestControlCharSanitizer(t *testing.T) {
	// Illegal C0 controls (0x00, 0x08) become spaces; tab, LF, CR and normal
	// text pass through untouched. Replacements are counted, not dropped, so
	// the text keeps its length.
	in := "ab\x08cd\x00ef\tgh\nij\rk"
	s := NewControlCharSanitizer(strings.NewReader(in))
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatal(err)
	}
	want := "ab cd ef\tgh\nij\rk"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
	if n := s.Replaced(); n != 2 {
		t.Errorf("Replaced() = %d, want 2", n)
	}
}

func TestControlCharSanitizerCountsEveryByte(t *testing.T) {
	// Every C0 control except tab/LF/CR is rewritten: 0x00-0x08 (9),
	// 0x0B-0x0C (2), 0x0E-0x1F (18) — 29 in all.
	var in strings.Builder
	for b := byte(0); b < 0x20; b++ {
		in.WriteByte(b)
	}
	s := NewControlCharSanitizer(strings.NewReader(in.String()))
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatal(err)
	}
	if n := s.Replaced(); n != 29 {
		t.Errorf("Replaced() = %d, want 29", n)
	}
	if string(got) != strings.Repeat(" ", 9)+"\t\n"+"  \r"+strings.Repeat(" ", 18) {
		t.Errorf("got %q, want tab/LF/CR preserved and the rest spaces", string(got))
	}
}

func TestControlCharSanitizerLeavesUTF8Alone(t *testing.T) {
	// Multi-byte UTF-8 sequences consist of bytes >= 0x80 and must survive
	// byte-level sanitizing untouched.
	in := "héllo wörld — ünïcode ✓"
	s := NewControlCharSanitizer(strings.NewReader(in))
	got, err := io.ReadAll(s)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != in {
		t.Errorf("got %q, want %q", string(got), in)
	}
	if n := s.Replaced(); n != 0 {
		t.Errorf("Replaced() = %d, want 0", n)
	}
}
