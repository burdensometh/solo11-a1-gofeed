package shared

import (
	"io"
	"strings"
	"testing"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

func readUTF8(t *testing.T, in string) string {
	t.Helper()
	got, err := io.ReadAll(NewUTF8Reader(strings.NewReader(in)))
	if err != nil {
		t.Fatal(err)
	}
	return string(got)
}

func TestNewUTF8ReaderDeclaredEncoding(t *testing.T) {
	// 0xE9 is é in ISO-8859-1 and invalid UTF-8; the declaration must drive
	// the transcoding.
	doc := `<?xml version="1.0" encoding="ISO-8859-1"?><root>caf` + "\xe9" + `</root>`
	if got := readUTF8(t, doc); !strings.Contains(got, "café") {
		t.Errorf("got %q, want café transcoded from ISO-8859-1", got)
	}
}

func TestNewUTF8ReaderUnknownLabelFallsBackToLatin1(t *testing.T) {
	// An unrecognized declared encoding must not fail the document: it is
	// decoded as latin-1 (Windows-1252). 0xE9 is é there, and 0x93 is a
	// Windows-1252 left double quotation mark — not the C1 control that
	// strict ISO-8859-1 would yield.
	doc := `<?xml version="1.0" encoding="x-not-an-encoding"?><root>` + "\x93" + `caf` + "\xe9" + `</root>`
	got := readUTF8(t, doc)
	if !strings.Contains(got, "café") {
		t.Errorf("got %q, want café from the latin-1 fallback", got)
	}
	if !strings.Contains(got, "“") {
		t.Errorf("got %q, want 0x93 decoded as Windows-1252 U+201C", got)
	}
}

func TestNewUTF8ReaderNoDeclarationIsUTF8(t *testing.T) {
	doc := `<root>café ✓</root>`
	if got := readUTF8(t, doc); got != doc {
		t.Errorf("got %q, want the document passed through unchanged", got)
	}
}

func TestNewUTF8ReaderStripsUTF8BOM(t *testing.T) {
	doc := "\xef\xbb\xbf" + `<root/>`
	if got := readUTF8(t, doc); got != "<root/>" {
		t.Errorf("got %q, want the UTF-8 BOM stripped", got)
	}
}

func TestNewUTF8ReaderUTF16BOM(t *testing.T) {
	for _, tc := range []struct {
		name string
		enc  func() []byte
	}{
		{"little-endian", func() []byte {
			b, _, _ := transform.Bytes(unicode.UTF16(unicode.LittleEndian, unicode.ExpectBOM).NewEncoder(), []byte(`<root>café</root>`))
			return b
		}},
		{"big-endian", func() []byte {
			b, _, _ := transform.Bytes(unicode.UTF16(unicode.BigEndian, unicode.ExpectBOM).NewEncoder(), []byte(`<root>café</root>`))
			return b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := io.ReadAll(NewUTF8Reader(strings.NewReader(string(tc.enc()))))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != `<root>café</root>` {
				t.Errorf("got %q, want <root>café</root>", string(got))
			}
		})
	}
}

func TestNewUTF8ReaderDeclarationAfterUTF8BOM(t *testing.T) {
	// A UTF-8 BOM wins over a contradictory declaration: the BOM is stripped
	// and the (already UTF-8) body passes through.
	doc := "\xef\xbb\xbf" + `<?xml version="1.0" encoding="ISO-8859-1"?><root>café</root>`
	got := readUTF8(t, doc)
	if !strings.HasPrefix(got, `<?xml`) || !strings.Contains(got, "café") {
		t.Errorf("got %q, want BOM stripped and UTF-8 body preserved", got)
	}
}

func TestDeclaredEncoding(t *testing.T) {
	for _, tc := range []struct {
		doc   string
		want  string
		found bool
	}{
		{`<?xml version="1.0" encoding="ISO-8859-1"?><root/>`, "ISO-8859-1", true},
		{`<?xml version="1.0" encoding='windows-1252' standalone="yes"?>`, "windows-1252", true},
		{`<?xml encoding = "utf-8" ?>`, "utf-8", true},
		{`<?xml version="1.0"?><root/>`, "", false},
		{`<root/>`, "", false},
		{`<?xml version="1.0" encoding=`, "", false},
	} {
		got, found := declaredEncoding([]byte(tc.doc))
		if got != tc.want || found != tc.found {
			t.Errorf("declaredEncoding(%q) = %q, %v; want %q, %v", tc.doc, got, found, tc.want, tc.found)
		}
	}
}
