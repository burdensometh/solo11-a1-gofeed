package shared

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"io"

	xpp "github.com/mmcdole/goxpp/v2"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// sniffSize bounds how many leading bytes NewUTF8Reader examines for a byte
// order mark or the encoding pseudo-attribute of the XML declaration. 1024
// matches the WHATWG prescan window; the declaration of any real feed sits
// well within it.
const sniffSize = 1024

var (
	bomUTF8    = []byte{0xEF, 0xBB, 0xBF}
	bomUTF16BE = []byte{0xFE, 0xFF}
	bomUTF16LE = []byte{0xFF, 0xFE}
)

// NewUTF8Reader wraps r so it yields UTF-8. The transcoding follows the
// document's own declaration: a byte order mark, or the encoding
// pseudo-attribute of the XML declaration; with neither, XML's default of
// UTF-8 is assumed and r passes through unchanged. A declared encoding that
// is not recognized falls back to latin-1 — Windows-1252, the web-compatible
// interpretation of latin-1 that mislabeled feeds almost always actually
// contain — rather than failing the whole document.
func NewUTF8Reader(r io.Reader) io.Reader {
	br := bufio.NewReaderSize(r, sniffSize)
	// A short document makes Peek return io.EOF with what it has; that
	// prefix is all the sniffing below needs, so the error is ignored.
	peek, _ := br.Peek(sniffSize)

	// A byte order mark settles the encoding on its own (XML 1.0 §F.1).
	// encoding/xml does not skip BOMs, so each is consumed here. The "<?"
	// signatures catch BOM-less UTF-16 — including a stream whose BOM an
	// earlier look-ahead already consumed.
	utf16be := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder()
	utf16le := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder()
	switch {
	case bytes.HasPrefix(peek, bomUTF8):
		br.Discard(len(bomUTF8))
		return br
	case bytes.HasPrefix(peek, bomUTF16BE):
		br.Discard(len(bomUTF16BE))
		return transform.NewReader(br, utf16be)
	case bytes.HasPrefix(peek, bomUTF16LE):
		br.Discard(len(bomUTF16LE))
		return transform.NewReader(br, utf16le)
	case bytes.HasPrefix(peek, []byte{0x00, 0x3C, 0x00, 0x3F}): // "<?" in UTF-16BE
		return transform.NewReader(br, utf16be)
	case bytes.HasPrefix(peek, []byte{0x3C, 0x00, 0x3F, 0x00}): // "<?" in UTF-16LE
		return transform.NewReader(br, utf16le)
	}

	label, ok := declaredEncoding(peek)
	if !ok {
		return br
	}
	if enc, _ := charset.Lookup(label); enc != nil {
		return transform.NewReader(br, enc.NewDecoder())
	}
	return transform.NewReader(br, charmap.Windows1252.NewDecoder())
}

// declaredEncoding extracts the encoding pseudo-attribute from an XML
// declaration ("<?xml ... encoding="label" ...?>") at the very start of peek.
// The declaration is ASCII, so it reads directly from the raw bytes of any
// ASCII-superset encoding.
func declaredEncoding(peek []byte) (string, bool) {
	if !bytes.HasPrefix(peek, []byte("<?xml")) {
		return "", false
	}
	decl := peek[len("<?xml"):]
	end := bytes.Index(decl, []byte("?>"))
	if end < 0 {
		return "", false
	}
	decl = decl[:end]

	for {
		decl = bytes.TrimLeft(decl, " \t\r\n")
		if len(decl) == 0 {
			return "", false
		}
		i := bytes.IndexAny(decl, "= \t\r\n")
		if i < 0 {
			return "", false
		}
		name := string(decl[:i])
		decl = bytes.TrimLeft(decl[i:], " \t\r\n")
		if len(decl) == 0 || decl[0] != '=' {
			return "", false
		}
		decl = bytes.TrimLeft(decl[1:], " \t\r\n")
		if len(decl) == 0 || (decl[0] != '"' && decl[0] != '\'') {
			return "", false
		}
		quote := decl[0]
		decl = decl[1:]
		i = bytes.IndexByte(decl, quote)
		if i < 0 {
			return "", false
		}
		if name == "encoding" {
			return string(decl[:i]), true
		}
		decl = decl[i+1:]
	}
}

// NewXMLParser returns a pull parser over r, which may arrive in any
// encoding: the stream is first transcoded to UTF-8 (NewUTF8Reader), then
// stripped of the control characters XML 1.0 forbids (ControlCharSanitizer),
// and only then tokenized — non-strict, so real-world feeds with unescaped
// entities and other common mistakes still parse. The returned sanitizer
// reports how many control characters were replaced once parsing is done.
func NewXMLParser(r io.Reader) (*xpp.Parser, *ControlCharSanitizer) {
	sanitizer := NewControlCharSanitizer(NewUTF8Reader(r))
	d := xml.NewDecoder(sanitizer)
	d.Strict = false
	// The stream is already UTF-8, but the XML declaration may still name
	// the original encoding. Satisfy the decoder's charset hook with a
	// pass-through so it does not transcode a second time.
	d.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	return xpp.New(d), sanitizer
}
