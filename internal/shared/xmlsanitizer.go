package shared

import (
	"io"
	"log"
)

// ControlCharSanitizer wraps a reader and rewrites the C0 control characters
// that XML 1.0 forbids — 0x00-0x1F except tab, LF and CR — into spaces as
// they are read, counting each replacement. It runs on the transcoded UTF-8
// stream (see NewXMLParser), where those byte values only ever denote the
// control characters themselves: multi-byte UTF-8 sequences consist solely of
// bytes >= 0x80, so rewriting single bytes cannot corrupt a character.
// Replacing (rather than dropping) keeps offsets stable and lets malformed
// feeds parse instead of failing outright.
type ControlCharSanitizer struct {
	r        io.Reader
	replaced int
}

// NewControlCharSanitizer returns a reader that sanitizes r's output.
func NewControlCharSanitizer(r io.Reader) *ControlCharSanitizer {
	return &ControlCharSanitizer{r: r}
}

func (c *ControlCharSanitizer) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	for i := 0; i < n; i++ {
		if b := p[i]; b < 0x20 && b != '\t' && b != '\n' && b != '\r' {
			p[i] = ' '
			c.replaced++
		}
	}
	return n, err
}

// Replaced reports how many control characters have been rewritten so far.
// Call it once parsing is finished (or abandoned) to get the final count.
func (c *ControlCharSanitizer) Replaced() int {
	return c.replaced
}

// LogSanitized records how many control characters s rewrote, if any. kind
// names the feed format for the log message ("rss", "atom"). Parsing is never
// interrupted: the count is only observed and logged after the fact.
func LogSanitized(s *ControlCharSanitizer, kind string) {
	if n := s.Replaced(); n > 0 {
		log.Printf("gofeed: %s feed contained %d control characters disallowed by XML 1.0; replaced with spaces", kind, n)
	}
}
