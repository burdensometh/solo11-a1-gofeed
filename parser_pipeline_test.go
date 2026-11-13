package gofeed_test

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/mmcdole/gofeed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Feeds fetched from the wild arrive in legacy encodings and littered with
// control characters. The read pipeline transcodes to UTF-8 as declared
// (falling back to latin-1) and blanks XML-1.0-illegal control characters, so
// such feeds parse instead of failing the whole batch.

func TestParser_Latin1DeclaredFeed(t *testing.T) {
	// 0xE9 is é in ISO-8859-1 and invalid UTF-8; the declaration drives the
	// transcoding.
	doc := `<?xml version="1.0" encoding="ISO-8859-1"?>` +
		`<rss version="2.0"><channel><title>caf` + "\xe9" + `</title>` +
		`<item><title>item</title></item></channel></rss>`
	feed, err := gofeed.NewParser().Parse(strings.NewReader(doc))
	require.NoError(t, err)
	require.Equal(t, "café", feed.Title)
}

func TestParser_UnknownEncodingFallsBackToLatin1(t *testing.T) {
	// A declared encoding nobody supports used to fail the whole document;
	// now it decodes as latin-1 (Windows-1252).
	doc := `<?xml version="1.0" encoding="x-bogus-charset"?>` +
		`<rss version="2.0"><channel><title>caf` + "\xe9" + `</title></channel></rss>`
	feed, err := gofeed.NewParser().Parse(strings.NewReader(doc))
	require.NoError(t, err)
	require.Equal(t, "café", feed.Title)
}

func TestParser_ControlCharactersReplacedAndCounted(t *testing.T) {
	var logBuf bytes.Buffer
	defer log.SetOutput(log.Writer())
	log.SetOutput(&logBuf)

	doc := `<rss version="2.0"><channel><title>ne` + "\x08" + `ws</title>` +
		`<item><title>it` + "\x00" + `em</title></item></channel></rss>`
	feed, err := gofeed.NewParser().Parse(strings.NewReader(doc))
	require.NoError(t, err)
	require.Equal(t, "ne ws", feed.Title)
	require.Len(t, feed.Items, 1)
	require.Equal(t, "it em", feed.Items[0].Title)
	// The cleaning is recorded, not silent.
	assert.Contains(t, logBuf.String(), "2 control characters")
}

func TestParser_UTF16Feed(t *testing.T) {
	// UTF-16LE with BOM, encoded by hand: BOM, then each rune little-endian.
	doc := `<?xml version="1.0" encoding="UTF-16"?><rss version="2.0"><channel><title>café</title></channel></rss>`
	raw := []byte{0xFF, 0xFE}
	for _, r := range doc {
		raw = append(raw, byte(r), byte(r>>8))
	}
	feed, err := gofeed.NewParser().Parse(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, "café", feed.Title)
}
