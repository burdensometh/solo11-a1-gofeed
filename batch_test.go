package gofeed_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mmcdole/gofeed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchSource builds a self-describing feed for one source: every title, link
// and GUID carries the source name, so any entry that shows up under another
// source is immediately visible.
func batchSource(name string, kind int, items int) string {
	var b strings.Builder
	switch kind % 3 {
	case 0:
		fmt.Fprintf(&b, `<?xml version="1.0"?><rss version="2.0"><channel><title>%s</title><link>http://%s.example/</link>`, name, name)
		for i := 0; i < items; i++ {
			fmt.Fprintf(&b, `<item><title>%s-item-%d</title><link>http://%s.example/%d</link><guid>%s-%d</guid></item>`, name, i, name, i, name, i)
		}
		b.WriteString(`</channel></rss>`)
	case 1:
		fmt.Fprintf(&b, `<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><title>%s</title><id>urn:%s</id>`, name, name)
		for i := 0; i < items; i++ {
			fmt.Fprintf(&b, `<entry><title>%s-item-%d</title><link href="http://%s.example/%d"/><id>urn:%s:%d</id></entry>`, name, i, name, i, name, i)
		}
		b.WriteString(`</feed>`)
	default:
		fmt.Fprintf(&b, `{"version":"https://jsonfeed.org/version/1.1","title":"%s","home_page":"http://%s.example/","items":[`, name, name)
		for i := 0; i < items; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"id":"%s-%d","title":"%s-item-%d","url":"http://%s.example/%d"}`, name, i, name, i, name, i)
		}
		b.WriteString(`]}`)
	}
	return b.String()
}

func sourceMapFetcher(bodies map[string]string) gofeed.Fetcher {
	return gofeed.FetcherFunc(func(ctx context.Context, src gofeed.Source) ([]byte, error) {
		body, ok := bodies[src.ID]
		if !ok {
			return nil, fmt.Errorf("no body for %s", src.ID)
		}
		// Return a copy so each call owns its bytes.
		return []byte(body), nil
	})
}

// assertFeedBelongsTo checks that every part of feed carries the source name.
func assertFeedBelongsTo(t *testing.T, name string, feed *gofeed.Feed) {
	t.Helper()
	require.NotNil(t, feed)
	assert.Equal(t, name, feed.Title)
	for _, item := range feed.Items {
		require.NotNil(t, item)
		assert.True(t, strings.HasPrefix(item.Title, name+"-item-"), "source %s got item %q", name, item.Title)
		assert.Contains(t, item.Link, name+".example", "source %s got item link %q", name, item.Link)
	}
}

// TestBatchConcurrentIsolation parses many sources of mixed formats at once and
// checks that every result contains only its own entries.
func TestBatchConcurrentIsolation(t *testing.T) {
	const sources = 24
	bodies := map[string]string{}
	var list []gofeed.Source
	for i := 0; i < sources; i++ {
		id := fmt.Sprintf("src%d", i)
		bodies[id] = batchSource(id, i, 15)
		list = append(list, gofeed.Source{ID: id})
	}

	batch := gofeed.NewBatch(gofeed.BatchOptions{
		Fetcher:     sourceMapFetcher(bodies),
		Concurrency: 8,
	})
	for run := 0; run < 10; run++ {
		res, err := batch.Parse(context.Background(), list)
		require.NoError(t, err)
		require.NoError(t, res.Err())
		require.Len(t, res.Results, sources)
		for _, src := range list {
			feed, ok := res.Feed(src.ID)
			require.True(t, ok, "missing result for %s", src.ID)
			assertFeedBelongsTo(t, src.ID, feed)
		}
	}
}

// TestBatchRerunAfterFailureIsClean makes sure a failed attempt leaves no state
// behind: the same Batch parses a good source correctly on both sides of a
// failure, and a rerun after a restart-equivalent fresh Batch is identical.
func TestBatchRerunAfterFailureIsClean(t *testing.T) {
	bodies := map[string]string{
		"alpha": batchSource("alpha", 0, 4),
		"beta":  batchSource("beta", 1, 4),
	}
	fetcher := gofeed.FetcherFunc(func(ctx context.Context, src gofeed.Source) ([]byte, error) {
		if src.ID == "broken" {
			return nil, errors.New("boom")
		}
		return []byte(bodies[src.ID]), nil
	})
	list := []gofeed.Source{{ID: "alpha"}, {ID: "broken"}, {ID: "beta"}}
	batch := gofeed.NewBatch(gofeed.BatchOptions{Fetcher: fetcher})

	res, err := batch.Parse(context.Background(), list)
	require.NoError(t, err)
	require.Error(t, res.Err())
	assert.Len(t, res.Errors(), 1)

	alpha, ok := res.Feed("alpha")
	require.True(t, ok)
	assertFeedBelongsTo(t, "alpha", alpha)
	beta, ok := res.Feed("beta")
	require.True(t, ok)
	assertFeedBelongsTo(t, "beta", beta)
	_, ok = res.Feed("broken")
	assert.False(t, ok, "a failed source must not expose a feed")

	// A fresh Batch, as after a process restart, produces the same output.
	fresh := gofeed.NewBatch(gofeed.BatchOptions{Fetcher: fetcher})
	res2, err := fresh.Parse(context.Background(), list)
	require.NoError(t, err)
	alpha2, ok := res2.Feed("alpha")
	require.True(t, ok)
	assert.Equal(t, alpha.Title, alpha2.Title)
	assert.Len(t, alpha2.Items, len(alpha.Items))
}

// TestBatchFetcherBufferReuseIsContained covers a fetcher that hands back a
// slice aliasing one internal buffer. The batch copies the body per attempt, so
// overwriting the buffer after the call cannot change an already parsed result.
func TestBatchFetcherBufferReuseIsContained(t *testing.T) {
	shared := make([]byte, 4096)
	fetcher := gofeed.FetcherFunc(func(ctx context.Context, src gofeed.Source) ([]byte, error) {
		body := batchSource(src.ID, 0, 3)
		n := copy(shared, body)
		return shared[:n], nil
	})
	batch := gofeed.NewBatch(gofeed.BatchOptions{Fetcher: fetcher, Concurrency: 1})
	list := []gofeed.Source{{ID: "one"}, {ID: "two"}, {ID: "three"}, {ID: "four"}}
	res, err := batch.Parse(context.Background(), list)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	for _, src := range list {
		feed, ok := res.Feed(src.ID)
		require.True(t, ok)
		assertFeedBelongsTo(t, src.ID, feed)
	}
}

// TestBatchDetectsSharedFeed simulates a parser factory that returns the same
// feed value for every source. The second claim is reported at the assemble
// stage instead of silently giving two sources the same entries.
func TestBatchDetectsSharedFeed(t *testing.T) {
	shared := &gofeed.Feed{Title: "shared", Items: []*gofeed.Item{{Title: "e", Link: "http://e.example/"}}}
	batch := gofeed.NewBatch(gofeed.BatchOptions{
		Fetcher: sourceMapFetcher(map[string]string{"a": "x", "b": "y"}),
		ParserFactory: func(gofeed.Source) gofeed.FeedParser {
			return feedParserFunc(func(io.Reader) (*gofeed.Feed, error) { return shared, nil })
		},
		Concurrency: 1,
	})
	res, err := batch.Parse(context.Background(), []gofeed.Source{{ID: "a"}, {ID: "b"}})
	require.NoError(t, err)

	_, ok := res.Feed("a")
	require.True(t, ok)
	_, ok = res.Feed("b")
	assert.False(t, ok)

	sr, ok := res.Result("b")
	require.True(t, ok)
	var mix *gofeed.MixError
	require.ErrorAs(t, sr.Err, &mix)
	assert.Equal(t, gofeed.StageAssemble, mix.Stage)
	assert.Contains(t, mix.Error(), `owned by source "a"`)
}

// TestBatchVerifyAttributesMixing covers the semantic check: when a feed that
// was parsed for source A carries source B's content, Verify rejects it and the
// MixError names the verify stage with the full digest trace.
func TestBatchVerifyAttributesMixing(t *testing.T) {
	// bSwapped returns a's body when b is requested, the shape of the reported
	// bug where A's entries appear under B.
	fetcher := gofeed.FetcherFunc(func(ctx context.Context, src gofeed.Source) ([]byte, error) {
		return []byte(batchSource("source-a", 0, 2)), nil
	})
	batch := gofeed.NewBatch(gofeed.BatchOptions{
		BatchID: "batch-7",
		Fetcher: fetcher,
		Verify: func(src gofeed.Source, feed *gofeed.Feed) error {
			if feed.Title != src.ID {
				return fmt.Errorf("feed title %q does not match source %q", feed.Title, src.ID)
			}
			return nil
		},
	})
	res, err := batch.Parse(context.Background(), []gofeed.Source{{ID: "source-b"}})
	require.NoError(t, err)
	sr, ok := res.Result("source-b")
	require.True(t, ok)
	require.Error(t, sr.Err)
	_, ok = res.Feed("source-b")
	assert.False(t, ok, "a feed that fails verification must not be exposed")

	var mix *gofeed.MixError
	require.ErrorAs(t, sr.Err, &mix)
	assert.Equal(t, "batch-7", mix.BatchID)
	assert.Equal(t, "source-b", mix.SourceID)
	assert.Equal(t, gofeed.StageVerify, mix.Stage)
	assert.Equal(t, 1, mix.Attempt)

	// The trace identifies the step and ties the fetch body to the parse result.
	fetch, ok := sr.LastAttempt().Stage(gofeed.StageFetch)
	require.True(t, ok)
	parse, ok := sr.LastAttempt().Stage(gofeed.StageParse)
	require.True(t, ok)
	assert.NotEmpty(t, fetch.Digest)
	assert.NotEmpty(t, parse.Digest)
	assert.NotEqual(t, fetch.Digest, parse.Digest, "body and feed digests describe different values")
	assert.Contains(t, mix.Error(), "fetch")
	assert.Contains(t, mix.Error(), "verify")
}

// feedParserFunc adapts a function to gofeed.FeedParser.
type feedParserFunc func(io.Reader) (*gofeed.Feed, error)

func (f feedParserFunc) Parse(r io.Reader) (*gofeed.Feed, error) { return f(r) }

// TestBatchRetriesGetFreshParser checks that each attempt gets its own parser
// and that every attempt is traced with its own attempt number.
func TestBatchRetriesGetFreshParser(t *testing.T) {
	var parsers int32
	var attempts int32
	batch := gofeed.NewBatch(gofeed.BatchOptions{
		Fetcher:     sourceMapFetcher(map[string]string{"a": batchSource("a", 0, 2)}),
		MaxAttempts: 3,
		Backoff:     func(int, error) time.Duration { return 0 },
		ParserFactory: func(gofeed.Source) gofeed.FeedParser {
			atomic.AddInt32(&parsers, 1)
			return feedParserFunc(func(r io.Reader) (*gofeed.Feed, error) {
				if atomic.AddInt32(&attempts, 1) < 3 {
					return nil, errors.New("transient")
				}
				return gofeed.NewParser().Parse(r)
			})
		},
	})
	res, err := batch.Parse(context.Background(), []gofeed.Source{{ID: "a"}})
	require.NoError(t, err)
	feed, ok := res.Feed("a")
	require.True(t, ok)
	assertFeedBelongsTo(t, "a", feed)
	assert.EqualValues(t, 3, parsers, "each attempt must build its own parser")
	sr, _ := res.Result("a")
	require.Len(t, sr.Attempts, 3)
	for i, attempt := range sr.Attempts {
		assert.Equal(t, i+1, attempt.Attempt)
		assert.Contains(t, attempt.ID, "#"+fmt.Sprint(i+1))
	}
	assert.Error(t, sr.Attempts[0].Err)
	assert.NoError(t, sr.Attempts[2].Err)
}

// TestBatchTraceEvents checks that trace events are self-describing and
// JSON-serializable for a journal.
func TestBatchTraceEvents(t *testing.T) {
	var mu sync.Mutex
	var events []gofeed.TraceEvent
	batch := gofeed.NewBatch(gofeed.BatchOptions{
		BatchID: "run-1",
		Fetcher: sourceMapFetcher(map[string]string{
			"a": batchSource("a", 0, 2),
			"b": batchSource("b", 1, 2),
		}),
		Concurrency: 2,
		Trace: func(e gofeed.TraceEvent) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		},
	})
	res, err := batch.Parse(context.Background(), []gofeed.Source{{ID: "a"}, {ID: "b"}})
	require.NoError(t, err)
	require.NoError(t, res.Err())

	perSource := map[string]map[gofeed.Stage]bool{}
	for _, e := range events {
		assert.Equal(t, "run-1", e.BatchID)
		assert.NotEmpty(t, e.AttemptID)
		if perSource[e.SourceID] == nil {
			perSource[e.SourceID] = map[gofeed.Stage]bool{}
		}
		perSource[e.SourceID][e.Stage] = true

		data, err := json.Marshal(e)
		require.NoError(t, err)
		var wire map[string]any
		require.NoError(t, json.Unmarshal(data, &wire))
		assert.Equal(t, e.SourceID, wire["source_id"])
		assert.Equal(t, string(e.Stage), wire["stage"])
	}
	for _, id := range []string{"a", "b"} {
		for _, stage := range []gofeed.Stage{gofeed.StageFetch, gofeed.StageDetect, gofeed.StageParse, gofeed.StageAssemble} {
			assert.True(t, perSource[id][stage], "source %s missing %s trace", id, stage)
		}
	}
}

// TestBatchRejectsBadSourceIDs covers the attribution precondition: every
// result must be addressable, so IDs are required and unique.
func TestBatchRejectsBadSourceIDs(t *testing.T) {
	batch := gofeed.NewBatch(gofeed.BatchOptions{Fetcher: sourceMapFetcher(nil)})

	_, err := batch.Parse(context.Background(), []gofeed.Source{{ID: ""}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")

	_, err = batch.Parse(context.Background(), []gofeed.Source{{ID: "dup"}, {ID: "dup"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")

	res, err := batch.Parse(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, res.Results)
	assert.NoError(t, res.Err())
}

// TestBatchContextCancellation checks that a cancelled batch stops promptly and
// reports the cancellation rather than a partial result.
func TestBatchContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fetcher := gofeed.FetcherFunc(func(ctx context.Context, src gofeed.Source) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	batch := gofeed.NewBatch(gofeed.BatchOptions{Fetcher: fetcher, MaxAttempts: 3})
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	res, err := batch.Parse(ctx, []gofeed.Source{{ID: "a"}, {ID: "b"}})
	require.NoError(t, err)
	require.Error(t, res.Err())
	for _, sr := range res.Results {
		require.Error(t, sr.Err)
		assert.ErrorIs(t, sr.Err, context.Canceled)
		assert.Nil(t, sr.Feed)
	}
}

// TestHTTPFetcher checks the default fetcher end to end, including per-source
// isolation over the network path.
func TestHTTPFetcher(t *testing.T) {
	bodies := map[string]string{
		"/a": batchSource("a", 0, 3),
		"/b": batchSource("b", 1, 3),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := bodies[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		io.WriteString(w, body)
	}))
	defer srv.Close()

	batch := gofeed.NewBatch(gofeed.BatchOptions{
		Fetcher:     gofeed.HTTPFetcher{Client: srv.Client(), UserAgent: "test"},
		Concurrency: 2,
	})
	list := []gofeed.Source{
		{ID: "a", URL: srv.URL + "/a"},
		{ID: "b", URL: srv.URL + "/b"},
		{ID: "missing", URL: srv.URL + "/missing"},
	}
	res, err := batch.Parse(context.Background(), list)
	require.NoError(t, err)

	feedA, ok := res.Feed("a")
	require.True(t, ok)
	assertFeedBelongsTo(t, "a", feedA)
	feedB, ok := res.Feed("b")
	require.True(t, ok)
	assertFeedBelongsTo(t, "b", feedB)

	sr, _ := res.Result("missing")
	var httpErr gofeed.HTTPError
	require.ErrorAs(t, sr.Err, &httpErr)
	assert.Equal(t, http.StatusNotFound, httpErr.StatusCode)
}

// TestHTTPFetcherMaxByteSize checks that the default fetcher enforces the size
// limit rather than truncating, so an oversized source fails cleanly.
func TestHTTPFetcherMaxByteSize(t *testing.T) {
	body := batchSource("a", 0, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, body)
	}))
	defer srv.Close()

	small := gofeed.NewBatch(gofeed.BatchOptions{
		Fetcher: gofeed.HTTPFetcher{Client: srv.Client(), MaxByteSize: int64(len(body) - 1)},
	})
	res, err := small.Parse(context.Background(), []gofeed.Source{{ID: "a", URL: srv.URL}})
	require.NoError(t, err)
	sr, _ := res.Result("a")
	require.ErrorIs(t, sr.Err, gofeed.ErrResponseTooLarge)
	assert.Nil(t, sr.Feed)

	exact := gofeed.NewBatch(gofeed.BatchOptions{
		Fetcher: gofeed.HTTPFetcher{Client: srv.Client(), MaxByteSize: int64(len(body))},
	})
	res, err = exact.Parse(context.Background(), []gofeed.Source{{ID: "a", URL: srv.URL}})
	require.NoError(t, err)
	feed, ok := res.Feed("a")
	require.True(t, ok)
	assertFeedBelongsTo(t, "a", feed)
}
