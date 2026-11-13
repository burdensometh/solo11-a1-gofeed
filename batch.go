package gofeed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Stage names one step of a batch parse. Every stage emits a TraceEvent tagged
// with the source and attempt it ran for, so a result that ended up holding
// another source's content can be traced back to the step that introduced it.
type Stage string

const (
	// StageFetch retrieves the source body.
	StageFetch Stage = "fetch"
	// StageDetect sniffs the feed type of the fetched body.
	StageDetect Stage = "detect"
	// StageParse parses the body into a universal Feed.
	StageParse Stage = "parse"
	// StageVerify runs the caller's ownership check, if configured.
	StageVerify Stage = "verify"
	// StageAssemble checks that the feed is not shared with another source and
	// stores it in its source's result slot.
	StageAssemble Stage = "assemble"
)

// Source identifies one feed in a batch. ID is the attribution key: results
// are keyed by it, every trace event carries it, and duplicate IDs are rejected
// before parsing starts. It should be stable across retries and restarts so a
// persisted trace can be correlated with a later run.
type Source struct {
	// ID uniquely identifies the source within a batch.
	ID string
	// URL is fetched by HTTPFetcher. Custom Fetchers may ignore it.
	URL string
}

// Fetcher retrieves the body of a source. Implementations must be safe for
// concurrent use and should return a slice owned by the call. The batch runner
// still copies the bytes into an attempt-private buffer before parsing, so a
// Fetcher that reuses a buffer cannot leak one source's bytes into another's
// result.
type Fetcher interface {
	Fetch(ctx context.Context, src Source) ([]byte, error)
}

// FetcherFunc adapts a function to Fetcher.
type FetcherFunc func(ctx context.Context, src Source) ([]byte, error)

// Fetch calls f.
func (f FetcherFunc) Fetch(ctx context.Context, src Source) ([]byte, error) {
	return f(ctx, src)
}

// HTTPFetcher is the default Fetcher. Each call creates its own request and
// reads the body fully into a buffer owned by that call, so no state is shared
// between sources.
type HTTPFetcher struct {
	// Client is used for requests. Nil means a shared default client (safe for
	// concurrent use).
	Client *http.Client
	// UserAgent is sent as the User-Agent header when non-empty.
	UserAgent string
	// Auth enables HTTP basic auth when both fields are set.
	Auth *Auth
	// MaxByteSize limits the response size. Zero means no limit. Exceeding it
	// returns ErrResponseTooLarge.
	MaxByteSize int64
	// Timeout bounds a single fetch. Zero means defaultRequestTimeout.
	Timeout time.Duration
}

// Fetch retrieves src.URL.
func (h HTTPFetcher) Fetch(ctx context.Context, src Source) ([]byte, error) {
	if strings.TrimSpace(src.URL) == "" {
		return nil, fmt.Errorf("gofeed: source %q has no URL", src.ID)
	}
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client := h.Client
	if client == nil {
		client = defaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return nil, err
	}
	if h.UserAgent != "" {
		req.Header.Set("User-Agent", h.UserAgent)
	}
	if h.Auth != nil && h.Auth.Username != "" && h.Auth.Password != "" {
		req.SetBasicAuth(h.Auth.Username, h.Auth.Password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, HTTPError{StatusCode: resp.StatusCode, Status: resp.Status}
	}

	if h.MaxByteSize <= 0 {
		return io.ReadAll(resp.Body)
	}
	body := &limitedReader{r: resp.Body, left: h.MaxByteSize}
	data, err := io.ReadAll(body)
	if body.err == ErrResponseTooLarge {
		return nil, ErrResponseTooLarge
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// FeedParser is the part of *Parser a batch attempt needs. Each call to
// BatchOptions.ParserFactory must return a parser dedicated to that attempt:
// sharing one mutable parser between sources is exactly the kind of
// cross-source state the batch runner is designed to rule out.
type FeedParser interface {
	Parse(feed io.Reader) (*Feed, error)
}

// TraceEvent is emitted for every stage of every attempt. Events are
// self-describing: BatchID, SourceID, AttemptID and Attempt say who the step
// ran for, and Digest makes it possible to spot a body or feed that changed
// hands.
type TraceEvent struct {
	BatchID   string
	SourceID  string
	AttemptID string
	Attempt   int
	Stage     Stage
	At        time.Time
	Duration  time.Duration
	// Bytes is the fetched body size (fetch and later stages).
	Bytes int
	// Digest is the SHA-256 hex digest of the data the stage produced: the
	// fetched body for fetch, the parsed feed content for parse and later
	// stages.
	Digest string
	// FeedType is the detected or parsed feed type (detect and later stages).
	FeedType string
	// Items is the number of items in the parsed feed (parse and later stages).
	Items int
	// Err is the stage error, if any.
	Err error
}

// MarshalJSON renders durations as milliseconds and errors as text, so trace
// events can be written to a log or journal directly.
func (e TraceEvent) MarshalJSON() ([]byte, error) {
	type wire struct {
		BatchID    string `json:"batch_id,omitempty"`
		SourceID   string `json:"source_id"`
		AttemptID  string `json:"attempt_id"`
		Attempt    int    `json:"attempt"`
		Stage      Stage  `json:"stage"`
		At         string `json:"at"`
		DurationMS int64  `json:"duration_ms"`
		Bytes      int    `json:"bytes,omitempty"`
		Digest     string `json:"digest,omitempty"`
		FeedType   string `json:"feed_type,omitempty"`
		Items      int    `json:"items,omitempty"`
		Error      string `json:"error,omitempty"`
	}
	w := wire{
		BatchID:    e.BatchID,
		SourceID:   e.SourceID,
		AttemptID:  e.AttemptID,
		Attempt:    e.Attempt,
		Stage:      e.Stage,
		At:         e.At.UTC().Format(time.RFC3339Nano),
		DurationMS: e.Duration.Milliseconds(),
		Bytes:      e.Bytes,
		Digest:     e.Digest,
		FeedType:   e.FeedType,
		Items:      e.Items,
	}
	if e.Err != nil {
		w.Error = e.Err.Error()
	}
	return json.Marshal(w)
}

// AttemptTrace records one attempt at parsing a source, including every stage
// it reached.
type AttemptTrace struct {
	// ID is stable for a batch ID, source ID and attempt number, so a log line
	// can be joined to this attempt.
	ID       string
	SourceID string
	Attempt  int
	Started  time.Time
	Duration time.Duration
	Stages   []TraceEvent
	// Err is the attempt's failure; nil on success.
	Err error
}

// Stage returns the trace event for stage, or false if the attempt did not
// reach it.
func (t *AttemptTrace) Stage(stage Stage) (TraceEvent, bool) {
	if t == nil {
		return TraceEvent{}, false
	}
	for _, e := range t.Stages {
		if e.Stage == stage {
			return e, true
		}
	}
	return TraceEvent{}, false
}

// SourceResult is the outcome for exactly one source. Feed is only ever set
// when Err is nil, so a caller can trust that a non-nil Feed belongs to
// Source.ID.
type SourceResult struct {
	Source   Source
	Feed     *Feed
	Attempts []AttemptTrace
	Err      error
}

// LastAttempt returns the most recent attempt trace, or nil if there is none.
func (r *SourceResult) LastAttempt() *AttemptTrace {
	if r == nil || len(r.Attempts) == 0 {
		return nil
	}
	return &r.Attempts[len(r.Attempts)-1]
}

// BatchResult holds the outcome of a batch in input order, indexed by source
// ID.
type BatchResult struct {
	BatchID string
	Results []*SourceResult
	index   map[string]*SourceResult
}

// Result returns the result for id.
func (r *BatchResult) Result(id string) (*SourceResult, bool) {
	if r == nil {
		return nil, false
	}
	sr, ok := r.index[id]
	return sr, ok
}

// Feed returns the parsed feed for id. The boolean is false when the source
// failed or is unknown.
func (r *BatchResult) Feed(id string) (*Feed, bool) {
	sr, ok := r.Result(id)
	if !ok || sr.Err != nil || sr.Feed == nil {
		return nil, false
	}
	return sr.Feed, true
}

// Errors returns the per-source failures in input order.
func (r *BatchResult) Errors() []error {
	if r == nil {
		return nil
	}
	var errs []error
	for _, sr := range r.Results {
		if sr != nil && sr.Err != nil {
			errs = append(errs, sr.Err)
		}
	}
	return errs
}

// Err joins the per-source failures, or returns nil when every source
// succeeded.
func (r *BatchResult) Err() error {
	errs := r.Errors()
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// MixError reports that a result does not belong unambiguously to its source.
// Stage names the step at which the isolation check failed, and Stages is the
// trace of everything that happened before it, including the body and feed
// digests, so the mixing can be attributed to a step.
type MixError struct {
	BatchID  string
	SourceID string
	Attempt  int
	Stage    Stage
	Detail   string
	Cause    error
	Stages   []TraceEvent
}

// Error describes the failure and the stage trace.
func (e *MixError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "gofeed: source %q result is not its own", e.SourceID)
	if e.BatchID != "" {
		fmt.Fprintf(&b, " (batch %q)", e.BatchID)
	}
	fmt.Fprintf(&b, ": mixed at stage %q during attempt %d", e.Stage, e.Attempt)
	if e.Detail != "" {
		fmt.Fprintf(&b, ": %s", e.Detail)
	}
	if len(e.Stages) > 0 {
		fmt.Fprintf(&b, "; trace: %s", summarizeStages(e.Stages))
	}
	return b.String()
}

// Unwrap returns the underlying verification error, if any.
func (e *MixError) Unwrap() error { return e.Cause }

func summarizeStages(stages []TraceEvent) string {
	parts := make([]string, 0, len(stages))
	for _, s := range stages {
		p := string(s.Stage)
		if s.FeedType != "" {
			p += "(" + s.FeedType + ")"
		}
		if s.Bytes > 0 {
			p += fmt.Sprintf(" %dB", s.Bytes)
		}
		if s.Items > 0 {
			p += fmt.Sprintf(" %d items", s.Items)
		}
		if s.Digest != "" {
			p += " sha256:" + shortDigest(s.Digest)
		}
		if s.Err != nil {
			p += " error=" + s.Err.Error()
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, " -> ")
}

func shortDigest(d string) string {
	if len(d) > 12 {
		return d[:12]
	}
	return d
}

func bodyDigest(b []byte) string {
	if b == nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// feedDigest hashes the fields that identify a feed's content. It is limited
// to deterministic fields (no maps) so repeated runs produce the same digest.
func feedDigest(f *Feed) string {
	if f == nil {
		return ""
	}
	h := sha256.New()
	fmt.Fprintf(h, "type=%s\x1ftitle=%s\x1flink=%s\x1fitems=%d\n", f.FeedType, f.Title, f.Link, len(f.Items))
	for _, item := range f.Items {
		if item == nil {
			io.WriteString(h, "<nil>\n")
			continue
		}
		fmt.Fprintf(h, "%s\x1f%s\x1f%s\n", item.Title, item.Link, item.GUID)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func feedTypeName(ft FeedType) string {
	switch ft {
	case FeedTypeRSS:
		return "rss"
	case FeedTypeAtom:
		return "atom"
	case FeedTypeJSON:
		return "json"
	}
	return "unknown"
}

// BatchOptions configures a Batch. The zero value is usable: the default fetcher
// is HTTPFetcher, each attempt gets a fresh *Parser, and concurrency defaults
// to GOMAXPROCS.
type BatchOptions struct {
	// BatchID labels every trace event and attempt ID. Optional, but setting it
	// is recommended so traces from different runs, including after a restart,
	// can be told apart.
	BatchID string
	// Concurrency is the maximum number of sources parsed at once. Zero means
	// GOMAXPROCS.
	Concurrency int
	// MaxAttempts is the number of attempts per source. Zero or negative means
	// 1. Each attempt gets a fresh parser and its own body buffer.
	MaxAttempts int
	// Backoff returns the delay before the given attempt (2 for the first
	// retry). Nil retries immediately.
	Backoff func(attempt int, err error) time.Duration
	// Fetcher retrieves source bodies. Nil means HTTPFetcher{}.
	Fetcher Fetcher
	// ParserFactory returns the parser for one attempt of src. It is called
	// once per attempt and must return a fresh parser owned by that attempt; a
	// shared parser is a configuration error. Nil means a new *Parser each
	// time.
	ParserFactory func(src Source) FeedParser
	// Verify is the semantic ownership check: return a non-nil error when the
	// parsed feed does not belong to src (for example, its title or item host
	// does not match the source). A failure is reported as a MixError naming
	// the verify stage, and the feed is not stored.
	Verify func(src Source, feed *Feed) error
	// Trace observes every stage. It may be called concurrently from multiple
	// goroutines and must be safe for concurrent use.
	Trace func(event TraceEvent)
}

// Batch parses a set of sources concurrently while keeping every source's
// parse state, body buffer and result slot private to that source.
//
// A Batch holds only configuration and is safe for concurrent use. No state is
// carried between Parse calls, so retries, reruns and process restarts all
// start from the same clean state.
type Batch struct {
	opts        BatchOptions
	fetcher     Fetcher
	factory     func(Source) FeedParser
	concurrency int
	maxAttempts int
	batchID     string
}

// NewBatch returns a Batch configured by opts.
func NewBatch(opts BatchOptions) *Batch {
	b := &Batch{opts: opts, batchID: opts.BatchID}

	b.fetcher = opts.Fetcher
	if b.fetcher == nil {
		b.fetcher = HTTPFetcher{}
	}

	b.factory = opts.ParserFactory
	if b.factory == nil {
		b.factory = func(Source) FeedParser { return NewParser() }
	}

	b.concurrency = opts.Concurrency
	if b.concurrency <= 0 {
		b.concurrency = runtime.GOMAXPROCS(0)
	}
	if b.concurrency < 1 {
		b.concurrency = 1
	}

	b.maxAttempts = opts.MaxAttempts
	if b.maxAttempts <= 0 {
		b.maxAttempts = 1
	}
	return b
}

// Parse parses sources concurrently. The returned error is non-nil only for
// configuration mistakes (empty or duplicate source IDs); per-source failures
// live on the results, and BatchResult.Err joins them.
//
// Parse does not retain ctx, sources or results between calls.
func (b *Batch) Parse(ctx context.Context, sources []Source) (*BatchResult, error) {
	if b == nil {
		return nil, errors.New("gofeed: nil Batch")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	seen := make(map[string]struct{}, len(sources))
	for _, src := range sources {
		if strings.TrimSpace(src.ID) == "" {
			return nil, fmt.Errorf("gofeed: source ID for %q is empty; every source needs a unique ID so results can be attributed", src.URL)
		}
		if _, ok := seen[src.ID]; ok {
			return nil, fmt.Errorf("gofeed: duplicate source ID %q; IDs are result keys and must be unique", src.ID)
		}
		seen[src.ID] = struct{}{}
	}

	result := &BatchResult{
		BatchID: b.batchID,
		Results: make([]*SourceResult, len(sources)),
		index:   make(map[string]*SourceResult, len(sources)),
	}
	if len(sources) == 0 {
		return result, nil
	}

	// Ownership registry for this Parse call only. It detects a feed or item
	// pointer sitting in two sources' results, which is the observable shape of
	// the "A's entries ended up under B" failure.
	reg := newOwnershipRegistry()

	workers := b.concurrency
	if workers > len(sources) {
		workers = len(sources)
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				// Each index is written by exactly one worker and each
				// SourceResult is created inside that worker: no result slot is
				// shared between sources.
				result.Results[i] = b.parseSource(ctx, sources[i], reg)
			}
		}()
	}
	for i := range sources {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	for _, sr := range result.Results {
		result.index[sr.Source.ID] = sr
	}
	return result, nil
}

func (b *Batch) parseSource(ctx context.Context, src Source, reg *ownershipRegistry) *SourceResult {
	sr := &SourceResult{Source: src}
	if err := ctx.Err(); err != nil {
		sr.Err = err
		return sr
	}

	var lastErr error
	for attempt := 1; attempt <= b.maxAttempts; attempt++ {
		if attempt > 1 && b.opts.Backoff != nil {
			if delay := b.opts.Backoff(attempt, lastErr); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					sr.Err = ctx.Err()
					return sr
				case <-timer.C:
				}
			}
		}

		feed, trace := b.runAttempt(ctx, src, attempt, reg)
		if trace.Err == nil {
			trace.Err = b.assemble(src, attempt, feed, &trace, reg)
			if trace.Err == nil {
				sr.Feed = feed
			}
		}
		sr.Attempts = append(sr.Attempts, trace)
		if trace.Err == nil {
			return sr
		}

		lastErr = trace.Err
		if err := ctx.Err(); err != nil {
			sr.Err = err
			return sr
		}
	}

	sr.Err = lastErr
	if sr.Err == nil {
		sr.Err = errors.New("gofeed: batch attempt failed without an error")
	}
	return sr
}

// runAttempt executes one isolated fetch/detect/parse/verify cycle. Every byte
// slice and parser it touches is created for this attempt only.
func (b *Batch) runAttempt(ctx context.Context, src Source, attempt int, reg *ownershipRegistry) (feed *Feed, trace AttemptTrace) {
	started := time.Now()
	attemptID := b.attemptID(src.ID, attempt)
	trace = AttemptTrace{ID: attemptID, SourceID: src.ID, Attempt: attempt, Started: started}
	defer func() { trace.Duration = time.Since(started) }()

	record := func(event TraceEvent) {
		event.BatchID = b.batchID
		event.SourceID = src.ID
		event.AttemptID = attemptID
		event.Attempt = attempt
		trace.Stages = append(trace.Stages, event)
		if b.opts.Trace != nil {
			b.opts.Trace(event)
		}
	}

	fetchStart := time.Now()
	body, fetchErr := b.fetcher.Fetch(ctx, src)
	fetchDigest := bodyDigest(body)
	record(TraceEvent{
		Stage:    StageFetch,
		At:       fetchStart,
		Duration: time.Since(fetchStart),
		Bytes:    len(body),
		Digest:   fetchDigest,
		Err:      fetchErr,
	})
	if fetchErr != nil {
		trace.Err = fetchErr
		return nil, trace
	}

	// Copy the body into a buffer owned by this attempt. A Fetcher that reuses
	// or shares a buffer between calls can then only corrupt its own copy, and
	// only at the moment of the copy, which the digest check below catches.
	private := append([]byte(nil), body...)
	if got := bodyDigest(private); got != fetchDigest {
		err := b.mixError(src, attempt, StageDetect,
			"the fetched body changed while it was being copied; the fetcher reuses or shares a body buffer between sources", nil, trace.Stages)
		trace.Err = err
		return nil, trace
	}
	if err := ctx.Err(); err != nil {
		trace.Err = err
		return nil, trace
	}

	detectStart := time.Now()
	feedType := feedTypeName(DetectFeedType(bytes.NewReader(private)))
	record(TraceEvent{
		Stage:    StageDetect,
		At:       detectStart,
		Duration: time.Since(detectStart),
		Bytes:    len(private),
		FeedType: feedType,
	})
	if err := ctx.Err(); err != nil {
		trace.Err = err
		return nil, trace
	}

	parseStart := time.Now()
	parser := b.factory(src)
	if parser == nil {
		err := errors.New("gofeed: ParserFactory returned a nil parser")
		trace.Err = err
		return nil, trace
	}
	parsed, parseErr := parser.Parse(bytes.NewReader(private))
	parseDigest := feedDigest(parsed)
	items := 0
	if parsed != nil {
		items = len(parsed.Items)
	}
	record(TraceEvent{
		Stage:    StageParse,
		At:       parseStart,
		Duration: time.Since(parseStart),
		Bytes:    len(private),
		Digest:   parseDigest,
		FeedType: feedType,
		Items:    items,
		Err:      parseErr,
	})
	if parseErr != nil {
		trace.Err = parseErr
		return nil, trace
	}
	if parsed == nil {
		trace.Err = errors.New("gofeed: feed parser returned a nil feed without an error")
		return nil, trace
	}

	if b.opts.Verify != nil {
		verifyStart := time.Now()
		verr := b.opts.Verify(src, parsed)
		var verifyErr error
		if verr != nil {
			verifyErr = b.mixError(src, attempt, StageVerify,
				"the ownership check rejected the parsed feed: "+verr.Error(), verr, trace.Stages)
		}
		record(TraceEvent{
			Stage:    StageVerify,
			At:       verifyStart,
			Duration: time.Since(verifyStart),
			Digest:   feedDigest(parsed),
			FeedType: feedType,
			Items:    len(parsed.Items),
			Err:      verifyErr,
		})
		if verifyErr != nil {
			trace.Err = verifyErr
			return nil, trace
		}
	}

	return parsed, trace
}

// assemble checks that the feed was not shared with another source, then
// records it as this source's own.
func (b *Batch) assemble(src Source, attempt int, feed *Feed, trace *AttemptTrace, reg *ownershipRegistry) error {
	start := time.Now()
	digest := feedDigest(feed)

	var err error
	if parseDigest, ok := trace.Stage(StageParse); ok && parseDigest.Digest != "" && digest != parseDigest.Digest {
		err = b.mixError(src, attempt, StageAssemble,
			"the parsed feed changed between the parse and assemble stages; the feed value is shared between sources", nil, trace.Stages)
	}
	if err == nil {
		if detail := reg.claim(src.ID, feed); detail != "" {
			err = b.mixError(src, attempt, StageAssemble, detail, nil, trace.Stages)
		}
	}

	b.recordTrace(trace, TraceEvent{
		Stage:    StageAssemble,
		At:       start,
		Duration: time.Since(start),
		Digest:   digest,
		Items:    len(feed.Items),
		Err:      err,
	})
	return err
}

func (b *Batch) recordTrace(trace *AttemptTrace, event TraceEvent) {
	event.BatchID = b.batchID
	event.SourceID = trace.SourceID
	event.AttemptID = trace.ID
	event.Attempt = trace.Attempt
	trace.Stages = append(trace.Stages, event)
	if b.opts.Trace != nil {
		b.opts.Trace(event)
	}
}

// ownershipRegistry records which source owns which *Feed and *Item for one
// Parse call. Two sources claiming the same pointer is the exact "entries
// under the wrong source" shape and is reported as a MixError.
type ownershipRegistry struct {
	mu    sync.Mutex
	feeds map[*Feed]string
	items map[*Item]string
}

func newOwnershipRegistry() *ownershipRegistry {
	return &ownershipRegistry{
		feeds: map[*Feed]string{},
		items: map[*Item]string{},
	}
}

// claim reserves feed and its items for sourceID. It returns a non-empty
// detail when a pointer already belongs to a different source; in that case
// nothing is recorded.
func (r *ownershipRegistry) claim(sourceID string, feed *Feed) string {
	if r == nil || feed == nil {
		return ""
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if owner, ok := r.feeds[feed]; ok && owner != sourceID {
		return fmt.Sprintf("the feed value is already owned by source %q", owner)
	}
	for _, item := range feed.Items {
		if item == nil {
			continue
		}
		if owner, ok := r.items[item]; ok && owner != sourceID {
			return fmt.Sprintf("an item is already owned by source %q (item title %q)", owner, item.Title)
		}
	}

	if _, ok := r.feeds[feed]; !ok {
		r.feeds[feed] = sourceID
	}
	for _, item := range feed.Items {
		if item == nil {
			continue
		}
		if _, ok := r.items[item]; !ok {
			r.items[item] = sourceID
		}
	}
	return ""
}

func (b *Batch) mixError(src Source, attempt int, stage Stage, detail string, cause error, stages []TraceEvent) *MixError {
	return &MixError{
		BatchID:  b.batchID,
		SourceID: src.ID,
		Attempt:  attempt,
		Stage:    stage,
		Detail:   detail,
		Cause:    cause,
		Stages:   append([]TraceEvent(nil), stages...),
	}
}

func (b *Batch) attemptID(sourceID string, attempt int) string {
	if b.batchID == "" {
		return fmt.Sprintf("%s#%d", sourceID, attempt)
	}
	return fmt.Sprintf("%s/%s#%d", b.batchID, sourceID, attempt)
}
