package statusstream

import (
	"context"
	"sync"
	"testing"
	"time"

	"log/slog"
)

// fakeStatusReader is a test double for statusReader.
type fakeStatusReader struct {
	mu      sync.Mutex
	results []string
	calls   []time.Time // records the `since` argument of each ListSince call
	err     error
}

func (f *fakeStatusReader) ListSince(_ context.Context, _ string, since time.Time) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, since)
	if f.err != nil {
		return nil, f.err
	}
	return f.results, nil
}

func newFakeReader(ids ...string) *fakeStatusReader {
	return &fakeStatusReader{results: ids}
}

func testLogger() *slog.Logger {
	return slog.Default()
}

// TestPollWatcher_FiresOnChangeForUpdatedItems asserts that onChange is called
// once per documentID returned by ListSince.
func TestPollWatcher_FiresOnChangeForUpdatedItems(t *testing.T) {
	reader := newFakeReader("doc-aaa", "doc-bbb")

	var mu sync.Mutex
	var received []string
	onChange := func(id string) {
		mu.Lock()
		received = append(received, id)
		mu.Unlock()
	}

	pw := newPollWatcher(reader, "test-table", onChange, 10*time.Millisecond, time.Minute, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pw.Run(ctx)

	// Wait for at least one poll cycle.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	pw.Stop()

	mu.Lock()
	defer mu.Unlock()

	if len(received) < 2 {
		t.Fatalf("expected at least 2 onChange calls, got %d", len(received))
	}
	seen := make(map[string]bool)
	for _, id := range received {
		seen[id] = true
	}
	for _, want := range []string{"doc-aaa", "doc-bbb"} {
		if !seen[want] {
			t.Errorf("expected onChange to be called with %q", want)
		}
	}
}

// TestPollWatcher_ClosesAfterWatchDuration asserts that the watcher's doneCh
// is closed after watchDuration elapses without calling Stop.
func TestPollWatcher_ClosesAfterWatchDuration(t *testing.T) {
	reader := newFakeReader()
	pw := newPollWatcher(reader, "test-table", func(string) {}, 10*time.Millisecond, 50*time.Millisecond, testLogger())

	ctx := context.Background()
	go pw.Run(ctx)

	select {
	case <-pw.Done():
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("expected watcher to close after watchDuration, but it did not")
	}
}

// TestPollWatcher_StopCancelsGracefully asserts that calling Stop causes the
// watcher to exit cleanly without leaking a goroutine.
func TestPollWatcher_StopCancelsGracefully(t *testing.T) {
	reader := newFakeReader()
	// Long watchDuration so it won't self-close during the test.
	pw := newPollWatcher(reader, "test-table", func(string) {}, 10*time.Millisecond, time.Hour, testLogger())

	ctx := context.Background()
	go pw.Run(ctx)

	// Give it time to start.
	time.Sleep(30 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		pw.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Stop returned, watcher is gone.
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within timeout — possible goroutine leak")
	}
}

// TestPollWatcher_LookbackWindowIsWatchDuration asserts that ListSince is
// called with a `since` value approximately equal to now - watchDuration.
func TestPollWatcher_LookbackWindowIsWatchDuration(t *testing.T) {
	watchDuration := 5 * time.Minute
	reader := newFakeReader()

	pw := newPollWatcher(reader, "test-table", func(string) {}, 10*time.Millisecond, watchDuration, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pw.Run(ctx)

	// Wait for at least one poll call.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reader.mu.Lock()
		n := len(reader.calls)
		reader.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	pw.Stop()

	reader.mu.Lock()
	calls := reader.calls
	reader.mu.Unlock()

	if len(calls) == 0 {
		t.Fatal("expected at least one ListSince call")
	}

	// The since argument should be approximately now - watchDuration.
	// Allow ±5s tolerance for test execution time.
	since := calls[0]
	expectedSince := time.Now().UTC().Add(-watchDuration)
	diff := expectedSince.Sub(since)
	if diff < 0 {
		diff = -diff
	}
	if diff > 5*time.Second {
		t.Errorf("ListSince called with since=%v, expected approximately %v (diff=%v)",
			since, expectedSince, diff)
	}
}

// TestPollWatcher_IgnoresEmptyResult asserts that onChange is never called
// when ListSince returns an empty slice.
func TestPollWatcher_IgnoresEmptyResult(t *testing.T) {
	reader := newFakeReader() // returns no IDs

	called := false
	onChange := func(string) { called = true }

	pw := newPollWatcher(reader, "test-table", onChange, 10*time.Millisecond, time.Hour, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pw.Run(ctx)

	// Wait for several poll cycles.
	time.Sleep(100 * time.Millisecond)
	pw.Stop()

	if called {
		t.Error("onChange should not be called when ListSince returns empty")
	}
}
