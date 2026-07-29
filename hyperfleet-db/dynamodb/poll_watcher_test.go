package dynamodb

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// --- fakeReader ---

// fakeReader implements SinceReader[string]. It records calls and returns
// a configurable list of items or error.
type fakeReader struct {
	callCount int
	items     []*string
	err       error
}

func (f *fakeReader) ListSince(_ context.Context, _ time.Time) ([]*string, error) {
	f.callCount++
	return f.items, f.err
}

// --- fakeObject ---

// fakeObject is a minimal runtime.Object for use in watch events.
// We use *corev1.ConfigMap because it is a concrete type that already
// satisfies runtime.Object — avoids writing a hand-rolled stub.
type fakeObject = corev1.ConfigMap

// --- TestPollWatcher_ConvertErrorIsSkipped ---

// If convert returns an error for one item, that item must be dropped and the
// next item must still be delivered. A convert error must never be fatal.
func TestPollWatcher_ConvertErrorIsSkipped(t *testing.T) {
	good := "good"
	bad := "bad"

	reader := &fakeReader{
		items: []*string{&bad, &good},
	}

	callCount := 0
	convert := func(s *string) (runtime.Object, error) {
		callCount++
		if *s == "bad" {
			return nil, errors.New("convert error")
		}
		obj := &fakeObject{}
		return obj, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w := NewPollWatcher[string](
		ctx,
		"test-table",
		reader,
		convert,
		10*time.Millisecond, // fast poll for the test
		30*time.Second,      // long watch duration — we stop it manually
	)
	defer w.Stop()

	// Wait for at least one Modified event (the "good" item).
	select {
	case ev, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("result channel closed before receiving any events")
		}
		if ev.Type != watch.Modified {
			t.Errorf("expected Modified event, got %v", ev.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a watch event; convert errors may be fatal")
	}

	// Verify convert was called at least twice (once for "bad", once for "good").
	if callCount < 2 {
		t.Errorf("expected convert to be called for both items, got %d calls", callCount)
	}
}

// --- TestPollWatcher_ScanErrorDoesNotHotSpin ---

// If ListSince returns an error on every call, the watcher must still pace
// itself using the ticker — not busy-loop. We verify by counting calls over a
// fixed duration and asserting the call rate is bounded.
func TestPollWatcher_ScanErrorDoesNotHotSpin(t *testing.T) {
	reader := &fakeReader{
		err: errors.New("simulated scan error"),
	}

	convert := func(_ *string) (runtime.Object, error) {
		return &fakeObject{}, nil
	}

	pollInterval := 20 * time.Millisecond
	measureDuration := 120 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), measureDuration)
	defer cancel()

	w := NewPollWatcher[string](
		ctx,
		"test-table",
		reader,
		convert,
		pollInterval,
		30*time.Second,
	)
	defer w.Stop()

	// Wait for the context to expire (and therefore the watcher to finish).
	<-ctx.Done()
	w.Stop()

	// With a 20 ms ticker over 120 ms we expect at most ~6 ticks.
	// A hot-spinning watcher would produce hundreds of calls.
	// We allow 2× headroom over the theoretical maximum.
	maxExpected := int(measureDuration/pollInterval)*2 + 2
	if reader.callCount > maxExpected {
		t.Errorf("scan error caused hot spin: %d calls in %v (max expected %d)",
			reader.callCount, measureDuration, maxExpected)
	}
	if reader.callCount == 0 {
		t.Error("ListSince was never called — watcher did not poll")
	}
}

// --- TestPollWatcher_ClosesAfterWatchDuration ---

// The watcher result channel must be closed after watchDuration has elapsed,
// signalling the informer to perform a full relist.
func TestPollWatcher_ClosesAfterWatchDuration(t *testing.T) {
	reader := &fakeReader{items: nil}
	convert := func(_ *string) (runtime.Object, error) { return &fakeObject{}, nil }

	watchDuration := 50 * time.Millisecond

	ctx := context.Background()
	w := NewPollWatcher[string](
		ctx,
		"test-table",
		reader,
		convert,
		10*time.Millisecond,
		watchDuration,
	)

	// Drain events until the channel is closed.
	deadline := time.After(watchDuration * 10)
	for {
		select {
		case _, ok := <-w.ResultChan():
			if !ok {
				// Channel closed — correct behaviour.
				return
			}
		case <-deadline:
			t.Fatal("result channel was not closed after watchDuration elapsed")
		}
	}
}

// --- TestPollWatcher_StopCancelsCleanly ---

// Calling Stop must cause the watcher to terminate without blocking.
func TestPollWatcher_StopCancelsCleanly(t *testing.T) {
	reader := &fakeReader{}
	convert := func(_ *string) (runtime.Object, error) { return &fakeObject{}, nil }

	ctx := context.Background()
	w := NewPollWatcher[string](
		ctx,
		"test-table",
		reader,
		convert,
		100*time.Millisecond,
		60*time.Second,
	)

	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Clean stop — success.
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() blocked for more than 3 seconds")
	}
}

// --- TestListWatchWithoutWatchListSemantics ---

// IsWatchListSemanticsUnSupported must return true so client-go's Reflector
// does not wait for a bookmark event that DynamoDB will never emit.
func TestListWatchWithoutWatchListSemantics_IsUnsupported(t *testing.T) {
	lw := ListWatchWithoutWatchListSemantics{
		ListWatch: &cache.ListWatch{},
	}
	if !lw.IsWatchListSemanticsUnSupported() {
		t.Error("IsWatchListSemanticsUnSupported() returned false; " +
			"WatchList mode will be attempted and WaitForCacheSync will block forever")
	}
}
