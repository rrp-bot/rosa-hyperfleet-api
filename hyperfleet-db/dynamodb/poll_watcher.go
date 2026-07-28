package dynamodb

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// DefaultPollInterval is how often the quick poll queries DynamoDB for
	// recently updated items.
	DefaultPollInterval = 15 * time.Second

	// DefaultWatchDuration is how long a PollWatcher runs before closing,
	// causing the caller (e.g. SharedIndexInformer) to trigger a full
	// consistent re-list followed by a fresh PollWatcher. This is the
	// mechanism that gives the 5-minute unconditional relist guarantee.
	DefaultWatchDuration = 5 * time.Minute
)

// SinceReader is the minimal interface PollWatcher requires from the database
// layer. It is satisfied by *CRUD[T, PT] but is kept as a separate interface
// so PollWatcher can be tested without a real DynamoDB client.
type SinceReader[T any] interface {
	ListSince(ctx context.Context, since time.Time) ([]*T, error)
}

// ConvertFn converts a typed value into a runtime.Object suitable for
// delivery to the SharedIndexInformer event channel.
type ConvertFn[T any] func(*T) (runtime.Object, error)

// PollWatcher implements watch.Interface. It polls DynamoDB on a fixed
// interval for items whose updateTime is within the lookback window, sending
// Modified events to the result channel. After WatchDuration it closes the
// result channel, which causes the SharedIndexInformer to perform a full
// consistent re-list and then call Watch again.
//
// Correctness model:
//   - The quick poll (every PollInterval) surfaces recently changed items with
//     low latency. It uses eventually consistent reads and a lookback window
//     equal to WatchDuration to absorb any clock skew or propagation delay.
//   - The full re-list (triggered when this watcher closes) fetches every item
//     with a consistent Scan, guaranteeing nothing is permanently missed.
//   - Controllers always call Get (ConsistentRead=true) before acting, so
//     Modified events here are purely a notification mechanism.
type PollWatcher[T any] struct {
	resultCh chan watch.Event
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewPollWatcher creates and starts a PollWatcher. It immediately launches a
// background goroutine; callers should call Stop to release resources.
func NewPollWatcher[T any](
	ctx context.Context,
	tableName string,
	reader SinceReader[T],
	convert ConvertFn[T],
	pollInterval time.Duration,
	watchDuration time.Duration,
) *PollWatcher[T] {
	ctx, cancel := context.WithCancel(ctx)
	w := &PollWatcher[T]{
		resultCh: make(chan watch.Event, 100),
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	go w.run(ctx, tableName, reader, convert, pollInterval, watchDuration)
	return w
}

func (w *PollWatcher[T]) run(
	ctx context.Context,
	tableName string,
	reader SinceReader[T],
	convert ConvertFn[T],
	pollInterval time.Duration,
	watchDuration time.Duration,
) {
	defer close(w.done)
	defer close(w.resultCh)

	klog.V(4).InfoS("poll watcher starting", "table", tableName,
		"pollInterval", pollInterval, "watchDuration", watchDuration)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// After watchDuration the watcher closes, causing the informer to re-list.
	deadline := time.NewTimer(watchDuration)
	defer deadline.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.V(4).InfoS("poll watcher stopping (context cancelled)", "table", tableName)
			return

		case <-deadline.C:
			// Intentional close — the informer will perform a full re-list.
			klog.V(4).InfoS("poll watcher closing to trigger re-list", "table", tableName)
			return

		case <-ticker.C:
			w.poll(ctx, tableName, reader, convert, watchDuration)
		}
	}
}

func (w *PollWatcher[T]) poll(
	ctx context.Context,
	tableName string,
	reader SinceReader[T],
	convert ConvertFn[T],
	lookback time.Duration,
) {
	since := time.Now().UTC().Add(-lookback)
	items, err := reader.ListSince(ctx, since)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		klog.V(2).InfoS("poll watcher scan error", "table", tableName, "err", err)
		return
	}

	klog.V(5).InfoS("poll watcher scan complete", "table", tableName, "items", len(items))

	for _, item := range items {
		obj, err := convert(item)
		if err != nil {
			klog.V(4).InfoS("poll watcher skipping unconvertible item",
				"table", tableName, "err", err)
			continue
		}
		select {
		case w.resultCh <- watch.Event{Type: watch.Modified, Object: obj}:
		case <-ctx.Done():
			return
		}
	}
}

// Stop cancels the poll watcher and waits for it to exit cleanly.
func (w *PollWatcher[T]) Stop() {
	w.cancel()
	<-w.done
}

// ResultChan returns the channel on which watch.Events are delivered.
func (w *PollWatcher[T]) ResultChan() <-chan watch.Event {
	return w.resultCh
}

// -------------------------------------------------------------------
// ListWatchWithoutWatchListSemantics
// -------------------------------------------------------------------

// ListWatchWithoutWatchListSemantics wraps a cache.ListWatch to opt out of
// client-go's WatchList streaming mode. DynamoDB does not emit bookmark
// events, which WatchList mode requires. Without this wrapper, the Reflector
// waits for a bookmark that never arrives and WaitForCacheSync blocks forever.
type ListWatchWithoutWatchListSemantics struct {
	*cache.ListWatch
}

// IsWatchListSemanticsUnSupported implements the optional interface checked by
// client-go's Reflector to disable WatchList negotiation.
func (ListWatchWithoutWatchListSemantics) IsWatchListSemanticsUnSupported() bool { return true }
