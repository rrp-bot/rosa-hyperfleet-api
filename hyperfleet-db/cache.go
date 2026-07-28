package hyperfleetdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/go-logr/logr"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dyndbu "github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-db/dynamodb"
)

// dynCache implements cache.Cache backed by DynamoDB poll-watch.
type dynCache struct {
	scheme      *runtime.Scheme
	ddb         *dynamodb.Client
	tablePrefix string
	restMapper  apimeta.RESTMapper
	logger      logr.Logger
	shard       *ShardConfig
	unsharded   map[schema.GroupVersionKind]bool

	mu        sync.Mutex
	informers map[schema.GroupVersionKind]*dynInformer
	started   bool
	stopCh    chan struct{}
}

var _ cache.Cache = (*dynCache)(nil)

// --- cache.Reader — direct consistent reads, never hits the local store ---

func (c *dynCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	dc := &dynClient{
		scheme:      c.scheme,
		ddb:         c.ddb,
		tablePrefix: c.tablePrefix,
		restMapper:  c.restMapper,
	}
	return dc.Get(ctx, key, obj, opts...)
}

func (c *dynCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	dc := &dynClient{
		scheme:      c.scheme,
		ddb:         c.ddb,
		tablePrefix: c.tablePrefix,
		restMapper:  c.restMapper,
	}
	return dc.List(ctx, list, opts...)
}

// --- cache.Informers ---

func (c *dynCache) GetInformer(
	ctx context.Context, obj client.Object, opts ...cache.InformerGetOption,
) (cache.Informer, error) {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return nil, err
	}
	return c.getOrCreateInformer(gvk)
}

func (c *dynCache) GetInformerForKind(
	ctx context.Context, gvk schema.GroupVersionKind, opts ...cache.InformerGetOption,
) (cache.Informer, error) {
	return c.getOrCreateInformer(gvk)
}

func (c *dynCache) RemoveInformer(_ context.Context, _ client.Object) error {
	return nil
}

// Start launches all registered informers and blocks until ctx is cancelled.
func (c *dynCache) Start(ctx context.Context) error {
	c.mu.Lock()
	c.started = true
	c.stopCh = make(chan struct{})
	informers := make([]*dynInformer, 0, len(c.informers))
	for _, inf := range c.informers {
		informers = append(informers, inf)
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, inf := range informers {
		wg.Add(1)
		go func(inf *dynInformer) {
			defer wg.Done()
			inf.inner.Run(c.stopCh)
		}(inf)
	}

	<-ctx.Done()

	c.mu.Lock()
	if c.stopCh != nil {
		close(c.stopCh)
		c.stopCh = nil
	}
	c.mu.Unlock()

	wg.Wait()
	return nil
}

// WaitForCacheSync blocks until all registered informers have completed their
// initial List (full consistent scan). Uses toolscache.WaitForCacheSync which
// polls HasSynced.
func (c *dynCache) WaitForCacheSync(ctx context.Context) bool {
	c.mu.Lock()
	syncFuncs := make([]toolscache.InformerSynced, 0, len(c.informers))
	for _, inf := range c.informers {
		syncFuncs = append(syncFuncs, inf.inner.HasSynced)
	}
	c.mu.Unlock()

	return toolscache.WaitForCacheSync(ctx.Done(), syncFuncs...)
}

// IndexField is a no-op — DynamoDB's poll-watch informer does not support
// client-go field indexing. Controllers that need indexing must List directly.
func (c *dynCache) IndexField(_ context.Context, _ client.Object, _ string, _ client.IndexerFunc) error {
	return nil
}

// getOrCreateInformer returns (creating if needed) the SharedIndexInformer for
// the given GVK. The informer uses a ListWatch that:
//   - Lists: full consistent Scan of the GVK's table.
//   - Watches: PollWatcher[client.Object] polling every 15 s for items whose
//     updateTime advanced within the last 5 minutes; closes after 5 min to
//     trigger a full re-list.
func (c *dynCache) getOrCreateInformer(gvk schema.GroupVersionKind) (*dynInformer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if inf, ok := c.informers[gvk]; ok {
		return inf, nil
	}

	tableName, err := tableForGVK(c.tablePrefix, gvk)
	if err != nil {
		return nil, err
	}

	listGVK := schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	}

	// Capture loop variables for the closures.
	capturedGVK := gvk
	capturedListGVK := listGVK
	capturedTable := tableName
	capturedShard := c.shard
	capturedUnsharded := c.unsharded
	capturedDDB := c.ddb
	capturedScheme := c.scheme

	lw := &dyndbu.ListWatchWithoutWatchListSemantics{
		ListWatch: &toolscache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
				listObj, err := capturedScheme.New(capturedListGVK)
				if err != nil {
					return nil, fmt.Errorf("scheme.New(%v): %w", capturedListGVK, err)
				}
				oList, ok := listObj.(client.ObjectList)
				if !ok {
					return nil, fmt.Errorf("type %T does not implement client.ObjectList", listObj)
				}

				input := scanTableInput(capturedTable)
				var objects []client.Object
				paginator := dynamodb.NewScanPaginator(capturedDDB, input)
				for paginator.HasMorePages() {
					page, err := paginator.NextPage(ctx)
					if err != nil {
						return nil, fmt.Errorf("dynCache List %s: %w", capturedTable, err)
					}
					for _, rawItem := range page.Items {
						obj, err := itemToObjectWithGVK(rawItem, capturedGVK, capturedScheme)
						if err != nil {
							return nil, fmt.Errorf("dynCache List %s convert: %w", capturedTable, err)
						}
						// Apply shard filter (skip if not for this replica).
						if !isShardedForGVK(capturedShard, capturedUnsharded, capturedGVK, obj.GetNamespace()) {
							continue
						}
						objects = append(objects, obj)
					}
				}

				if err := setListItems(oList, objects); err != nil {
					return nil, err
				}
				return oList, nil
			},

			WatchFuncWithContext: func(ctx context.Context, _ metav1.ListOptions) (watch.Interface, error) {
				// SinceReader backed directly by DynamoDB Scan with FilterExpression.
				sr := &dynSinceReader{
					ddb:    capturedDDB,
					table:  capturedTable,
					scheme: capturedScheme,
					gvk:    capturedGVK,
					shard:  capturedShard,
					unsharded: capturedUnsharded,
				}
				// ConvertFn: client.Object is already a runtime.Object.
				convertFn := func(obj *client.Object) (runtime.Object, error) {
					return *obj, nil
				}
				return dyndbu.NewPollWatcher[client.Object](
					ctx,
					capturedTable,
					sr,
					convertFn,
					dyndbu.DefaultPollInterval,
					dyndbu.DefaultWatchDuration,
				), nil
			},
		},
	}

	exampleObj, err := c.scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("scheme.New(%v): %w", gvk, err)
	}

	si := toolscache.NewSharedIndexInformerWithOptions(lw, exampleObj, toolscache.SharedIndexInformerOptions{
		ObjectDescription: gvk.Kind,
	})

	inf := &dynInformer{inner: si}
	c.informers[gvk] = inf

	c.mu.Unlock()
	defer c.mu.Lock()

	if c.started && c.stopCh != nil {
		go si.Run(c.stopCh)
	}

	return inf, nil
}

// isShardedForGVK returns true if namespace belongs to the owned shard, or if
// the GVK is in the unsharded set, or if sharding is disabled.
func isShardedForGVK(shard *ShardConfig, unsharded map[schema.GroupVersionKind]bool, gvk schema.GroupVersionKind, namespace string) bool {
	if shard == nil {
		return true
	}
	if unsharded[gvk] {
		return true
	}
	return shard.matches(namespace)
}

// --- dynSinceReader implements dyndbu.SinceReader[client.Object] ---

type dynSinceReader struct {
	ddb       *dynamodb.Client
	table     string
	scheme    *runtime.Scheme
	gvk       schema.GroupVersionKind
	shard     *ShardConfig
	unsharded map[schema.GroupVersionKind]bool
}

func (r *dynSinceReader) ListSince(ctx context.Context, since time.Time) ([]*client.Object, error) {
	input := scanSinceInput(r.table, since)
	var result []*client.Object
	paginator := dynamodb.NewScanPaginator(r.ddb, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("dynCache ListSince %s: %w", r.table, err)
		}
		for _, rawItem := range page.Items {
			obj, err := itemToObjectWithGVK(rawItem, r.gvk, r.scheme)
			if err != nil {
				continue // skip unconvertible items; watcher logs this
			}
			if !isShardedForGVK(r.shard, r.unsharded, r.gvk, obj.GetNamespace()) {
				continue
			}
			o := client.Object(obj)
			result = append(result, &o)
		}
	}
	return result, nil
}

// --- dynInformer delegates to toolscache.SharedIndexInformer ---

type dynInformer struct {
	inner toolscache.SharedIndexInformer
}

var _ cache.Informer = (*dynInformer)(nil)

func (inf *dynInformer) AddEventHandler(
	handler toolscache.ResourceEventHandler,
) (toolscache.ResourceEventHandlerRegistration, error) {
	return inf.inner.AddEventHandler(handler)
}

func (inf *dynInformer) AddEventHandlerWithResyncPeriod(
	handler toolscache.ResourceEventHandler, resyncPeriod time.Duration,
) (toolscache.ResourceEventHandlerRegistration, error) {
	return inf.inner.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
}

func (inf *dynInformer) AddEventHandlerWithOptions(
	handler toolscache.ResourceEventHandler, opts toolscache.HandlerOptions,
) (toolscache.ResourceEventHandlerRegistration, error) {
	return inf.inner.AddEventHandlerWithOptions(handler, opts)
}

func (inf *dynInformer) RemoveEventHandler(handle toolscache.ResourceEventHandlerRegistration) error {
	return inf.inner.RemoveEventHandler(handle)
}

func (inf *dynInformer) AddIndexers(indexers toolscache.Indexers) error {
	return inf.inner.AddIndexers(indexers)
}

func (inf *dynInformer) HasSynced() bool {
	return inf.inner.HasSynced()
}

func (inf *dynInformer) HasSyncedChecker() toolscache.DoneChecker {
	return inf.inner.HasSyncedChecker()
}

func (inf *dynInformer) IsStopped() bool {
	return inf.inner.IsStopped()
}
