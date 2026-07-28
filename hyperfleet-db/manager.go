// Package hyperfleetdb provides a DynamoDB-backed controller-runtime Manager
// and client.Client for the hyperfleet-api monorepo.
//
// It implements the same controller-runtime interfaces (manager.Manager,
// client.Client, cache.Cache) that the PostgreSQL-backed pgruntime formerly
// provided, allowing controllers and platform-api to remain unchanged while
// the backing store moves to DynamoDB.
//
// The generic DynamoDB CRUD and poll-watch primitives live in the
// [github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-db/dynamodb]
// sub-package, which has zero dependency on controller-runtime and is
// designed to be importable by kube-applier-aws.
package hyperfleetdb

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/go-logr/logr"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/conversion"
)

// ShardConfig partitions the cache's informer List/Watch streams across
// replicas by hashtext(namespace) % Mod. The direct client and cache reader
// are never sharded. UnshardedGVKs lists GVKs that all replicas should see
// (e.g. cluster-scoped resources).
type ShardConfig struct {
	Mod           int
	Owned         []int
	UnshardedGVKs []schema.GroupVersionKind
}

// matches reports whether namespace belongs to one of the owned shards.
// Uses FNV-32a, which matches PostgreSQL's hashtext() for ASCII strings.
func (s *ShardConfig) matches(namespace string) bool {
	if s == nil {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(namespace))
	shard := int(h.Sum32()) % s.Mod
	for _, owned := range s.Owned {
		if shard == owned {
			return true
		}
	}
	return false
}

// Options configures a DynamoDB-backed controller-runtime Manager.
type Options struct {
	// Scheme must include all GVKs the manager will handle.
	Scheme *runtime.Scheme

	// DynamoDB is the AWS DynamoDB client to use for all table operations.
	DynamoDB *dynamodb.Client

	// TablePrefix is prepended to every CRD table name.
	// E.g. "rc01-" yields tables "rc01-clusters", "rc01-nodepools", etc.
	TablePrefix string

	// Shard configures namespace-hash sharding across StatefulSet replicas.
	// nil means no sharding (all objects visible to this replica).
	Shard *ShardConfig

	// Logger is used for manager-level log output.
	Logger logr.Logger

	// HealthProbeBindAddress is the address for the /healthz and /readyz endpoints.
	// Empty means no health probe server.
	HealthProbeBindAddress string
}

// NewManager creates a controller-runtime Manager backed by DynamoDB.
func NewManager(opts Options) (manager.Manager, error) {
	if opts.Scheme == nil {
		return nil, fmt.Errorf("hyperfleetdb: Scheme is required")
	}
	if opts.DynamoDB == nil {
		return nil, fmt.Errorf("hyperfleetdb: DynamoDB client is required")
	}
	if opts.Logger.GetSink() == nil {
		opts.Logger = logr.Discard()
	}

	unsharded := map[schema.GroupVersionKind]bool{}
	if opts.Shard != nil {
		for _, gvk := range opts.Shard.UnshardedGVKs {
			unsharded[gvk] = true
		}
	}

	restMapper := buildRESTMapper(opts.Scheme)

	dc := &dynClient{
		scheme:      opts.Scheme,
		ddb:         opts.DynamoDB,
		tablePrefix: opts.TablePrefix,
		restMapper:  restMapper,
	}

	dca := &dynCache{
		scheme:      opts.Scheme,
		ddb:         opts.DynamoDB,
		tablePrefix: opts.TablePrefix,
		restMapper:  restMapper,
		logger:      opts.Logger.WithName("cache"),
		shard:       opts.Shard,
		unsharded:   unsharded,
		informers:   make(map[schema.GroupVersionKind]*dynInformer),
	}

	elected := make(chan struct{})
	close(elected)

	return &dynManager{
		scheme:        opts.Scheme,
		dc:            dc,
		dca:           dca,
		restMapper:    restMapper,
		logger:        opts.Logger,
		opts:          opts,
		elected:       elected,
		healthzChecks: make(map[string]healthz.Checker),
		readyzChecks:  make(map[string]healthz.Checker),
	}, nil
}

// NewClient creates a standalone client.Client backed by DynamoDB, without
// the manager/cache/watch infrastructure. Intended for stateless HTTP services
// (platform-api) that need CRUD access.
// The returned function is a no-op (the AWS SDK has no connection to close)
// but is provided for API compatibility with the former NewClient signature.
func NewClient(opts Options) (client.Client, func(), error) {
	if opts.Scheme == nil {
		return nil, nil, fmt.Errorf("hyperfleetdb: Scheme is required")
	}
	if opts.DynamoDB == nil {
		return nil, nil, fmt.Errorf("hyperfleetdb: DynamoDB client is required")
	}

	restMapper := buildRESTMapper(opts.Scheme)

	dc := &dynClient{
		scheme:      opts.Scheme,
		ddb:         opts.DynamoDB,
		tablePrefix: opts.TablePrefix,
		restMapper:  restMapper,
	}
	return dc, func() {}, nil
}

// --- dynManager ---

type dynManager struct {
	scheme     *runtime.Scheme
	dc         *dynClient
	dca        *dynCache
	restMapper apimeta.RESTMapper
	logger     logr.Logger
	opts       Options
	elected    chan struct{}

	mu        sync.Mutex
	runnables []manager.Runnable

	healthzChecks map[string]healthz.Checker
	readyzChecks  map[string]healthz.Checker
}

// cluster.Cluster

func (m *dynManager) GetHTTPClient() *http.Client {
	panic("hyperfleetdb: no HTTP client — DynamoDB backend has no kube-apiserver")
}
func (m *dynManager) GetConfig() *rest.Config {
	panic("hyperfleetdb: no rest.Config — DynamoDB backend has no kube-apiserver")
}
func (m *dynManager) GetCache() cache.Cache                { return m.dca }
func (m *dynManager) GetScheme() *runtime.Scheme           { return m.scheme }
func (m *dynManager) GetClient() client.Client             { return m.dc }
func (m *dynManager) GetFieldIndexer() client.FieldIndexer { return m.dca }
func (m *dynManager) GetRESTMapper() apimeta.RESTMapper    { return m.restMapper }
func (m *dynManager) GetAPIReader() client.Reader          { return m.dc }
func (m *dynManager) GetEventRecorderFor(_ string) record.EventRecorder {
	return &noopEventRecorder{}
}
func (m *dynManager) GetEventRecorder(_ string) events.EventRecorder {
	return &noopEventsRecorder{}
}

// manager.Manager

func (m *dynManager) Add(runnable manager.Runnable) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runnables = append(m.runnables, runnable)
	return nil
}
func (m *dynManager) Elected() <-chan struct{} { return m.elected }
func (m *dynManager) AddMetricsServerExtraHandler(_ string, _ http.Handler) error { return nil }
func (m *dynManager) AddHealthzCheck(name string, check healthz.Checker) error {
	m.healthzChecks[name] = check
	return nil
}
func (m *dynManager) AddReadyzCheck(name string, check healthz.Checker) error {
	m.readyzChecks[name] = check
	return nil
}

func (m *dynManager) Start(ctx context.Context) error {
	m.logger.Info("starting hyperfleetdb manager")

	var wg sync.WaitGroup

	if addr := m.opts.HealthProbeBindAddress; addr != "" {
		srv := m.buildHealthProbeServer(addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.logger.Info("starting health probe server", "addr", addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				m.logger.Error(err, "health probe server failed")
			}
		}()
		go func() {
			<-ctx.Done()
			_ = srv.Close()
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.dca.Start(ctx); err != nil {
			m.logger.Error(err, "cache start failed")
		}
	}()

	if !m.dca.WaitForCacheSync(ctx) {
		return fmt.Errorf("hyperfleetdb: cache sync failed")
	}
	m.logger.Info("cache synced")

	m.mu.Lock()
	runnables := make([]manager.Runnable, len(m.runnables))
	copy(runnables, m.runnables)
	m.mu.Unlock()

	for _, r := range runnables {
		wg.Add(1)
		go func(r manager.Runnable) {
			defer wg.Done()
			if err := r.Start(ctx); err != nil {
				m.logger.Error(err, "runnable exited with error")
			}
		}(r)
	}

	<-ctx.Done()
	m.logger.Info("shutting down hyperfleetdb manager")
	wg.Wait()
	return nil
}

func (m *dynManager) buildHealthProbeServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.runChecks(w, r, m.healthzChecks)
	}))
	mux.Handle("/readyz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.runChecks(w, r, m.readyzChecks)
	}))
	return &http.Server{Addr: addr, Handler: mux}
}

func (m *dynManager) runChecks(w http.ResponseWriter, _ *http.Request, checks map[string]healthz.Checker) {
	for name, check := range checks {
		if err := check(nil); err != nil {
			http.Error(w, fmt.Sprintf("check %q failed: %v", name, err), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, "ok")
}

func (m *dynManager) GetWebhookServer() webhook.Server {
	panic("hyperfleetdb: webhooks not supported")
}
func (m *dynManager) GetLogger() logr.Logger                    { return m.logger }
func (m *dynManager) GetControllerOptions() config.Controller   { return config.Controller{} }
func (m *dynManager) GetConverterRegistry() conversion.Registry { return conversion.NewRegistry() }

var _ manager.Manager = (*dynManager)(nil)

// --- helpers ---

type noopEventRecorder struct{}

func (r *noopEventRecorder) Event(_ runtime.Object, _, _, _ string) {}
func (r *noopEventRecorder) Eventf(_ runtime.Object, _, _, _ string, _ ...any) {}
func (r *noopEventRecorder) AnnotatedEventf(_ runtime.Object, _ map[string]string, _, _, _ string, _ ...any) {
}

type noopEventsRecorder struct{}

func (r *noopEventsRecorder) Eventf(_, _ runtime.Object, _, _, _, _ string, _ ...any) {}
