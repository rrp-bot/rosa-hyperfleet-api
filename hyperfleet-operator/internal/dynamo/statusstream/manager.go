package statusstream

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hyperfleetv1alpha1 "github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-operator/api/v1alpha1"
)

type watcherHandle struct {
	watcher *PollWatcher
	cancel  context.CancelFunc
}

// Manager discovers management clusters and runs one PollWatcher per MC per
// table suffix. It polls the MC list periodically to start watchers for new
// MCs and stop watchers for removed MCs. Watchers that close after their
// watchDuration are automatically restarted, providing the unconditional
// relist guarantee.
type Manager struct {
	dbClient      *dynamodb.Client
	mcReader      client.Reader
	tableSuffixes []string
	onChange      OnChange
	logger        *slog.Logger
}

func NewManager(
	dbClient *dynamodb.Client,
	mcReader client.Reader,
	tableSuffixes []string,
	onChange OnChange,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		dbClient:      dbClient,
		mcReader:      mcReader,
		tableSuffixes: tableSuffixes,
		onChange:      onChange,
		logger:        logger,
	}
}

// Run blocks until ctx is canceled. It polls the MC list every interval and
// ensures one PollWatcher goroutine runs per (MC, table suffix) pair.
// Watchers that naturally close (watchDuration elapsed) are restarted.
func (m *Manager) Run(ctx context.Context, interval time.Duration) {
	active := make(map[string]watcherHandle)

	defer func() {
		for _, h := range active {
			h.cancel()
		}
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	m.syncWatchers(ctx, active)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.syncWatchers(ctx, active)
		}
	}
}

func (m *Manager) syncWatchers(ctx context.Context, active map[string]watcherHandle) {
	var list hyperfleetv1alpha1.ManagementClusterList
	if err := m.mcReader.List(ctx, &list); err != nil {
		m.logger.Error("failed to list ManagementCluster CRs", "error", err)
		return
	}

	desired := make(map[string]struct{}, len(list.Items)*len(m.tableSuffixes))
	for _, mc := range list.Items {
		for _, suffix := range m.tableSuffixes {
			desired[mc.Name+suffix] = struct{}{}
		}
	}

	// Stop watchers for removed MCs.
	for key, h := range active {
		if _, ok := desired[key]; !ok {
			m.logger.Info("stopping status poll watcher", "key", key)
			h.cancel()
			delete(active, key)
		}
	}

	for _, mc := range list.Items {
		if strings.HasPrefix(mc.Name, "test-mc-") {
			continue
		}
		for _, suffix := range m.tableSuffixes {
			key := mc.Name + suffix
			tableName := mc.Name + suffix

			h, running := active[key]
			if running {
				// Check if the watcher exited naturally (watchDuration elapsed).
				select {
				case <-h.watcher.Done():
					m.logger.Info("restarting poll watcher after watchDuration", "key", key)
					h.cancel()
					delete(active, key)
				default:
					// Still running — nothing to do.
					continue
				}
			}

			// Start a new watcher.
			watcherCtx, cancel := context.WithCancel(ctx)
			pw := NewPollWatcher(m.dbClient, tableName, m.onChange, m.logger)
			active[key] = watcherHandle{watcher: pw, cancel: cancel}
			m.logger.Info("starting status poll watcher", "mc", mc.Name, "table", tableName)
			go pw.Run(watcherCtx)
		}
	}
}
