package statusstream

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	defaultPollInterval  = 15 * time.Second
	defaultWatchDuration = 5 * time.Minute
)

// OnChange is called when a status item is found to have been recently updated.
// documentID is the partition key of the changed item.
type OnChange func(documentID string)

// statusReader is the minimal interface PollWatcher needs from the database
// layer. It is kept unexported so it can be satisfied by a test fake without
// pulling in a real DynamoDB client.
type statusReader interface {
	// ListSince returns the documentIDs of status items whose updateTime
	// attribute is strictly after since. Only documentID is projected; the
	// caller (controller reconcile loop) does a consistent GetItem before
	// acting on the result.
	ListSince(ctx context.Context, tableName string, since time.Time) ([]string, error)
}

// dynamoStatusReader implements statusReader against a real DynamoDB table.
type dynamoStatusReader struct {
	client *dynamodb.Client
}

func newDynamoStatusReader(client *dynamodb.Client) statusReader {
	return &dynamoStatusReader{client: client}
}

// ListSince scans the table for items updated after since, projecting only
// documentID. The scan is eventually consistent — these events are a doorbell
// only; controllers always do a consistent GetItem before acting.
func (r *dynamoStatusReader) ListSince(ctx context.Context, tableName string, since time.Time) ([]string, error) {
	var ids []string
	paginator := dynamodb.NewScanPaginator(r.client, &dynamodb.ScanInput{
		TableName:                aws.String(tableName),
		ConsistentRead:           aws.Bool(false),
		FilterExpression:         aws.String("updateTime > :since"),
		ProjectionExpression:     aws.String("documentID"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":since": &dynamodbtypes.AttributeValueMemberS{
				Value: since.UTC().Format(time.RFC3339),
			},
		},
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			av, ok := item["documentID"]
			if !ok {
				continue
			}
			sv, ok := av.(*dynamodbtypes.AttributeValueMemberS)
			if !ok {
				continue
			}
			ids = append(ids, sv.Value)
		}
	}
	return ids, nil
}

// PollWatcher polls a single DynamoDB status table for recently updated items
// using a timestamp-based scan, calling onChange for each documentID found.
//
// After watchDuration the watcher returns, signalling the Manager to restart
// it — this restart acts as the unconditional relist (any items updated but
// missed by the poll window will be caught because the Manager calls
// syncWatchers which re-evaluates all active MCs).
//
// Correctness model:
//   - The quick poll (every pollInterval) surfaces recently changed items with
//     low latency. It uses an eventually consistent Scan with a lookback
//     window equal to watchDuration to absorb any clock skew.
//   - The watcher close + restart cycle guarantees no item is permanently
//     missed: on restart, the new watcher's first poll covers the full
//     watchDuration lookback from now.
//   - Controllers always call GetItem (ConsistentRead=true) before acting, so
//     the onChange events here are purely a notification mechanism.
type PollWatcher struct {
	reader       statusReader
	tableName    string
	onChange     OnChange
	pollInterval time.Duration
	watchDuration time.Duration
	logger       *slog.Logger
	stopCh       chan struct{}
	doneCh       chan struct{}
}

// NewPollWatcher creates a PollWatcher backed by a real DynamoDB client.
func NewPollWatcher(
	client *dynamodb.Client,
	tableName string,
	onChange OnChange,
	logger *slog.Logger,
) *PollWatcher {
	return newPollWatcher(
		newDynamoStatusReader(client),
		tableName,
		onChange,
		defaultPollInterval,
		defaultWatchDuration,
		logger,
	)
}

// newPollWatcher is the internal constructor used by tests to inject a fake reader
// and custom intervals.
func newPollWatcher(
	reader statusReader,
	tableName string,
	onChange OnChange,
	pollInterval time.Duration,
	watchDuration time.Duration,
	logger *slog.Logger,
) *PollWatcher {
	return &PollWatcher{
		reader:        reader,
		tableName:     tableName,
		onChange:      onChange,
		pollInterval:  pollInterval,
		watchDuration: watchDuration,
		logger:        logger.With("table", tableName),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// Run blocks until Stop is called or watchDuration elapses, polling the table
// on each pollInterval tick. It is safe to call from a goroutine; the Manager
// calls it in one.
func (w *PollWatcher) Run(ctx context.Context) {
	defer close(w.doneCh)

	w.logger.Info("poll watcher starting",
		"pollInterval", w.pollInterval,
		"watchDuration", w.watchDuration,
	)

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	deadline := time.NewTimer(w.watchDuration)
	defer deadline.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("poll watcher stopping (context cancelled)")
			return
		case <-w.stopCh:
			w.logger.Info("poll watcher stopping (Stop called)")
			return
		case <-deadline.C:
			// Intentional close — the Manager detects doneCh and restarts.
			w.logger.Info("poll watcher closing to trigger restart")
			return
		case <-ticker.C:
			w.poll(ctx)
		}
	}
}

// Stop signals the watcher to stop and waits for it to exit.
func (w *PollWatcher) Stop() {
	close(w.stopCh)
	<-w.doneCh
}

// Done returns a channel that is closed when the watcher has exited (either
// via Stop, context cancellation, or watchDuration expiry). The Manager uses
// this to detect when a watcher needs restarting.
func (w *PollWatcher) Done() <-chan struct{} {
	return w.doneCh
}

func (w *PollWatcher) poll(ctx context.Context) {
	since := time.Now().UTC().Add(-w.watchDuration)
	ids, err := w.reader.ListSince(ctx, w.tableName, since)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warn("poll watcher scan error", "error", err)
		return
	}

	if len(ids) == 0 {
		return
	}

	w.logger.Debug("poll watcher found updated items", "count", len(ids))
	for _, id := range ids {
		w.onChange(id)
	}
}
