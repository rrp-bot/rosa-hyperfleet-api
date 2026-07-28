// Package dynamodb provides generic DynamoDB CRUD and poll-watch primitives
// for desire-document tables used by hyperfleet services.
//
// This package has zero dependency on controller-runtime or any Kubernetes
// types. It is designed to be importable by any service that needs to interact
// with the hyperfleet DynamoDB table schema (kube-applier-aws, hyperfleet-
// operator, platform-api).
//
// Key types:
//
//   - [DynamoDBMetadataAccessor] — interface every stored type must implement.
//   - [KubeContentAccessor] — interface for types with RawExtension fields.
//   - [CRUD] — generic Get/List/ListSince/Create/Replace/Delete over a single
//     DynamoDB table.
//   - [PollWatcher] — generic timestamp-based poll watcher that implements the
//     doorbell pattern: poll every 15 s, close after 5 min to trigger relist.
//   - Error sentinels: [ErrNotFound], [ErrAlreadyExists],
//     [ErrPreconditionFailed].
package dynamodb
