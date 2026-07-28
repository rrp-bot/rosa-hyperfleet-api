// Package main is the compactor binary, now a no-op.
//
// With the DynamoDB backend, tombstone cleanup is handled automatically by
// DynamoDB's TTL feature: soft-deleted items carry a `ttl` attribute (Unix
// epoch) and DynamoDB removes them within 48 h of expiry. No explicit
// compaction process is required.
//
// This binary is retained as a placeholder so existing Helm chart / pipeline
// references to the compactor image continue to work without error. It starts,
// logs that it is a no-op, and exits 0.
package main

import (
	"log/slog"
)

func main() {
	slog.Info("compactor is a no-op: DynamoDB TTL handles tombstone cleanup automatically")
}
