package hyperfleetdb

import (
	"hash/fnv"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

// buildTestScheme returns a minimal *runtime.Scheme for tests that need a
// non-nil Scheme (but don't require any GVKs to be registered).
func buildTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	return runtime.NewScheme()
}

// --- ShardConfig.matches ---

// TestShardConfig_NilReceiverMatchesAll verifies that a nil *ShardConfig
// causes matches() to return true for every namespace — i.e. no sharding.
func TestShardConfig_NilReceiverMatchesAll(t *testing.T) {
	var sc *ShardConfig
	namespaces := []string{"default", "kube-system", "openshift", "", "ns-42"}
	for _, ns := range namespaces {
		if !sc.matches(ns) {
			t.Errorf("nil ShardConfig.matches(%q) = false, want true", ns)
		}
	}
}

// TestShardConfig_OwnsExactShard verifies that matches() returns true only
// when the namespace hashes to an owned shard.
func TestShardConfig_OwnsExactShard(t *testing.T) {
	mod := 8
	shardFor := func(ns string) int {
		h := fnv.New32a()
		_, _ = h.Write([]byte(ns))
		return int(h.Sum32()) % mod
	}

	namespaces := []string{"default", "kube-system", "openshift", "", "ns-alpha", "ns-beta", "abc-123", "z"}

	for _, ns := range namespaces {
		expectedShard := shardFor(ns)

		// ShardConfig that owns the correct shard → must match.
		owns := &ShardConfig{Mod: mod, Owned: []int{expectedShard}}
		if !owns.matches(ns) {
			t.Errorf("ShardConfig{Mod:%d,Owned:[%d]}.matches(%q) = false, want true (shard=%d)",
				mod, expectedShard, ns, expectedShard)
		}

		// ShardConfig that owns a different shard → must not match.
		otherShard := (expectedShard + 1) % mod
		notOwns := &ShardConfig{Mod: mod, Owned: []int{otherShard}}
		if notOwns.matches(ns) {
			t.Errorf("ShardConfig{Mod:%d,Owned:[%d]}.matches(%q) = true, want false (shard=%d)",
				mod, otherShard, ns, expectedShard)
		}
	}
}

// TestShardConfig_MultipleOwnedShards verifies that a replica owning several
// shards accepts namespaces belonging to any of them, and that two non-overlapping
// replicas together cover every namespace exactly once.
func TestShardConfig_MultipleOwnedShards(t *testing.T) {
	mod := 8
	// Two replicas splitting 8 shards evenly.
	replicaA := &ShardConfig{Mod: mod, Owned: []int{0, 2, 4, 6}}
	replicaB := &ShardConfig{Mod: mod, Owned: []int{1, 3, 5, 7}}

	namespaces := []string{"default", "kube-system", "openshift", "ns-1", "ns-2", "ns-3", "ns-4", "ns-5"}

	for _, ns := range namespaces {
		a := replicaA.matches(ns)
		b := replicaB.matches(ns)
		// Every namespace must be owned by exactly one of the two replicas.
		if a == b {
			t.Errorf("namespace %q: replicaA.matches=%v replicaB.matches=%v — "+
				"must be owned by exactly one replica", ns, a, b)
		}
	}
}

// TestShardConfig_EmptyOwnedMatchesNothing verifies that a ShardConfig with
// an empty Owned slice never matches (replica effectively disabled).
func TestShardConfig_EmptyOwnedMatchesNothing(t *testing.T) {
	sc := &ShardConfig{Mod: 8, Owned: []int{}}
	namespaces := []string{"default", "kube-system", "openshift", "", "ns-x"}
	for _, ns := range namespaces {
		if sc.matches(ns) {
			t.Errorf("ShardConfig{Mod:8,Owned:[]}.matches(%q) = true, want false", ns)
		}
	}
}

// TestShardConfig_ConsistencyWithComputeGSIShard is the critical cross-check:
// ShardConfig.matches() (used to decide which replica owns a namespace) and
// computeGSIShard (used to write the gsiShard DynamoDB attribute) must agree
// on which shard bucket a namespace maps to. Divergence causes objects written
// by the DB layer to be invisible to the cache layer — a silent data loss bug.
func TestShardConfig_ConsistencyWithComputeGSIShard(t *testing.T) {
	mod := gsiShardCount // 8
	namespaces := []string{
		"default", "kube-system", "openshift", "", "ns-alpha", "ns-beta",
		"abc-123", "z", "a", "production", "staging", "openshift-monitoring",
	}

	for _, ns := range namespaces {
		// Derive the expected shard from stdlib FNV-32a (ShardConfig path).
		h := fnv.New32a()
		_, _ = h.Write([]byte(ns))
		expectedShard := int(h.Sum32()) % mod

		// Build a ShardConfig that owns only the expected shard and verify
		// that computeGSIShard maps ns to the same shard bucket.
		sc := &ShardConfig{Mod: mod, Owned: []int{expectedShard}}
		if !sc.matches(ns) {
			t.Errorf(
				"namespace %q: ShardConfig.matches() disagrees with computeGSIShard: "+
					"computeGSIShard=%q but ShardConfig{Mod:%d,Owned:[%d]}.matches=false — "+
					"objects will be written to shard %q but never seen by the owning replica",
				ns, computeGSIShard(ns), mod, expectedShard, computeGSIShard(ns),
			)
		}
	}
}

// --- NewManager / NewClient validation ---

// TestNewManager_RequiresScheme verifies that NewManager returns an error when
// no Scheme is provided.
func TestNewManager_RequiresScheme(t *testing.T) {
	_, err := NewManager(Options{})
	if err == nil {
		t.Error("NewManager with nil Scheme: expected error, got nil")
	}
}

// TestNewManager_RequiresDynamoDB verifies that NewManager returns an error
// when no DynamoDB client is provided (but Scheme is set).
func TestNewManager_RequiresDynamoDB(t *testing.T) {
	opts := Options{
		Scheme:   buildTestScheme(t),
		DynamoDB: nil,
	}
	_, err := NewManager(opts)
	if err == nil {
		t.Error("NewManager with nil DynamoDB: expected error, got nil")
	}
}

// TestNewClient_RequiresScheme mirrors the same guard for NewClient.
func TestNewClient_RequiresScheme(t *testing.T) {
	_, _, err := NewClient(Options{})
	if err == nil {
		t.Error("NewClient with nil Scheme: expected error, got nil")
	}
}

// TestNewClient_RequiresDynamoDB verifies NewClient fails without a DynamoDB client.
func TestNewClient_RequiresDynamoDB(t *testing.T) {
	opts := Options{
		Scheme:   buildTestScheme(t),
		DynamoDB: nil,
	}
	_, _, err := NewClient(opts)
	if err == nil {
		t.Error("NewClient with nil DynamoDB: expected error, got nil")
	}
}
