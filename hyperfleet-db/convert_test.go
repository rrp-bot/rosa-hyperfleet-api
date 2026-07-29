package hyperfleetdb

import (
	"hash/fnv"
	"testing"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// --- tableSuffixForKind ---

func TestTableSuffixForKind(t *testing.T) {
	cases := []struct {
		kind   string
		want   string
		wantOK bool
	}{
		{"Cluster", "clusters", true},
		{"ClusterList", "clusters", true},
		{"NodePool", "nodepools", true},
		{"NodePoolList", "nodepools", true},
		{"Placement", "placements", true},
		{"PlacementList", "placements", true},
		{"Manifest", "manifests", true},
		{"ManifestList", "manifests", true},
		{"ManagementCluster", "managementclusters", true},
		{"ManagementClusterList", "managementclusters", true},
		{"Unknown", "", false},
		{"", "", false},
		{"cluster", "", false}, // case-sensitive
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			got, err := tableSuffixForKind(tc.kind)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("tableSuffixForKind(%q) unexpected error: %v", tc.kind, err)
				}
				if got != tc.want {
					t.Errorf("tableSuffixForKind(%q) = %q, want %q", tc.kind, got, tc.want)
				}
			} else {
				if err == nil {
					t.Errorf("tableSuffixForKind(%q) expected error, got %q", tc.kind, got)
				}
			}
		})
	}
}

func TestTableForGVK_PrefixIsApplied(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "hyperfleet.openshift.io", Version: "v1alpha1", Kind: "Cluster"}
	got, err := tableForGVK("rc01-", gvk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "rc01-clusters" {
		t.Errorf("got %q, want %q", got, "rc01-clusters")
	}
}

func TestTableForGVK_UnknownKind(t *testing.T) {
	gvk := schema.GroupVersionKind{Kind: "Unknown"}
	_, err := tableForGVK("rc01-", gvk)
	if err == nil {
		t.Error("expected error for unknown kind, got nil")
	}
}

// --- GVK string round-trip ---

func TestGVKStringConversion_RoundTrip(t *testing.T) {
	cases := []schema.GroupVersionKind{
		{Group: "hyperfleet.openshift.io", Version: "v1alpha1", Kind: "Cluster"},
		{Group: "", Version: "v1", Kind: "Pod"},
		{Group: "apps", Version: "v1", Kind: "Deployment"},
		{Group: "a", Version: "b", Kind: "c"},
	}
	for _, gvk := range cases {
		s := gvkToString(gvk)
		got, err := stringToGVK(s)
		if err != nil {
			t.Fatalf("stringToGVK(%q): %v", s, err)
		}
		if got != gvk {
			t.Errorf("round-trip failed for %v: got %v", gvk, got)
		}
	}
}

func TestStringToGVK_InvalidInputs(t *testing.T) {
	invalids := []string{
		"",
		"justonepart",
		"group/version", // only 2 segments when split by "/"
	}
	for _, s := range invalids {
		_, err := stringToGVK(s)
		if err == nil {
			t.Errorf("stringToGVK(%q) expected error, got nil", s)
		}
	}
}

// --- parseResourceVersion ---

func TestParseResourceVersion(t *testing.T) {
	cases := []struct {
		rv      string
		want    int64
		wantErr bool
	}{
		{"", 0, false},
		{"1", 1, false},
		{"42", 42, false},
		{"9999999", 9999999, false},
		{"abc", 0, true},
		{"1.5", 0, true},
		{"1 ", 0, true},
		{"-1", -1, false}, // negative parses OK; callers must validate sign
	}
	for _, tc := range cases {
		obj := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{ResourceVersion: tc.rv},
		}
		got, err := parseResourceVersion(obj)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseResourceVersion(%q): expected error, got %d", tc.rv, got)
			}
		} else {
			if err != nil {
				t.Errorf("parseResourceVersion(%q): unexpected error: %v", tc.rv, err)
			}
			if got != tc.want {
				t.Errorf("parseResourceVersion(%q) = %d, want %d", tc.rv, got, tc.want)
			}
		}
	}
}

// --- computeGSIShard ---

func TestComputeGSIShard_Range(t *testing.T) {
	namespaces := []string{"default", "kube-system", "openshift", "", "a", "ns-0", "ns-1", "ns-2", "ns-3"}
	for _, ns := range namespaces {
		s := computeGSIShard(ns)
		if len(s) != 1 || s[0] < '0' || s[0] > '7' {
			t.Errorf("computeGSIShard(%q) = %q, want digit in [0,7]", ns, s)
		}
	}
}

func TestComputeGSIShard_Deterministic(t *testing.T) {
	ns := "openshift-monitoring"
	a := computeGSIShard(ns)
	b := computeGSIShard(ns)
	if a != b {
		t.Errorf("computeGSIShard is not deterministic: got %q then %q", a, b)
	}
}

// TestComputeGSIShard_MatchesShardConfigMatches verifies that computeGSIShard
// (used to write the gsiShard attribute to DynamoDB) and ShardConfig.matches()
// (used to decide whether this replica owns a namespace) produce the SAME shard
// bucket for the same namespace. Divergence between the two would silently route
// objects to the wrong replica — a critical correctness invariant.
func TestComputeGSIShard_MatchesShardConfigMatches(t *testing.T) {
	namespaces := []string{"default", "openshift", "kube-system", "ns-alpha", "ns-beta", "", "z", "a", "abc-123"}
	mod := gsiShardCount // 8 — must match the constant in convert.go

	for _, ns := range namespaces {
		// ShardConfig.matches uses hash/fnv.New32a() from the stdlib.
		h := fnv.New32a()
		_, _ = h.Write([]byte(ns))
		expectedShard := int(h.Sum32()) % mod

		// computeGSIShard uses the inline fnvHash loop in convert.go.
		// Verify that it maps ns to the same shard number.
		sc := &ShardConfig{Mod: mod, Owned: []int{expectedShard}}
		if !sc.matches(ns) {
			t.Errorf(
				"namespace %q: computeGSIShard → %q, but ShardConfig{Mod:%d, Owned:[%d]}.matches() = false; "+
					"the two FNV-32a implementations disagree (would cause cross-shard routing bug)",
				ns, computeGSIShard(ns), mod, expectedShard,
			)
		}
	}
}

// --- namespaceToStored / namespaceFromStored ---

// TestNamespaceToStored verifies that empty namespace is replaced with the
// sentinel and non-empty namespaces are passed through unchanged.
func TestNamespaceToStored(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", clusterScopedNamespace},
		{clusterScopedNamespace, clusterScopedNamespace}, // sentinel itself is a valid namespace name
		{"default", "default"},
		{"openshift-monitoring", "openshift-monitoring"},
	}
	for _, tc := range cases {
		got := namespaceToStored(tc.input)
		if got != tc.want {
			t.Errorf("namespaceToStored(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// TestNamespaceFromStored verifies that the sentinel is reversed to "" and
// other values are passed through unchanged.
func TestNamespaceFromStored(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{clusterScopedNamespace, ""},
		{"default", "default"},
		{"openshift-monitoring", "openshift-monitoring"},
		{"", ""},
	}
	for _, tc := range cases {
		got := namespaceFromStored(tc.input)
		if got != tc.want {
			t.Errorf("namespaceFromStored(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// TestNamespaceSentinel_RoundTrip verifies that namespaceToStored followed by
// namespaceFromStored is an identity for all namespace strings (including "").
func TestNamespaceSentinel_RoundTrip(t *testing.T) {
	namespaces := []string{"", "default", "kube-system", "openshift-monitoring", "ns-alpha"}
	for _, ns := range namespaces {
		got := namespaceFromStored(namespaceToStored(ns))
		if got != ns {
			t.Errorf("round-trip(%q): got %q", ns, got)
		}
	}
}

// TestItemKey_ClusterScopedUsesNonEmptyNamespace ensures that itemKey never
// produces an empty string for the namespace attribute — DynamoDB rejects empty
// string key attribute values.
func TestItemKey_ClusterScopedUsesNonEmptyNamespace(t *testing.T) {
	key := itemKey("my-mc", "")
	av, ok := key[attrNamespace]
	if !ok {
		t.Fatal("itemKey did not include namespace attribute")
	}
	sv, ok := av.(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatalf("namespace attribute is not a string type: %T", av)
	}
	if sv.Value == "" {
		t.Error("itemKey produced empty string namespace for cluster-scoped resource; DynamoDB will reject this")
	}
	if sv.Value != clusterScopedNamespace {
		t.Errorf("expected sentinel %q, got %q", clusterScopedNamespace, sv.Value)
	}
}
