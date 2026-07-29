package dynamodb

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"k8s.io/apimachinery/pkg/runtime"
)

// --- rawExtToString / stringToRawExt round-trip ---

func TestRawExtRoundTrip_ValidJSON(t *testing.T) {
	input := `{"foo":"bar","n":42}`
	ext := &runtime.RawExtension{Raw: []byte(input)}

	s, err := rawExtToString(ext)
	if err != nil {
		t.Fatalf("rawExtToString: %v", err)
	}
	if s == "" {
		t.Fatal("rawExtToString returned empty string for valid JSON")
	}

	got, err := stringToRawExt(s)
	if err != nil {
		t.Fatalf("stringToRawExt: %v", err)
	}
	if got == nil {
		t.Fatal("stringToRawExt returned nil for non-empty string")
	}
	if !jsonSemanticEqual(t, got.Raw, []byte(input)) {
		t.Errorf("round-trip mismatch: got %s, want %s", got.Raw, input)
	}
}

func TestRawExtRoundTrip_Nil(t *testing.T) {
	s, err := rawExtToString(nil)
	if err != nil {
		t.Fatalf("rawExtToString(nil): %v", err)
	}
	if s != "" {
		t.Errorf("rawExtToString(nil) = %q, want empty", s)
	}

	got, err := stringToRawExt("")
	if err != nil {
		t.Fatalf("stringToRawExt empty: %v", err)
	}
	if got != nil {
		t.Errorf("stringToRawExt(\"\") = %v, want nil", got)
	}
}

func TestRawExtRoundTrip_EmptyRaw(t *testing.T) {
	ext := &runtime.RawExtension{Raw: []byte{}}
	s, err := rawExtToString(ext)
	if err != nil {
		t.Fatalf("rawExtToString(empty raw): %v", err)
	}
	if s != "" {
		t.Errorf("rawExtToString({Raw:[]}) = %q, want empty", s)
	}
}

func TestRawExtToString_InvalidJSON(t *testing.T) {
	ext := &runtime.RawExtension{Raw: []byte(`{not valid json`)}
	_, err := rawExtToString(ext)
	if err == nil {
		t.Error("rawExtToString(invalid JSON): expected error, got nil")
	}
}

func TestStringToRawExt_InvalidJSON(t *testing.T) {
	_, err := stringToRawExt("{not valid")
	if err == nil {
		t.Error("stringToRawExt(invalid JSON): expected error, got nil")
	}
}

// --- KubeContentAttributeValues ---

// fakeAccessor is a test implementation of KubeContentAccessor.
type fakeAccessor struct {
	spec   *runtime.RawExtension
	status *runtime.RawExtension
}

func (f *fakeAccessor) GetSpecKubeContent() *runtime.RawExtension    { return f.spec }
func (f *fakeAccessor) SetSpecKubeContent(e *runtime.RawExtension)   { f.spec = e }
func (f *fakeAccessor) GetStatusKubeContent() *runtime.RawExtension  { return f.status }
func (f *fakeAccessor) SetStatusKubeContent(e *runtime.RawExtension) { f.status = e }

func TestKubeContentAttributeValues_BothNil(t *testing.T) {
	acc := &fakeAccessor{}
	m, err := KubeContentAttributeValues(acc)
	if err != nil {
		t.Fatalf("KubeContentAttributeValues: %v", err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

func TestKubeContentAttributeValues_SpecOnly(t *testing.T) {
	acc := &fakeAccessor{
		spec: &runtime.RawExtension{Raw: []byte(`{"a":1}`)},
	}
	m, err := KubeContentAttributeValues(acc)
	if err != nil {
		t.Fatalf("KubeContentAttributeValues: %v", err)
	}
	if _, ok := m[RawExtFieldSpecKubeContent]; !ok {
		t.Errorf("expected %s key in result", RawExtFieldSpecKubeContent)
	}
	if _, ok := m[RawExtFieldStatusKubeContent]; ok {
		t.Errorf("unexpected %s key in result (status was nil)", RawExtFieldStatusKubeContent)
	}
}

func TestKubeContentAttributeValues_Both(t *testing.T) {
	acc := &fakeAccessor{
		spec:   &runtime.RawExtension{Raw: []byte(`{"spec":true}`)},
		status: &runtime.RawExtension{Raw: []byte(`{"status":true}`)},
	}
	m, err := KubeContentAttributeValues(acc)
	if err != nil {
		t.Fatalf("KubeContentAttributeValues: %v", err)
	}
	if _, ok := m[RawExtFieldSpecKubeContent]; !ok {
		t.Error("expected spec_kubeContent key")
	}
	if _, ok := m[RawExtFieldStatusKubeContent]; !ok {
		t.Error("expected status_kubeContent key")
	}
}

func TestKubeContentAttributeValues_InvalidJSON(t *testing.T) {
	acc := &fakeAccessor{
		spec: &runtime.RawExtension{Raw: []byte(`{bad json`)},
	}
	_, err := KubeContentAttributeValues(acc)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

// --- KubeContentReadFromItem ---

func TestKubeContentReadFromItem_HappyPath(t *testing.T) {
	specJSON := `{"x":1}`
	statusJSON := `{"y":2}`

	item := map[string]types.AttributeValue{
		RawExtFieldSpecKubeContent:   &types.AttributeValueMemberS{Value: specJSON},
		RawExtFieldStatusKubeContent: &types.AttributeValueMemberS{Value: statusJSON},
	}

	acc := &fakeAccessor{}
	if err := KubeContentReadFromItem(acc, item); err != nil {
		t.Fatalf("KubeContentReadFromItem: %v", err)
	}

	if acc.spec == nil || string(acc.spec.Raw) != specJSON {
		t.Errorf("spec mismatch: got %v", acc.spec)
	}
	if acc.status == nil || string(acc.status.Raw) != statusJSON {
		t.Errorf("status mismatch: got %v", acc.status)
	}
}

func TestKubeContentReadFromItem_EmptyItem(t *testing.T) {
	acc := &fakeAccessor{}
	if err := KubeContentReadFromItem(acc, map[string]types.AttributeValue{}); err != nil {
		t.Fatalf("KubeContentReadFromItem on empty item: %v", err)
	}
	if acc.spec != nil || acc.status != nil {
		t.Errorf("expected nil spec and status, got spec=%v status=%v", acc.spec, acc.status)
	}
}

func TestKubeContentReadFromItem_InvalidJSONInItem(t *testing.T) {
	item := map[string]types.AttributeValue{
		RawExtFieldSpecKubeContent: &types.AttributeValueMemberS{Value: `{bad`},
	}
	acc := &fakeAccessor{}
	err := KubeContentReadFromItem(acc, item)
	if err == nil {
		t.Error("expected error for invalid JSON in item, got nil")
	}
}

// --- KubeContentAttributeValues + KubeContentReadFromItem round-trip ---

func TestKubeContentRoundTrip(t *testing.T) {
	original := &fakeAccessor{
		spec:   &runtime.RawExtension{Raw: []byte(`{"hello":"world"}`)},
		status: &runtime.RawExtension{Raw: []byte(`{"ready":true}`)},
	}

	// Simulate write path: produce DynamoDB attributes.
	attrs, err := KubeContentAttributeValues(original)
	if err != nil {
		t.Fatalf("KubeContentAttributeValues: %v", err)
	}

	// Simulate read path: populate a fresh accessor from the item.
	recovered := &fakeAccessor{}
	if err := KubeContentReadFromItem(recovered, attrs); err != nil {
		t.Fatalf("KubeContentReadFromItem: %v", err)
	}

	if recovered.spec == nil {
		t.Fatal("recovered spec is nil")
	}
	if !jsonSemanticEqual(t, recovered.spec.Raw, original.spec.Raw) {
		t.Errorf("spec round-trip mismatch: got %s, want %s", recovered.spec.Raw, original.spec.Raw)
	}
	if recovered.status == nil {
		t.Fatal("recovered status is nil")
	}
	if !jsonSemanticEqual(t, recovered.status.Raw, original.status.Raw) {
		t.Errorf("status round-trip mismatch: got %s, want %s", recovered.status.Raw, original.status.Raw)
	}
}

// --- helpers ---

// jsonSemanticEqual compares two JSON byte slices for semantic equality by
// unmarshalling both into any and comparing the normalised re-marshalled form.
func jsonSemanticEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	norm := func(src []byte) string {
		var v any
		if err := json.Unmarshal(src, &v); err != nil {
			t.Errorf("jsonSemanticEqual: unmarshal %q: %v", src, err)
			return ""
		}
		out, _ := json.Marshal(v)
		return string(out)
	}
	return norm(a) == norm(b)
}
