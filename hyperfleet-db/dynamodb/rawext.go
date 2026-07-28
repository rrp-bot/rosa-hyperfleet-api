package dynamodb

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"k8s.io/apimachinery/pkg/runtime"
)

// rawExtField names — top-level DynamoDB S attributes stored alongside the
// marshalled desire struct. These fields carry k8s RawExtension values that
// cannot be represented as DynamoDB maps (they are opaque JSON blobs).
const (
	RawExtFieldSpecKubeContent   = "spec_kubeContent"
	RawExtFieldStatusKubeContent = "status_kubeContent"
)

// KubeContentAccessor provides access to the RawExtension fields that are
// tagged dynamodbav:"-" and need manual serialisation. Types that carry no
// KubeContent return nil from both getters; the CRUD layer skips serialisation
// when both are nil.
type KubeContentAccessor interface {
	GetSpecKubeContent() *runtime.RawExtension
	SetSpecKubeContent(*runtime.RawExtension)
	GetStatusKubeContent() *runtime.RawExtension
	SetStatusKubeContent(*runtime.RawExtension)
}

// rawExtToString converts a RawExtension's JSON bytes into a compact JSON
// string suitable for DynamoDB S attribute storage.
func rawExtToString(ext *runtime.RawExtension) (string, error) {
	if ext == nil || len(ext.Raw) == 0 {
		return "", nil
	}
	// Re-marshal through map to normalise (compact, stable key order).
	var m any
	if err := json.Unmarshal(ext.Raw, &m); err != nil {
		return "", fmt.Errorf("unmarshal RawExtension: %w", err)
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("marshal RawExtension: %w", err)
	}
	return string(b), nil
}

// stringToRawExt converts a JSON string from a DynamoDB S attribute back into
// a RawExtension.
func stringToRawExt(s string) (*runtime.RawExtension, error) {
	if s == "" {
		return nil, nil
	}
	if !json.Valid([]byte(s)) {
		return nil, fmt.Errorf("stored kubeContent is not valid JSON")
	}
	return &runtime.RawExtension{Raw: []byte(s)}, nil
}

// KubeContentAttributeValues returns additional DynamoDB AttributeValue
// entries for the spec_kubeContent and status_kubeContent fields. The returned
// map is merged into the item map before a PutItem call.
func KubeContentAttributeValues(acc KubeContentAccessor) (map[string]types.AttributeValue, error) {
	result := make(map[string]types.AttributeValue)
	if ext := acc.GetSpecKubeContent(); ext != nil {
		s, err := rawExtToString(ext)
		if err != nil {
			return nil, err
		}
		if s != "" {
			result[RawExtFieldSpecKubeContent] = &types.AttributeValueMemberS{Value: s}
		}
	}
	if ext := acc.GetStatusKubeContent(); ext != nil {
		s, err := rawExtToString(ext)
		if err != nil {
			return nil, err
		}
		if s != "" {
			result[RawExtFieldStatusKubeContent] = &types.AttributeValueMemberS{Value: s}
		}
	}
	return result, nil
}

// KubeContentReadFromItem reads the manually-stored RawExtension fields from a
// DynamoDB item map and sets them on the desire object.
func KubeContentReadFromItem(acc KubeContentAccessor, item map[string]types.AttributeValue) error {
	if av, ok := item[RawExtFieldSpecKubeContent]; ok {
		if sv, ok := av.(*types.AttributeValueMemberS); ok {
			ext, err := stringToRawExt(sv.Value)
			if err != nil {
				return fmt.Errorf("read %s: %w", RawExtFieldSpecKubeContent, err)
			}
			acc.SetSpecKubeContent(ext)
		}
	}
	if av, ok := item[RawExtFieldStatusKubeContent]; ok {
		if sv, ok := av.(*types.AttributeValueMemberS); ok {
			ext, err := stringToRawExt(sv.Value)
			if err != nil {
				return fmt.Errorf("read %s: %w", RawExtFieldStatusKubeContent, err)
			}
			acc.SetStatusKubeContent(ext)
		}
	}
	return nil
}
