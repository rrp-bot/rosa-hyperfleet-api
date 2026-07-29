package hyperfleetdb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clusterScopedNamespace is the sentinel stored in the DynamoDB namespace range
// key for cluster-scoped resources (which have an empty Kubernetes namespace).
// DynamoDB rejects empty strings in key attributes, so we substitute this value
// on write and reverse it on read.
const clusterScopedNamespace = "_"

// DynamoDB attribute names for the CRD table schema.
const (
	attrName             = "name"
	attrNamespace        = "namespace"
	attrUID              = "uid"
	attrObjectVersion    = "objectVersion"
	attrUpdateTime       = "updateTime"
	attrGSIShard         = "gsiShard"
	attrCreateTime       = "createTime"
	attrSpec             = "spec"
	attrStatus           = "status"
	attrMetadata         = "metadata"
	attrDeletionTS       = "deletionTimestamp"
	attrTTL              = "ttl"

	gsiShardCount = 8 // number of GSI shard buckets for the updateTime-index
)

// GSIName is the name of the updateTime-index GSI on every CRD table.
const GSIName = "updateTime-index"

// storedMetadata is the JSON shape stored in the metadata DynamoDB attribute.
type storedMetadata struct {
	Labels          map[string]string       `json:"labels,omitempty"`
	Annotations     map[string]string       `json:"annotations,omitempty"`
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences,omitempty"`
	Finalizers      []string                `json:"finalizers,omitempty"`
	Generation      int64                   `json:"generation,omitempty"`
}

// crdItem is the in-memory representation of a DynamoDB CRD item.
type crdItem struct {
	Name              string
	Namespace         string
	UID               string
	ObjectVersion     int64
	UpdateTime        time.Time
	CreateTime        time.Time
	Spec              json.RawMessage
	Status            json.RawMessage
	Metadata          json.RawMessage
	DeletionTimestamp *time.Time
}

func (it *crdItem) hasFinalizers() bool {
	if len(it.Metadata) == 0 {
		return false
	}
	var sm storedMetadata
	if err := json.Unmarshal(it.Metadata, &sm); err != nil {
		return false
	}
	return len(sm.Finalizers) > 0
}

func (it *crdItem) isFullyDeleted() bool {
	return it.DeletionTimestamp != nil && !it.hasFinalizers()
}

// --- GVK / scheme helpers ---

func gvkToString(gvk schema.GroupVersionKind) string {
	return fmt.Sprintf("%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)
}

func stringToGVK(s string) (schema.GroupVersionKind, error) {
	parts := strings.SplitN(s, "/", 3)
	if len(parts) != 3 {
		return schema.GroupVersionKind{}, fmt.Errorf("invalid GVK string %q: expected group/version/kind", s)
	}
	return schema.GroupVersionKind{Group: parts[0], Version: parts[1], Kind: parts[2]}, nil
}

func resolveGVK(scheme *runtime.Scheme, obj runtime.Object) (schema.GroupVersionKind, error) {
	gvks, _, err := scheme.ObjectKinds(obj)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("resolve GVK: %w", err)
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK registered for type %T", obj)
	}
	return gvks[0], nil
}

func itemGVKFromListGVK(gvk schema.GroupVersionKind) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    strings.TrimSuffix(gvk.Kind, "List"),
	}
}

func buildRESTMapper(s *runtime.Scheme) apimeta.RESTMapper {
	mapper := apimeta.NewDefaultRESTMapper(s.PrioritizedVersionsAllGroups())
	for gvk := range s.AllKnownTypes() {
		if strings.HasSuffix(gvk.Kind, "List") || gvk.Kind == "" {
			continue
		}
		mapper.Add(gvk, apimeta.RESTScopeNamespace)
	}
	return mapper
}

// --- Table name helpers ---

// tableForGVK returns the DynamoDB table name for a given GVK and prefix.
// E.g. "rc01-" + Cluster → "rc01-clusters".
func tableForGVK(prefix string, gvk schema.GroupVersionKind) (string, error) {
	suffix, err := tableSuffixForKind(gvk.Kind)
	if err != nil {
		return "", err
	}
	return prefix + suffix, nil
}

func tableSuffixForKind(kind string) (string, error) {
	switch kind {
	case "Cluster":
		return "clusters", nil
	case "ClusterList":
		return "clusters", nil
	case "NodePool":
		return "nodepools", nil
	case "NodePoolList":
		return "nodepools", nil
	case "Placement":
		return "placements", nil
	case "PlacementList":
		return "placements", nil
	case "Manifest":
		return "manifests", nil
	case "ManifestList":
		return "manifests", nil
	case "ManagementCluster":
		return "managementclusters", nil
	case "ManagementClusterList":
		return "managementclusters", nil
	default:
		return "", fmt.Errorf("hyperfleetdb: no table mapping for kind %q", kind)
	}
}

// --- DynamoDB ↔ object marshalling ---

// objectToItem converts a client.Object into a DynamoDB attribute map for PutItem.
func objectToItem(obj client.Object, gvk schema.GroupVersionKind, objectVersion int64, now time.Time) (map[string]dynamodbtypes.AttributeValue, error) {
	spec, err := extractSpec(obj)
	if err != nil {
		return nil, err
	}
	status, err := extractStatus(obj)
	if err != nil {
		return nil, err
	}
	metadata, err := extractMetadata(obj)
	if err != nil {
		return nil, err
	}

	uid := string(obj.GetUID())
	if uid == "" {
		uid = uuid.New().String()
	}

	createTime := now
	if !obj.GetCreationTimestamp().Time.IsZero() {
		createTime = obj.GetCreationTimestamp().Time
	}

	ns := obj.GetNamespace()
	storedNS := namespaceToStored(ns)
	shard := computeGSIShard(ns)

	item := map[string]dynamodbtypes.AttributeValue{
		attrName:          &dynamodbtypes.AttributeValueMemberS{Value: obj.GetName()},
		attrNamespace:     &dynamodbtypes.AttributeValueMemberS{Value: storedNS},
		attrUID:           &dynamodbtypes.AttributeValueMemberS{Value: uid},
		attrObjectVersion: &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(objectVersion, 10)},
		attrUpdateTime:    &dynamodbtypes.AttributeValueMemberS{Value: now.UTC().Format(time.RFC3339Nano)},
		attrGSIShard:      &dynamodbtypes.AttributeValueMemberS{Value: shard},
		attrCreateTime:    &dynamodbtypes.AttributeValueMemberS{Value: createTime.UTC().Format(time.RFC3339Nano)},
		attrSpec:          &dynamodbtypes.AttributeValueMemberS{Value: string(spec)},
		attrStatus:        &dynamodbtypes.AttributeValueMemberS{Value: string(status)},
		attrMetadata:      &dynamodbtypes.AttributeValueMemberS{Value: string(metadata)},
	}

	if ts := obj.GetDeletionTimestamp(); ts != nil {
		item[attrDeletionTS] = &dynamodbtypes.AttributeValueMemberS{
			Value: ts.UTC().Format(time.RFC3339Nano),
		}
	}

	return item, nil
}

// itemToObject reads a DynamoDB attribute map into a client.Object.
func itemToObject(item map[string]dynamodbtypes.AttributeValue, scheme *runtime.Scheme) (client.Object, error) {
	it, err := parseItem(item)
	if err != nil {
		return nil, err
	}

	gvkStr, _ := getString(item, "gvk") // optional; may not be stored
	_ = gvkStr

	// We don't store gvk in the item — the caller knows the GVK from the table.
	// Return a partially populated crdItem; the caller fills in GVK.
	_ = it
	return nil, fmt.Errorf("hyperfleetdb: itemToObject requires GVK context — use itemToObjectWithGVK")
}

// itemToObjectWithGVK reads a DynamoDB attribute map into a client.Object of the given GVK.
func itemToObjectWithGVK(item map[string]dynamodbtypes.AttributeValue, gvk schema.GroupVersionKind, scheme *runtime.Scheme) (client.Object, error) {
	it, err := parseItem(item)
	if err != nil {
		return nil, err
	}

	runtimeObj, err := scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("scheme.New(%v): %w", gvk, err)
	}
	obj, ok := runtimeObj.(client.Object)
	if !ok {
		return nil, fmt.Errorf("type %T does not implement client.Object", runtimeObj)
	}

	if err := populateObjectFromItem(obj, it, gvk); err != nil {
		return nil, err
	}
	return obj, nil
}

func populateObjectFromItem(obj client.Object, it *crdItem, gvk schema.GroupVersionKind) error {
	obj.SetName(it.Name)
	obj.SetNamespace(it.Namespace)
	obj.SetUID(apitypes.UID(it.UID))
	obj.SetResourceVersion(strconv.FormatInt(it.ObjectVersion, 10))
	obj.SetCreationTimestamp(metav1.NewTime(it.CreateTime))
	if it.DeletionTimestamp != nil {
		dt := metav1.NewTime(*it.DeletionTimestamp)
		obj.SetDeletionTimestamp(&dt)
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)

	if len(it.Metadata) > 0 {
		var sm storedMetadata
		if err := json.Unmarshal(it.Metadata, &sm); err == nil {
			obj.SetLabels(sm.Labels)
			obj.SetAnnotations(sm.Annotations)
			obj.SetOwnerReferences(sm.OwnerReferences)
			obj.SetFinalizers(sm.Finalizers)
			obj.SetGeneration(sm.Generation)
		}
	}

	return injectSpecStatus(obj, it.Spec, it.Status)
}

func parseItem(item map[string]dynamodbtypes.AttributeValue) (*crdItem, error) {
	it := &crdItem{}

	it.Name, _ = getString(item, attrName)
	storedNS, _ := getString(item, attrNamespace)
	it.Namespace = namespaceFromStored(storedNS)
	it.UID, _ = getString(item, attrUID)

	if v, ok := getString(item, attrObjectVersion); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			it.ObjectVersion = n
		}
	}
	if av, ok := item[attrObjectVersion]; ok {
		if nv, ok := av.(*dynamodbtypes.AttributeValueMemberN); ok {
			n, err := strconv.ParseInt(nv.Value, 10, 64)
			if err == nil {
				it.ObjectVersion = n
			}
		}
	}

	if v, ok := getString(item, attrUpdateTime); ok {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err == nil {
			it.UpdateTime = t
		}
	}
	if v, ok := getString(item, attrCreateTime); ok {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err == nil {
			it.CreateTime = t
		}
	}
	if v, ok := getString(item, attrDeletionTS); ok && v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err == nil {
			it.DeletionTimestamp = &t
		}
	}

	if v, ok := getString(item, attrSpec); ok {
		it.Spec = json.RawMessage(v)
	}
	if v, ok := getString(item, attrStatus); ok {
		it.Status = json.RawMessage(v)
	}
	if v, ok := getString(item, attrMetadata); ok {
		it.Metadata = json.RawMessage(v)
	}

	return it, nil
}

func getString(item map[string]dynamodbtypes.AttributeValue, key string) (string, bool) {
	av, ok := item[key]
	if !ok {
		return "", false
	}
	sv, ok := av.(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return sv.Value, true
}

// namespaceToStored converts a Kubernetes namespace to the value stored in
// DynamoDB. Cluster-scoped resources have an empty namespace, which DynamoDB
// rejects as a key attribute value, so we substitute clusterScopedNamespace.
func namespaceToStored(ns string) string {
	if ns == "" {
		return clusterScopedNamespace
	}
	return ns
}

// namespaceFromStored reverses namespaceToStored: the sentinel is returned as
// the empty string that cluster-scoped Kubernetes resources actually carry.
func namespaceFromStored(stored string) string {
	if stored == clusterScopedNamespace {
		return ""
	}
	return stored
}

// computeGSIShard computes the GSI shard bucket string ("0"–"7") for a namespace.
func computeGSIShard(namespace string) string {
	h := fnvHash(namespace)
	return strconv.Itoa(int(h) % gsiShardCount)
}

func fnvHash(s string) uint32 {
	h := fnvNew32a()
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// fnvNew32a returns the FNV-32a offset basis.
func fnvNew32a() uint32 { return 2166136261 }

// --- spec/status/metadata helpers (reused from old convert.go) ---

func extractSpec(obj client.Object) (json.RawMessage, error) {
	val := reflect.ValueOf(obj).Elem()
	specField := val.FieldByName("Spec")
	if !specField.IsValid() {
		return json.RawMessage(`{}`), nil
	}
	return json.Marshal(specField.Interface())
}

func extractStatus(obj client.Object) (json.RawMessage, error) {
	val := reflect.ValueOf(obj).Elem()
	statusField := val.FieldByName("Status")
	if !statusField.IsValid() {
		return json.RawMessage(`{}`), nil
	}
	return json.Marshal(statusField.Interface())
}

func extractMetadata(obj client.Object) (json.RawMessage, error) {
	sm := storedMetadata{
		Labels:          obj.GetLabels(),
		Annotations:     obj.GetAnnotations(),
		OwnerReferences: obj.GetOwnerReferences(),
		Finalizers:      obj.GetFinalizers(),
		Generation:      obj.GetGeneration(),
	}
	return json.Marshal(sm)
}

func injectSpecStatus(obj client.Object, spec, status json.RawMessage) error {
	val := reflect.ValueOf(obj).Elem()

	if len(spec) > 0 && string(spec) != "{}" {
		specField := val.FieldByName("Spec")
		if specField.IsValid() && specField.CanAddr() {
			if err := json.Unmarshal(spec, specField.Addr().Interface()); err != nil {
				return fmt.Errorf("unmarshal spec: %w", err)
			}
		}
	}

	if len(status) > 0 && string(status) != "{}" {
		statusField := val.FieldByName("Status")
		if statusField.IsValid() && statusField.CanAddr() {
			if err := json.Unmarshal(status, statusField.Addr().Interface()); err != nil {
				return fmt.Errorf("unmarshal status: %w", err)
			}
		}
	}

	return nil
}

func setListItems(list client.ObjectList, items []client.Object) error {
	val := reflect.ValueOf(list).Elem()
	itemsField := val.FieldByName("Items")
	if !itemsField.IsValid() {
		return fmt.Errorf("type %T has no Items field", list)
	}
	if itemsField.Kind() != reflect.Slice {
		return fmt.Errorf("type %T Items field is not a slice", list)
	}

	elemType := itemsField.Type().Elem()
	slice := reflect.MakeSlice(itemsField.Type(), 0, len(items))

	for _, item := range items {
		itemVal := reflect.ValueOf(item)
		if itemVal.Kind() == reflect.Pointer {
			if itemVal.Type().Elem() == elemType {
				slice = reflect.Append(slice, itemVal.Elem())
			} else {
				return fmt.Errorf("item type %T does not match slice element type %v", item, elemType)
			}
		} else if itemVal.Type() == elemType {
			slice = reflect.Append(slice, itemVal)
		} else {
			return fmt.Errorf("item type %T does not match slice element type %v", item, elemType)
		}
	}

	itemsField.Set(slice)
	return nil
}

// parseResourceVersion extracts the integer object version from a
// ResourceVersion string.
func parseResourceVersion(obj client.Object) (int64, error) {
	rv := obj.GetResourceVersion()
	if rv == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(rv, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("resource version %q is not a valid integer: %w", rv, err)
	}
	return v, nil
}

// --- label set for selector matching ---

type labelSet map[string]string

func (ls labelSet) Has(key string) bool         { _, ok := ls[key]; return ok }
func (ls labelSet) Get(key string) string       { return ls[key] }
func (ls labelSet) Lookup(k string) (string, bool) { v, ok := ls[k]; return v, ok }

// --- DynamoDB key builders ---

func itemKey(name, namespace string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		attrName:      &dynamodbtypes.AttributeValueMemberS{Value: name},
		attrNamespace: &dynamodbtypes.AttributeValueMemberS{Value: namespaceToStored(namespace)},
	}
}

// scanTableInput returns a ScanInput for a full consistent scan.
func scanTableInput(tableName string) *dynamodb.ScanInput {
	return &dynamodb.ScanInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
	}
}

// scanSinceInput returns a ScanInput for an eventually-consistent updateTime filter scan.
func scanSinceInput(tableName string, since time.Time) *dynamodb.ScanInput {
	return &dynamodb.ScanInput{
		TableName:        aws.String(tableName),
		ConsistentRead:   aws.Bool(false),
		FilterExpression: aws.String("updateTime > :since"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":since": &dynamodbtypes.AttributeValueMemberS{
				Value: since.UTC().Format(time.RFC3339Nano),
			},
		},
	}
}
