package hyperfleetdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// dynClient implements client.Client backed by DynamoDB.
type dynClient struct {
	scheme      *runtime.Scheme
	ddb         *dynamodb.Client
	tablePrefix string
	restMapper  apimeta.RESTMapper
}

var _ client.Client = (*dynClient)(nil)

// isCondCheckFailed reports whether err is a DynamoDB ConditionalCheckFailedException.
func isCondCheckFailed(err error) bool {
	var ccf *dynamodbtypes.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

// --- client.Reader ---

func (c *dynClient) Get(ctx context.Context, key apitypes.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return err
	}
	tableName, err := tableForGVK(c.tablePrefix, gvk)
	if err != nil {
		return err
	}

	gr := schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}

	out, err := c.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		ConsistentRead: aws.Bool(true),
		Key:            itemKey(key.Name, key.Namespace),
	})
	if err != nil {
		return fmt.Errorf("dynamodb GetItem %s/%s: %w", tableName, key, err)
	}
	if len(out.Item) == 0 {
		return apierrors.NewNotFound(gr, key.Name)
	}

	it, err := parseItem(out.Item)
	if err != nil {
		return err
	}
	return populateObjectFromItem(obj, it, gvk)
}

func (c *dynClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(&listOpts)
	}

	gvk, err := resolveGVK(c.scheme, list)
	if err != nil {
		return err
	}
	itemGVK := itemGVKFromListGVK(gvk)
	tableName, err := tableForGVK(c.tablePrefix, itemGVK)
	if err != nil {
		return err
	}

	input := scanTableInput(tableName)

	var objects []client.Object
	paginator := dynamodb.NewScanPaginator(c.ddb, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("dynamodb Scan %s: %w", tableName, err)
		}
		for _, rawItem := range page.Items {
			obj, err := itemToObjectWithGVK(rawItem, itemGVK, c.scheme)
			if err != nil {
				return fmt.Errorf("dynamodb Scan %s convert: %w", tableName, err)
			}
			if !matchesListOptions(obj, listOpts) {
				continue
			}
			objects = append(objects, obj)
		}
	}

	return setListItems(list, objects)
}

// --- client.Writer ---

func (c *dynClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return err
	}
	tableName, err := tableForGVK(c.tablePrefix, gvk)
	if err != nil {
		return err
	}

	gr := schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}

	now := time.Now().UTC()
	if obj.GetUID() == "" {
		obj.SetUID(apitypes.UID(uuid.New().String()))
	}
	if obj.GetCreationTimestamp().IsZero() {
		obj.SetCreationTimestamp(metav1.NewTime(now))
	}

	item, err := objectToItem(obj, gvk, 1, now)
	if err != nil {
		return err
	}

	_, putErr := c.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(#n)"),
		ExpressionAttributeNames: map[string]string{
			"#n": attrName,
		},
	})
	if putErr != nil {
		if isCondCheckFailed(putErr) {
			return apierrors.NewAlreadyExists(gr, obj.GetName())
		}
		return fmt.Errorf("dynamodb PutItem (create) %s/%s: %w", tableName, obj.GetName(), putErr)
	}

	obj.SetResourceVersion("1")
	return nil
}

func (c *dynClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return err
	}
	tableName, err := tableForGVK(c.tablePrefix, gvk)
	if err != nil {
		return err
	}

	// Soft-delete: if the object has finalizers, stamp deletionTimestamp + ttl.
	// Otherwise hard-delete (TTL on tombstones is set regardless for safety).
	if len(obj.GetFinalizers()) > 0 {
		return c.softDelete(ctx, obj, gvk, tableName)
	}

	_, err = c.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key:       itemKey(obj.GetName(), obj.GetNamespace()),
	})
	if err != nil {
		return fmt.Errorf("dynamodb DeleteItem %s/%s: %w", tableName, obj.GetName(), err)
	}
	return nil
}

// softDelete stamps deletionTimestamp and a TTL (now+1h) on the item, then
// bumps objectVersion + updateTime so watchers observe the change.
func (c *dynClient) softDelete(ctx context.Context, obj client.Object, gvk schema.GroupVersionKind, tableName string) error {
	rv, err := parseResourceVersion(obj)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	ttlEpoch := now.Add(time.Hour).Unix()
	gr := schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}

	_, updateErr := c.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(tableName),
		Key:                 itemKey(obj.GetName(), obj.GetNamespace()),
		ConditionExpression: aws.String("#v = :expected"),
		UpdateExpression:    aws.String("SET #dt = :dt, #ttl = :ttl, #ov = :newv, #ut = :ut, #gs = :gs"),
		ExpressionAttributeNames: map[string]string{
			"#v":   attrObjectVersion,
			"#dt":  attrDeletionTS,
			"#ttl": attrTTL,
			"#ov":  attrObjectVersion,
			"#ut":  attrUpdateTime,
			"#gs":  attrGSIShard,
		},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":expected": &dynamodbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", rv)},
			":dt":       &dynamodbtypes.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
			":ttl":      &dynamodbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
			":newv":     &dynamodbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", rv + 1)},
			":ut":       &dynamodbtypes.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
			":gs":       &dynamodbtypes.AttributeValueMemberS{Value: computeGSIShard(obj.GetNamespace())},
		},
	})
	if updateErr != nil {
		if isCondCheckFailed(updateErr) {
			return apierrors.NewConflict(gr, obj.GetName(), fmt.Errorf("resource version conflict during soft-delete"))
		}
		return fmt.Errorf("dynamodb soft-delete %s/%s: %w", tableName, obj.GetName(), updateErr)
	}
	return nil
}

func (c *dynClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return err
	}
	tableName, err := tableForGVK(c.tablePrefix, gvk)
	if err != nil {
		return err
	}

	gr := schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}

	rv, err := parseResourceVersion(obj)
	if err != nil {
		return err
	}
	if rv == 0 {
		return fmt.Errorf("hyperfleetdb: Update called without ResourceVersion set on %s/%s",
			obj.GetNamespace(), obj.GetName())
	}

	now := time.Now().UTC()
	newVersion := rv + 1
	item, err := objectToItem(obj, gvk, newVersion, now)
	if err != nil {
		return err
	}

	_, putErr := c.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(tableName),
		Item:                item,
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#v": attrObjectVersion,
		},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":expected": &dynamodbtypes.AttributeValueMemberN{
				Value: fmt.Sprintf("%d", rv),
			},
		},
	})
	if putErr != nil {
		if isCondCheckFailed(putErr) {
			return apierrors.NewConflict(gr, obj.GetName(), fmt.Errorf("resource version conflict"))
		}
		return fmt.Errorf("dynamodb PutItem (update) %s/%s: %w", tableName, obj.GetName(), putErr)
	}

	obj.SetResourceVersion(fmt.Sprintf("%d", newVersion))
	return nil
}

func (c *dynClient) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "Patch is not supported by hyperfleetdb; use Update")
}

func (c *dynClient) DeleteAllOf(_ context.Context, _ client.Object, _ ...client.DeleteAllOfOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "DeleteAllOf is not supported by hyperfleetdb")
}

// --- client.StatusClient ---

func (c *dynClient) Status() client.StatusWriter {
	return &dynStatusWriter{dc: c}
}

// --- client.SubResourceClientConstructor ---

func (c *dynClient) SubResource(subResource string) client.SubResourceClient {
	return &dynSubResourceClient{dc: c, sub: subResource}
}

// --- scheme / mapper ---

func (c *dynClient) Scheme() *runtime.Scheme        { return c.scheme }
func (c *dynClient) RESTMapper() apimeta.RESTMapper { return c.restMapper }

func (c *dynClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return resolveGVK(c.scheme, obj)
}

func (c *dynClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	gvk, err := resolveGVK(c.scheme, obj)
	if err != nil {
		return false, err
	}
	mapping, err := c.restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return false, err
	}
	return mapping.Scope.Name() == apimeta.RESTScopeNameNamespace, nil
}

// --- dynStatusWriter ---

type dynStatusWriter struct {
	dc *dynClient
}

func (sw *dynStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return sw.dc.Update(ctx, obj)
}

func (sw *dynStatusWriter) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, "Status().Patch() is not supported by hyperfleetdb; use Status().Update()")
}

// --- dynSubResourceClient ---

type dynSubResourceClient struct {
	dc  *dynClient
	sub string
}

func (s *dynSubResourceClient) Get(_ context.Context, _ client.Object, _ client.Object, _ ...client.SubResourceGetOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, fmt.Sprintf("SubResource(%q).Get() is not supported by hyperfleetdb", s.sub))
}

func (s *dynSubResourceClient) Create(_ context.Context, _ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, fmt.Sprintf("SubResource(%q).Create() is not supported by hyperfleetdb", s.sub))
}

func (s *dynSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if s.sub == "status" {
		return s.dc.Update(ctx, obj)
	}
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, fmt.Sprintf("SubResource(%q).Update() is not supported by hyperfleetdb", s.sub))
}

func (s *dynSubResourceClient) Patch(_ context.Context, _ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
	return apierrors.NewMethodNotSupported(schema.GroupResource{}, fmt.Sprintf("SubResource(%q).Patch() is not supported by hyperfleetdb", s.sub))
}

// --- list helpers ---

// matchesListOptions checks namespace and label selector filters.
func matchesListOptions(obj client.Object, opts client.ListOptions) bool {
	if opts.Namespace != "" && obj.GetNamespace() != opts.Namespace {
		return false
	}
	if opts.LabelSelector != nil && !opts.LabelSelector.Empty() {
		if !opts.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
			return false
		}
	}
	return true
}
