package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// AttributeDocumentID is the DynamoDB partition key attribute name used by all
// desire-document tables.
const AttributeDocumentID = "documentID"

// DynamoDBMetadataAccessor provides generic access to the DynamoDB-specific
// metadata fields (DocumentID, Version, UpdateTime, CreateTime) that every
// stored type carries via an embedded metadata struct.
type DynamoDBMetadataAccessor interface {
	GetDocumentID() string
	GetVersion() int64
	GetUpdateTime() time.Time
	GetCreateTime() time.Time
	SetDocumentID(string)
	SetVersion(int64)
	SetUpdateTime(time.Time)
	SetCreateTime(time.Time)
}

// Item is the type constraint for the generic CRUD implementations. Every
// stored type must be a pointer to its value type, implement
// DynamoDBMetadataAccessor and KubeContentAccessor, and provide a DeepCopy
// method for safe mutation before writes.
type Item[T any] interface {
	*T
	DynamoDBMetadataAccessor
	KubeContentAccessor
	DeepCopy() *T
}

// isConditionalCheckFailed reports whether err is a DynamoDB
// ConditionalCheckFailedException, which indicates an optimistic concurrency
// or existence-check failure.
func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

// itemToValue converts a raw DynamoDB item map into a typed value.
// It uses attributevalue.UnmarshalMap for struct fields and then manually
// reads the documentID partition key and KubeContent string attributes.
func itemToValue[T any, PT Item[T]](item map[string]types.AttributeValue) (*T, error) {
	var obj T
	if err := attributevalue.UnmarshalMap(item, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal item: %w", err)
	}
	pt := PT(&obj)

	// Extract the partition key (documentID) — not in the struct body because
	// the struct field carries dynamodbav:"-".
	if av, ok := item[AttributeDocumentID]; ok {
		if sv, ok := av.(*types.AttributeValueMemberS); ok {
			pt.SetDocumentID(sv.Value)
		}
	}

	// Read kubeContent string attributes.
	if err := KubeContentReadFromItem(pt, item); err != nil {
		return nil, err
	}
	return &obj, nil
}

// valueToItem converts a typed value to a DynamoDB item map ready for PutItem.
// The documentID partition key is added explicitly. KubeContent fields are
// merged in as S attributes.
func valueToItem[T any, PT Item[T]](pt PT) (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(pt)
	if err != nil {
		return nil, fmt.Errorf("marshal item: %w", err)
	}
	// Explicitly set the partition key.
	item[AttributeDocumentID] = &types.AttributeValueMemberS{Value: pt.GetDocumentID()}

	// Merge kubeContent string attributes.
	kubeAttrs, err := KubeContentAttributeValues(pt)
	if err != nil {
		return nil, err
	}
	for k, v := range kubeAttrs {
		item[k] = v
	}
	return item, nil
}

// -------------------------------------------------------------------
// CRUD — generic Get/List/ListSince/Create/Replace/Delete
// -------------------------------------------------------------------

// CRUD provides generic CRUD operations over a single DynamoDB table whose
// items implement Item[T].
//
// Get returns ErrNotFound when no item exists for the given documentID.
// Create returns ErrAlreadyExists when the item already exists.
// Replace returns ErrPreconditionFailed when the item's version does not match
// the caller's expected version (optimistic concurrency conflict).
type CRUD[T any, PT Item[T]] struct {
	Client *dynamodb.Client
	Table  string
}

// Get fetches a single item by documentID using a strongly-consistent read.
func (c *CRUD[T, PT]) Get(ctx context.Context, documentID string) (*T, error) {
	out, err := c.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(c.Table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			AttributeDocumentID: &types.AttributeValueMemberS{Value: documentID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb Get %s/%s: %w", c.Table, documentID, err)
	}
	if len(out.Item) == 0 {
		return nil, newNotFoundError()
	}
	return itemToValue[T, PT](out.Item)
}

// List returns all items in the table using a strongly-consistent paginated
// Scan. This is used for full relists by the poll watcher machinery.
func (c *CRUD[T, PT]) List(ctx context.Context) ([]*T, error) {
	var result []*T
	paginator := dynamodb.NewScanPaginator(c.Client, &dynamodb.ScanInput{
		TableName:      aws.String(c.Table),
		ConsistentRead: aws.Bool(true),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("dynamodb Scan %s: %w", c.Table, err)
		}
		for _, item := range page.Items {
			obj, err := itemToValue[T, PT](item)
			if err != nil {
				return nil, fmt.Errorf("dynamodb Scan %s convert: %w", c.Table, err)
			}
			result = append(result, obj)
		}
	}
	return result, nil
}

// ListSince returns all items whose updateTime attribute is strictly after
// since. It uses an eventually consistent Scan with a FilterExpression.
//
// Callers MUST treat returned items as notifications only — they must fetch
// authoritative data via a consistent Get before acting.
func (c *CRUD[T, PT]) ListSince(ctx context.Context, since time.Time) ([]*T, error) {
	var result []*T
	paginator := dynamodb.NewScanPaginator(c.Client, &dynamodb.ScanInput{
		TableName:        aws.String(c.Table),
		ConsistentRead:   aws.Bool(false),
		FilterExpression: aws.String("updateTime > :since"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":since": &types.AttributeValueMemberS{
				Value: since.UTC().Format(time.RFC3339Nano),
			},
		},
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("dynamodb Scan (since) %s: %w", c.Table, err)
		}
		for _, item := range page.Items {
			obj, err := itemToValue[T, PT](item)
			if err != nil {
				return nil, fmt.Errorf("dynamodb Scan (since) %s convert: %w", c.Table, err)
			}
			result = append(result, obj)
		}
	}
	return result, nil
}

// Create writes a new item with version=1, setting createTime and updateTime
// to now. Returns ErrAlreadyExists if an item with the same documentID already
// exists.
func (c *CRUD[T, PT]) Create(ctx context.Context, obj *T) (*T, error) {
	pt := PT(obj)
	docID := pt.GetDocumentID()
	if docID == "" {
		return nil, fmt.Errorf("dynamodb Create %s: DocumentID is empty", c.Table)
	}

	now := time.Now().UTC()
	out := pt.DeepCopy()
	op := PT(out)
	op.SetVersion(1)
	op.SetUpdateTime(now)
	op.SetCreateTime(now)

	item, err := valueToItem[T, PT](op)
	if err != nil {
		return nil, fmt.Errorf("dynamodb Create %s/%s: %w", c.Table, docID, err)
	}

	_, err = c.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(c.Table),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": AttributeDocumentID,
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return nil, newAlreadyExistsError()
		}
		return nil, fmt.Errorf("dynamodb Create %s/%s: %w", c.Table, docID, err)
	}
	return out, nil
}

// Replace writes an updated item using optimistic concurrency. The item's
// current version must match obj's version; on success the returned item has
// version+1 and an updated updateTime. Returns ErrPreconditionFailed on
// version mismatch.
func (c *CRUD[T, PT]) Replace(ctx context.Context, obj *T) (*T, error) {
	pt := PT(obj)
	docID := pt.GetDocumentID()
	expectedVersion := pt.GetVersion()

	out := pt.DeepCopy()
	op := PT(out)
	op.SetVersion(expectedVersion + 1)
	op.SetUpdateTime(time.Now().UTC())

	item, err := valueToItem[T, PT](op)
	if err != nil {
		return nil, fmt.Errorf("dynamodb Replace %s/%s: %w", c.Table, docID, err)
	}

	_, err = c.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(c.Table),
		Item:                item,
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#v": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected": &types.AttributeValueMemberN{
				Value: strconv.FormatInt(expectedVersion, 10),
			},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return nil, newPreconditionFailedError()
		}
		return nil, fmt.Errorf("dynamodb Replace %s/%s: %w", c.Table, docID, err)
	}
	return out, nil
}

// Delete removes an item by documentID. It is idempotent — deleting a
// non-existent item does not return an error.
func (c *CRUD[T, PT]) Delete(ctx context.Context, documentID string) error {
	_, err := c.Client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(c.Table),
		Key: map[string]types.AttributeValue{
			AttributeDocumentID: &types.AttributeValueMemberS{Value: documentID},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb Delete %s/%s: %w", c.Table, documentID, err)
	}
	return nil
}
