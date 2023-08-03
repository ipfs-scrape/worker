package queue

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// DynamoDBQueue represents a queue backed by a DynamoDB table.
type DynamoDBQueue struct {
	TableName string
	QueueName string
	svc       *dynamodb.DynamoDB
	logger    *logrus.Entry
}

// NewDynamoDBQueue creates a new DynamoDBQueue instance.
func NewDynamoDBQueue(tableName, queueName string, svc *dynamodb.DynamoDB) (Queue, error) {
	// Check if the table exists
	_, err := svc.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		return nil, err
	}

	return &DynamoDBQueue{
		TableName: tableName,
		QueueName: queueName,
		svc:       svc,
		logger:    logrus.WithField("component", "DynamoDBQueue"),
	}, nil
}

// GenerateQueueItemID generates a new ID for a QueueItem.
func (q *DynamoDBQueue) GenerateQueueItemID() string {
	// can do better than this at scale, but for now, simple ordered key
	// i'm not even sure if that matters in dynamo indexing
	return fmt.Sprintf("queue-%s-%d", q.QueueName, time.Now().UnixNano())
}

// Push adds an item to the queue.
func (q *DynamoDBQueue) AddItem(queueItem QueueItem) error {
	id := uuid.New().String()

	ddbitem := NewDDBQueueItem(queueItem, q)
	if ddbitem == nil {
		return fmt.Errorf("failed to create new DDBQueueItem")
	}

	// Put the item in the DynamoDB table
	_, err := q.svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(q.TableName),
		Item:      ddbitem.AV(),
	})

	if err != nil {
		q.logger.WithError(err).Error("Failed to put item in DynamoDB table")
		return err
	}

	q.logger.WithField("ID", id).Info("Item added to queue")
	return nil
}

// Pop locks and returns the next item from the queue.
func (q *DynamoDBQueue) GetNextItem() (QueueItem, error) {

	expr, err := expression.NewBuilder().
		WithFilter(expression.And(
			IsUnlockedCondition,
			expression.BeginsWith(expression.Name("ID"), fmt.Sprintf("queue-%s-", q.QueueName)),
		)).
		Build()

	if err != nil {
		q.logger.WithError(err).Error("Failed to build DynamoDB expression")
		return QueueItem{}, err
	}

	// Define the input parameters for the Scan operation
	input := &dynamodb.ScanInput{
		TableName:                 aws.String(q.TableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		Limit:                     aws.Int64(1),
	}

	// Execute the Scan operation
	result, err := q.svc.Scan(input)
	if err != nil {
		q.logger.WithError(err).Error("Failed to scan DynamoDB table")
		return QueueItem{}, err
	}

	// Check if any items were returned by the Scan operation
	if len(result.Items) == 0 {
		q.logger.Info("No items available in queue")
		return QueueItem{}, fmt.Errorf("no items available in queue")
	}

	// Unmarshal and Lock
	item := NewDDBQueueItemWithOptions(result.Items[0], q)
	err = item.Lock()
	if err != nil {
		q.logger.WithError(err).Warn("Something may have beat us to the lock. Move on!")
		return QueueItem{}, fmt.Errorf("Something may have beat us to the lock. Move on!")
	}

	return item.Data, nil
}

// Done removes the specified item from DynamoDB.
func (q *DynamoDBQueue) Done(item QueueItem) error {
	// Create a new expression to delete the item from DynamoDB
	deleteExpr, err := expression.NewBuilder().
		WithCondition(expression.
			Equal(expression.Key("Data.id"), expression.Value(item.ID))).Build()
	if err != nil {
		q.logger.WithError(err).Error("Failed to build delete condition expression")
		return err
	}

	// Define the input parameters for the Scan operation
	scanInput := &dynamodb.ScanInput{
		TableName:                 aws.String(q.TableName),
		ExpressionAttributeNames:  deleteExpr.Names(),
		ExpressionAttributeValues: deleteExpr.Values(),
		FilterExpression:          deleteExpr.Filter(),
		Limit:                     aws.Int64(1),
	}

	// Execute the Scan operation
	result, err := q.svc.Scan(scanInput)
	if err != nil {
		q.logger.WithError(err).Error("Failed to scan DynamoDB table")
		return err
	}

	// Check if any items were returned by the Scan operation
	if len(result.Items) == 0 {
		q.logger.Info("No items available in table")
		return nil
	}

	// Delete the item from DynamoDB
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": result.Items[0]["ID"],
		},
		TableName:                 aws.String(q.TableName),
		ConditionExpression:       deleteExpr.Condition(),
		ExpressionAttributeNames:  deleteExpr.Names(),
		ExpressionAttributeValues: deleteExpr.Values(),
	}

	_, err = q.svc.DeleteItem(input)
	if err != nil {
		q.logger.WithError(err).Error("Failed to delete item from DynamoDB")
		return err
	}

	q.logger.WithField("ID", *result.Items[0]["ID"].S).Info("Item deleted from DynamoDB")

	return nil
}
