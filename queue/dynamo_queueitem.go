package queue

import (
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// DDBQueueItem represents an item in a DynamoDB queue.
type DDBQueueItem struct {
	ID       string
	Data     QueueItem
	Locked   bool
	LockTime int64
	av       *dynamodb.AttributeValue
	queue    *DynamoDBQueue
}

// NewDDBQueueItem creates a new DDBQueueItem instance with the specified ID, data, locked status, lock time, DynamoDB service, and logger.
func NewDDBQueueItemWithOptions(item map[string]*dynamodb.AttributeValue, q *DynamoDBQueue) *DDBQueueItem {
	var data QueueItem

	err := dynamodbattribute.Unmarshal(item["Data"], &data)
	if err != nil {
		q.logger.WithError(err).Error("Failed to unmarshal QueueItem from DynamoDB attribute value")
		return nil
	}
	// Convert the LockTime attribute to an int64 value
	lockTime, err := strconv.ParseInt(*item["LockTime"].N, 10, 64)
	if err != nil {
		q.logger.WithError(err).Error("Failed to parse LockTime attribute")
		return nil
	}

	return &DDBQueueItem{
		ID:       *item["ID"].S,
		Data:     data,
		Locked:   *item["Locked"].BOOL,
		LockTime: lockTime,
		av:       item["Data"],
		queue:    q,
	}
}

// NewDDBQueueItem creates a new DDBQueueItem instance with the specified ID and data.
func NewDDBQueueItem(data QueueItem, q *DynamoDBQueue) *DDBQueueItem {
	// Marshal the item to a DynamoDB attribute value
	av, err := dynamodbattribute.Marshal(data)
	if err != nil {
		q.logger.WithError(err).Error("Failed to marshal QueueItem to DynamoDB attribute value")
		return nil
	}
	return &DDBQueueItem{
		ID:       q.GenerateQueueItemID(),
		Data:     data,
		Locked:   false,
		LockTime: 0,
		av:       av,
		queue:    q,
	}
}

// AV returns the item as a map of DynamoDB attribute values.
func (item *DDBQueueItem) AV() map[string]*dynamodb.AttributeValue {

	return map[string]*dynamodb.AttributeValue{
		"ID":       {S: aws.String(item.ID)},
		"Data":     item.av,
		"Locked":   {BOOL: aws.Bool(item.Locked)},
		"LockTime": {N: aws.String(strconv.FormatInt(item.LockTime, 10))},
	}

}

// Lock locks the item in the queue.
func (item *DDBQueueItem) Lock() error {
	// Use a DynamoDB expression to lock the item in the queue
	updateExpr, err := expression.NewBuilder().
		WithCondition(IsUnlockedCondition).
		WithUpdate(
			expression.Set(expression.Name("Locked"), expression.Value(true)).Set(expression.Name("LockTime"), expression.Value(time.Now().UnixNano())),
		).
		Build()
	if err != nil {
		return err
	}

	// Update the item in the DynamoDB table
	_, err = item.queue.svc.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:                 aws.String(item.queue.TableName),
		Key:                       map[string]*dynamodb.AttributeValue{"ID": {S: aws.String(item.ID)}},
		UpdateExpression:          updateExpr.Update(),
		ConditionExpression:       updateExpr.Condition(),
		ExpressionAttributeNames:  updateExpr.Names(),
		ExpressionAttributeValues: updateExpr.Values(),
	})
	if err != nil {
		return err
	}

	// Update the "Locked" and "LockTime" attributes of the item
	item.Locked = true
	item.LockTime = time.Now().UnixNano()

	return nil
}

// Unlock unlocks the item in the queue.
func (item *DDBQueueItem) Unlock() error {
	// Use a DynamoDB expression to unlock the item in the queue
	updateExpr, err := expression.NewBuilder().
		WithCondition(expression.Equal(expression.Name("Locked"), expression.Value(true))).
		WithUpdate(
			expression.Set(expression.Name("Locked"), expression.Value(false)).Remove(expression.Name("LockTime")),
		).
		Build()
	if err != nil {
		return err
	}

	// Update the item in the DynamoDB table
	_, err = item.queue.svc.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:                 aws.String(item.queue.TableName),
		Key:                       map[string]*dynamodb.AttributeValue{"ID": {S: aws.String(item.ID)}},
		UpdateExpression:          updateExpr.Update(),
		ConditionExpression:       updateExpr.Condition(),
		ExpressionAttributeNames:  updateExpr.Names(),
		ExpressionAttributeValues: updateExpr.Values(),
	})
	if err != nil {
		return err
	}

	// Update the "Locked" and "LockTime" attributes of the item
	item.Locked = false
	item.LockTime = 0

	return nil
}
