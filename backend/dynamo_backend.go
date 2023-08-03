package backend

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type DynamoDBBackend struct {
	db        *dynamodb.DynamoDB
	tableName string
}

func NewDynamoDBBackend(tableName string, svc *dynamodb.DynamoDB) (Backend, error) {
	_, err := svc.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		return nil, err
	}

	return &DynamoDBBackend{db: svc, tableName: tableName}, nil
}

func (b *DynamoDBBackend) Create(metadata any) error {
	av, err := dynamodbattribute.MarshalMap(metadata)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(b.tableName),
		Item:      av,
	}

	_, err = b.db.PutItem(input)
	return err
}

func (b *DynamoDBBackend) Read(id string) (any, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(b.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(id),
			},
		},
	}

	result, err := b.db.GetItem(input)
	if err != nil {
		return nil, err
	}

	if result.Item == nil {
		return nil, errors.New("item not found")
	}

	var metadata any
	err = dynamodbattribute.UnmarshalMap(result.Item, &metadata)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (b *DynamoDBBackend) Update(metadata any) error {
	av, err := dynamodbattribute.MarshalMap(metadata)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(b.tableName),
		Item:      av,
	}

	_, err = b.db.PutItem(input)
	return err
}

func (b *DynamoDBBackend) Delete(id string) error {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(b.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(id),
			},
		},
	}

	_, err := b.db.DeleteItem(input)
	return err
}
func (b *DynamoDBBackend) Scan(prefix string) ([]any, error) {
	var items []any

	expr, err := expression.NewBuilder().WithFilter(expression.BeginsWith(expression.Name("ID"), prefix)).Build()
	if err != nil {
		return nil, err
	}

	var lastEvaluatedKey map[string]*dynamodb.AttributeValue
	for {
		input := &dynamodb.ScanInput{
			TableName:                 aws.String(b.tableName),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Limit:                     aws.Int64(100),
			ExclusiveStartKey:         lastEvaluatedKey,
		}

		result, err := b.db.Scan(input)
		if err != nil {
			return nil, err
		}

		err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &items)
		if err != nil {
			return nil, err
		}

		lastEvaluatedKey = result.LastEvaluatedKey
		if lastEvaluatedKey == nil {
			break
		}
	}

	return items, nil
}
