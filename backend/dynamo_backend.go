package backend

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
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
			"id": {
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
			"id": {
				S: aws.String(id),
			},
		},
	}

	_, err := b.db.DeleteItem(input)
	return err
}
