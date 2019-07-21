package danmucrawler

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/samsarahq/go/oops"
)

func MaybeCreateMessageTable(ctx context.Context, svc *dynamodb.DynamoDB) (*dynamodb.TableDescription, error) {
	tableName := "Messages"
	// Describe table first to check if it is already existing.
	describeInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	result, err := svc.DescribeTableWithContext(ctx, describeInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() != dynamodb.ErrCodeResourceNotFoundException {
				return nil, oops.Wrapf(aerr, "DescribeTable AWS")
			}
		} else {
			return nil, oops.Wrapf(err, "DescribeTable")
		}
	} else {
		// Table is existing.
		return result.Table, nil
	}
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("RoomId"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("TimeMs"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("UserId"),
				AttributeType: aws.String("S"),
			},
		},
		BillingMode: aws.String("PAY_PER_REQUEST"),
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String("UserIdIndex"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("UserId"),
						KeyType:       aws.String("HASH"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("RoomId"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("TimeMs"),
				KeyType:       aws.String("RANGE"),
			},
		},
		TableName: aws.String(tableName),
	}
	_, err = svc.CreateTableWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			return nil, oops.Wrapf(aerr, "CreateTable AWS error")
		}
		return nil, oops.Wrapf(err, "CreateTable")
	}
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, oops.Wrapf(ctx.Err(), "ctx.Done")
		case <-ticker.C:
			result, err := svc.DescribeTableWithContext(ctx, describeInput)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					if aerr.Code() != dynamodb.ErrCodeResourceNotFoundException {
						continue
					}
				} else {
					return nil, oops.Wrapf(err, "DescribeTable")
				}
			} else {
				// Table is existing.
				return result.Table, nil
			}
		}
	}
}

const (
	messageChanSize = 10000
	// The maximum batch size is 25: https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#DynamoDB.BatchWriteItem
	batchWriteSize = 25
)

type messageModel struct {
	dynamoDBClient *dynamodb.DynamoDB
	messageChan    chan DouyuChatMessage
	table          *dynamodb.TableDescription
}

func newMessageModel(ctx context.Context) (*messageModel, error) {
	log.Println("New Message Model Start")
	model := &messageModel{
		dynamoDBClient: dynamodb.New(session.New(), &aws.Config{
			Region:   aws.String("ap-northeast-1"),
			Endpoint: aws.String("http://localhost:8000"),
		}),
		messageChan: make(chan DouyuChatMessage, messageChanSize),
	}
	tableCtx, tableCancel := context.WithTimeout(ctx, time.Minute)
	defer tableCancel()
	table, err := MaybeCreateMessageTable(tableCtx, model.dynamoDBClient)
	if err != nil {
		return nil, oops.Wrapf(err, "MaybeCreateMessageTable")
	}
	model.table = table
	go model.persist(ctx)
	log.Println("New Message Model Success")
	return model, nil
}

func (m *messageModel) write(ctx context.Context, message DouyuChatMessage) error {
	select {
	case <-ctx.Done():
		return oops.Wrapf(ctx.Err(), "ctx.Done")
	case m.messageChan <- message:
	default:
		log.Println("Warning: message channel is full")
	}
	return nil
}

func (m *messageModel) persist(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 20)
	requests := make([]*dynamodb.WriteRequest, 0, batchWriteSize)
	for {
		select {
		case <-ctx.Done():
			return oops.Wrapf(ctx.Err(), "ctx.Done")
		case <-ticker.C:
			if len(requests) > 0 {
				_, err := m.dynamoDBClient.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]*dynamodb.WriteRequest{
						*m.table.TableName: requests,
					},
				})
				// TODO: log in cloudwatch.
				if err != nil {
					log.Println(requests)
					log.Println(err.Error())
				}
				requests = make([]*dynamodb.WriteRequest, 0, batchWriteSize)
			}
		case message := <-m.messageChan:
			requests = append(requests, &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: message.ToDynamoMap(),
				},
			})
			if len(requests) == batchWriteSize {
				_, err := m.dynamoDBClient.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]*dynamodb.WriteRequest{
						*m.table.TableName: requests,
					},
				})
				// TODO: log in cloudwatch.
				if err != nil {
					log.Println(requests)
					log.Println(err.Error())
				}
				requests = make([]*dynamodb.WriteRequest, 0, batchWriteSize)
			}
		}
	}
}
