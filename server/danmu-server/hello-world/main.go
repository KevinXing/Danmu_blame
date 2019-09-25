package main

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/samsarahq/go/oops"
)

var (
	// ErrNoIP No IP found in response
	ErrNoIP = errors.New("No IP in HTTP response")

	// ErrNon200Response non 200 status code in response
	ErrNon200Response = errors.New("Non 200 Response found")

	QueryTimeout = time.Second * 30

	db = dynamodb.New(
		session.New(),
		&aws.Config{
			Region:   aws.String("ap-northeast-1"),
			Endpoint: aws.String("http://dynamodb:8000"),
		})
)

func query(ctx context.Context) (string, error) {
	result, err := db.QueryWithContext(ctx, &dynamodb.QueryInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":RoomId": {
				S: aws.String("60937"),
			},
			":MaxMs": {
				N: aws.String("2542041403000"),
			},
		},
		KeyConditionExpression: aws.String("RoomId = :RoomId AND TimeMs < :MaxMs"),
		TableName:              aws.String("Messages"),
	})
	if err != nil {
		return "", oops.Wrapf(err, "query fails")
	}
	return result.GoString(), nil

}

func handler(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	ctx, _ := context.WithTimeout(context.Background(), QueryTimeout)
	body, err := query(ctx)
	return events.APIGatewayProxyResponse{
		Body:       body,
		StatusCode: 200,
	}, err
}

func main() {
	lambda.Start(handler)
}
