package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func main() {
	db := dynamodb.New(session.New(), &aws.Config{
		Region:   aws.String("ap-northeast-1"),
		Endpoint: aws.String("http://localhost:8000"),
	})
	result, err := db.Query(&dynamodb.QueryInput{
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
		fmt.Println(err.Error())
	}
	fmt.Println(result.Items)
}
