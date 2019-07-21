package danmucrawler

import (
	"context"
	"fmt"
	"helpers"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/samsarahq/go/oops"
)

const (
	chatMessageType = "chatmsg"
)

type DouyuChatMessage struct {
	BadgeNickName string
	BadgeLevel    string
	BadgeRoomId   string
	Level         string
	NickName      string
	RoomId        string
	Text          string
	TimeMs        int64
	UserId        string
}

func (m *DouyuChatMessage) ToDynamoMap() map[string]*dynamodb.AttributeValue {
	result := map[string]*dynamodb.AttributeValue{

		"Level":    &dynamodb.AttributeValue{S: aws.String(m.Level)},
		"NickName": &dynamodb.AttributeValue{S: aws.String(m.NickName)},
		"RoomId":   &dynamodb.AttributeValue{S: aws.String(m.RoomId)},
		"Text":     &dynamodb.AttributeValue{S: aws.String(m.Text)},
		"TimeMs":   &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", m.TimeMs))},
		"UserId":   &dynamodb.AttributeValue{S: aws.String(m.UserId)},
	}
	if len(m.BadgeNickName) > 0 && m.BadgeRoomId != "0" {
		result["BadgeLevel"] = &dynamodb.AttributeValue{S: aws.String(m.BadgeLevel)}
		result["BadgeRoomId"] = &dynamodb.AttributeValue{S: aws.String(m.BadgeRoomId)}
		result["BadgeNickName"] = &dynamodb.AttributeValue{S: aws.String(m.BadgeNickName)}
	}
	return result
}

func (ddc *DouyuDanmuCrawler) parseChatMessage(kv map[string]string) DouyuChatMessage {
	msg := DouyuChatMessage{
		RoomId:        kv["rid"],
		UserId:        kv["uid"],
		NickName:      kv["nn"],
		Text:          kv["txt"],
		Level:         kv["level"],
		BadgeNickName: kv["bnn"],
		BadgeLevel:    kv["bl"],
		BadgeRoomId:   kv["brid"],
		TimeMs:        helpers.TimeToMs(time.Now()),
	}
	return msg
}

func (ddc *DouyuDanmuCrawler) messageParser(ctx context.Context, message string) error {
	kvm := make(map[string]string)
	kvs := strings.Split(message, "/")
	for _, kv := range kvs {
		keyValue := strings.Split(kv, "@=")
		// Escape: '/' -> '@S'; '@' -> '@A'
		if len(keyValue) == 2 {
			value := strings.Replace(keyValue[1], "@S", "/", -1)
			value = strings.Replace(value, "@A", "@", -1)
			kvm[keyValue[0]] = value
		}
	}
	if kvm["type"] == chatMessageType {
		prasedMessage := ddc.parseChatMessage(kvm)
		log.Println(prasedMessage)
		return oops.Wrapf(ddc.model.write(ctx, prasedMessage), "parseChatMessage")
	}
	return nil
}
