package danmucrawler

import (
	"log"
	"strings"
)

const (
	chatMessageType = "chatmsg"
)

type DouyuChatMessage struct {
	groupId       int
	roomId        int
	userId        int64
	nickName      string
	text          string
	danmuId       int64
	level         int
	gift          int
	color         int
	clientType    int
	nobleLevel    int
	badgeNickName string
	badgeLevel    int
	badgeRoomId   int
}

func (ddc *DouyuDanmuCrawler) messageParser(message string) {
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
	switch messageType := kvm["type"]; messageType {
	case chatMessageType:
		ddc.chatMessageParser(kvm)
	}

}

func (ddc *DouyuDanmuCrawler) chatMessageParser(kv map[string]string) {
	log.Println(kv["nn"] + ":" + kv["txt"])
}
