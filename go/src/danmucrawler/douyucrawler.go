package danmucrawler

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/KevinXing/Danmu_blame/go/src/helpers"
	"github.com/samsarahq/go/oops"
)

const (
	douyuDanmuServer = "119.96.201.28:8601"
	//douyuDanmuServer  = "47.254.90.45:12601"
	writeDeadline     = time.Minute
	readDeadline      = time.Minute
	heartbeatInterval = time.Second * 45
	headLength        = 12
)

type DouyuDanmuCrawler struct {
	conn   net.Conn
	RoomId string
	model  *messageModel
}

func NewDouyuDanmuCrawler(ctx context.Context, roomId string) (*DouyuDanmuCrawler, error) {
	ddc := &DouyuDanmuCrawler{
		RoomId: roomId,
	}
	return ddc, nil
}

// https://github.com/HowardTangHw/DouyuTool/blob/master/%E6%96%97%E9%B1%BC%E5%BC%B9%E5%B9%95%E6%9C%8D%E5%8A%A1%E5%99%A8%E7%AC%AC%E4%B8%89%E6%96%B9%E6%8E%A5%E5%85%A5%E5%8D%8F%E8%AE%AEv1.6.2.pdf
func (ddc DouyuDanmuCrawler) send(message string) error {
	// Header format: little endian
	// message length: 4 bytes;
	// message length: 4 bytes;
	// message type(689): 2 bytes;
	// encode(0): 1 byte;
	// reserved(0): 1 byte;

	messageLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(messageLength, uint32(len(message)+9)) // messageLength * 2 + '\0'

	var buffer bytes.Buffer
	buffer.Write(messageLength)
	buffer.Write(messageLength)
	// type
	binary.Write(&buffer, binary.LittleEndian, uint16(689))
	// encode
	binary.Write(&buffer, binary.LittleEndian, uint8(0))
	// reserved
	binary.Write(&buffer, binary.LittleEndian, uint8(0))
	// message
	buffer.Write([]byte(message))
	// message body end
	binary.Write(&buffer, binary.LittleEndian, uint8(0))
	if err := ddc.conn.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
		return oops.Wrapf(err, "set write deadline fail")
	}
	if n, err := ddc.conn.Write(buffer.Bytes()); err != nil || n != buffer.Len() {
		return oops.Wrapf(err, "send message fail, already sent %d bytes\n", n)
	}
	return nil
}

func (ddc *DouyuDanmuCrawler) init() error {
	conn, err := net.Dial("tcp", douyuDanmuServer)
	if err != nil {
		return oops.Wrapf(err, "net.Dial")
	}
	ddc.conn = conn
	log.Println(ddc.conn)

	// login message
	loginMessage := fmt.Sprintf("type@=loginreq/roomid@=%s/", ddc.RoomId)
	if err := ddc.send(loginMessage); err != nil {
		return oops.Wrapf(err, "send login message fail")
	}

	// Damnu group
	joinGroupMessage := fmt.Sprintf("type@=joingroup/gid@=-9999/rid@=%s/", ddc.RoomId)
	if err := ddc.send(joinGroupMessage); err != nil {
		return oops.Wrapf(err, "send group message fail")
	}
	return nil
}

func (ddc *DouyuDanmuCrawler) heartBeat() error {
	heartBeatMessage := "type@=mrkl/"
	for {
		// TODO: ingore the error of heartbeat now.
		if err := ddc.send(heartBeatMessage); err != nil {
			return oops.Wrapf(err, "send")
		}
		time.Sleep(heartbeatInterval)
	}
}

func (ddc *DouyuDanmuCrawler) read(ctx context.Context) error {
	for {
		headerBuffer, err := helpers.TcpReadFixedSize(ctx, ddc.conn, headLength, readDeadline)
		if err != nil {
			return oops.Wrapf(err, "TcpReadFixedSize")
		}

		messageLength := binary.LittleEndian.Uint32(headerBuffer[0:]) - uint32(8)
		messageBuffer, err := helpers.TcpReadFixedSize(ctx, ddc.conn, int(messageLength), readDeadline)
		if err != nil {
			return oops.Wrapf(err, "TcpReadFixedSize")
		}
		//log.Println("ID:" + string(ddc.Id()) + ":" + string(messageBuffer))
		if err := ddc.messageParser(ctx, string(messageBuffer)); err != nil {
			log.Printf("Warning: messageParser fails: %s\n", err.Error())
		}
	}
}

func (ddc *DouyuDanmuCrawler) close() {
	if ddc.conn != nil {
		ddc.send("type@=logout/")
		ddc.conn.Close()
	}
}

// Run implement interface Crawler
func (ddc *DouyuDanmuCrawler) Run(ctx context.Context) error {
	log.Printf("Start crawler %s\n", ddc.RoomId)
	defer ddc.close()

	if err := ddc.init(); err != nil {
		return oops.Wrapf(err, "init douyu crawler")
	}

	c := make(chan error)
	go func() { c <- ddc.heartBeat() }()
	go func() { c <- ddc.read(ctx) }()

	select {
	case <-ctx.Done():
		close(c)
		return oops.Wrapf(ctx.Err(), "ctx cancelled")
	case err := <-c:
		return oops.Wrapf(err, "Run err")
	}

}

// Id implement inferface Crawler
func (ddc *DouyuDanmuCrawler) Id() CrawlerId {
	return CrawlerId("Douyu_" + ddc.RoomId)
}

// SetModel implements interface Crawler
func (ddc *DouyuDanmuCrawler) SetModel(model *messageModel) {
	ddc.model = model
}
