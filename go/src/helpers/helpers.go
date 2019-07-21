package helpers

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/samsarahq/go/oops"
)

// TcpReadFixedSize try to read m bytes from conn, it returns when read is done or the ctx is cancelled.
func TcpReadFixedSize(ctx context.Context, conn net.Conn, m int, timeout time.Duration) ([]byte, error) {
	ctx, _ = context.WithTimeout(ctx, timeout)
	c := make(chan error, 1)
	var messageBuffer bytes.Buffer
	go func() {
		var err error
		defer func() { c <- err }()
		for m > 0 {
			tmpBuffer := make([]byte, m)
			n, err := conn.Read(tmpBuffer)
			if err != nil {
				return
			}
			m = m - n
			if _, err = messageBuffer.Write(tmpBuffer); err != nil {
				return
			}
		}
		return
	}()
	select {
	case <-ctx.Done():
		close(c)
		return messageBuffer.Bytes(), oops.Wrapf(ctx.Err(), "ctx")
	case err := <-c:
		return messageBuffer.Bytes(), oops.Wrapf(err, "read")
	}
}

func TimeToMs(t time.Time) int64 {
	return t.UnixNano() / 1e6
}
