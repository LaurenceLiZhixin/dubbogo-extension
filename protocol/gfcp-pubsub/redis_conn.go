package gfcp_pubsub

import (
	"bytes"
	"github.com/go-redis/redis/v8"
	"net"
	"time"
)

func NewRedisConn(message *redis.Message) net.Conn {
	messageBuffer := bytes.NewBuffer([]byte(message.Payload))
	return &RedisConn{
		messageBuffer: messageBuffer,
	}
}

type RedisConn struct {
	messageBuffer *bytes.Buffer
}

func (r *RedisConn) Read(b []byte) (n int, err error) {
	return r.messageBuffer.Read(b)
}

func (r *RedisConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (r *RedisConn) Close() error {
	return nil
}

func (r *RedisConn) LocalAddr() net.Addr {
	return &net.IPAddr{} // todo
}

func (r *RedisConn) RemoteAddr() net.Addr {
	return &net.IPAddr{} // todo
}

func (r *RedisConn) SetDeadline(t time.Time) error {
	return nil
}

func (r *RedisConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (r *RedisConn) SetWriteDeadline(t time.Time) error {
	return nil
}
