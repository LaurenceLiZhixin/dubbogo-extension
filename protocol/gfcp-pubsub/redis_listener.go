package gfcp_pubsub

import (
	"context"
	"github.com/go-redis/redis/v8"
	"net"
)

func NewRedisListener() net.Listener {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	sub := rdb.Subscribe(context.Background(), "channel1")
	return &RedisListener{
		rdb:    rdb,
		pubsub: sub,
	}
}

type RedisListener struct {
	rdb    *redis.Client
	pubsub *redis.PubSub
}

func (r *RedisListener) Accept() (net.Conn, error) {
	// todo subscribe from redis
	iface, err := r.pubsub.ReceiveMessage(context.Background())
	if err != nil {
		// handle error
		return nil, err
	}

	return NewRedisConn(iface), nil

}

func (r *RedisListener) Close() error {
	return nil
}

func (r *RedisListener) Addr() net.Addr {
	return &net.IPAddr{}
}
