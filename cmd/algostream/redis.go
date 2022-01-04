package main

import (
	"context"
	"fmt"

	"github.com/algorand/go-algorand-sdk/types"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-codec/codec"
	"github.com/go-redis/redis/v8"
)

type RedisConfig struct {
	Addr     string `json:"addr"`
	Username string `json:"user"`
	Password string `json:"pass"`
	DB       int    `json:"db"`
}

func redisPusher(ctx context.Context, cfg *SteramerConfig, blocks chan *types.Block) error {

	rc := redis.NewClient(&redis.Options{
		Addr:       cfg.Redis.Addr,
		Password:   cfg.Redis.Password,
		Username:   cfg.Redis.Username,
		DB:         cfg.Redis.DB,
		MaxRetries: 0,
	})

	if cfg.stdout {
		rc = nil
	}

	go func() {
		for {
			select {
			case b := <-blocks:
				var output []byte
				enc := codec.NewEncoderBytes(&output, protocol.JSONStrictHandle)
				err := enc.Encode(b)
				if err != nil {
					continue
				}
				if rc == nil {
					fmt.Println(string(output))
				}
			case <-ctx.Done():
			}

		}
	}()
	return nil
}
