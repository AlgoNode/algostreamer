package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/algorand/go-algorand-sdk/types"
	"github.com/go-redis/redis/v8"
)

type RedisConfig struct {
	Addr     string `json:"addr"`
	Username string `json:"user"`
	Password string `json:"pass"`
	DB       int    `json:"db"`
}

func redisPusher(ctx context.Context, cfg *SteramerConfig, blocks chan *types.Block) error {
	redis.NewClient(&redis.Options{
		Addr:       cfg.Redis.Addr,
		Password:   cfg.Redis.Password,
		Username:   cfg.Redis.Username,
		DB:         cfg.Redis.DB,
		MaxRetries: 0,
	})

	go func() {
		for {
			select {
			case b := <-blocks:
				jb, _ := json.Marshal(b)
				fmt.Println(jb)
			case <-ctx.Done():
			}

		}
	}()
	return nil
}
