// Copyright (C) 2022 AlgoNode Org.
//
// algostreamer is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// algostreamer is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with algostreamer.  If not, see <https://www.gnu.org/licenses/>.

package rdb

import (
	"context"
	"fmt"
	"time"

	"github.com/algonode/algostreamer/internal/algod"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-codec/codec"
	"github.com/go-redis/redis/v8"
)

const (
	PFX_Node = "nd:"
)

type RedisConfig struct {
	Addr     string `json:"addr"`
	Username string `json:"user"`
	Password string `json:"pass"`
	DB       int    `json:"db"`
}

func handleBlockStdOut(b *algod.BlockWrap) error {
	var output []byte
	enc := codec.NewEncoderBytes(&output, protocol.JSONStrictHandle)
	err := enc.Encode(b)
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func handleStatusUpdate(ctx context.Context, status *algod.Status, rc *redis.Client, cfg *RedisConfig) error {
	// err := rc.HSet(ctx, PFX_Node+cfg.MyKey,
	// 	"round", uint64(status.LastRound),
	// 	"lag", status.LagMs).Err()
	// if err != nil {
	// 	fmt.Printf("Err: %s", err)
	// } else {
	fmt.Printf("SET OK %d \n", status.LastRound)
	// }
	return nil
}

func handleBlockRedis(ctx context.Context, b *algod.BlockWrap, rc *redis.Client, cfg *RedisConfig) error {
	fmt.Printf("SET OK %d from %s \n", uint64(b.Block.Round), b.Src)
	return nil
}

func RedisPusher(ctx context.Context, cfg *RedisConfig, blocks chan *algod.BlockWrap, status chan *algod.Status) error {

	var rc *redis.Client = nil

	if cfg != nil {
		rc = redis.NewClient(&redis.Options{
			Addr:       cfg.Addr,
			Password:   cfg.Password,
			Username:   cfg.Username,
			DB:         cfg.DB,
			MaxRetries: 0,
		})
	}

	go func() {
		for {
			select {
			case s := <-status:
				if rc != nil {
					handleStatusUpdate(ctx, s, rc, cfg)
				}
			case b := <-blocks:
				for {
					if rc == nil {
						handleBlockStdOut(b)
						break
					}
					err := handleBlockRedis(ctx, b, rc, cfg)
					if err == nil {
						break
					}
					time.Sleep(time.Second)
				}

			case <-ctx.Done():
			}

		}
	}()
	return nil
}
