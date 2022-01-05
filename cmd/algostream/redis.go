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
