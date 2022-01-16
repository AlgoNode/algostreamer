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
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/algonode/algostreamer/internal/algod"
	"github.com/algonode/algostreamer/internal/utils"

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

func RedisPusher(ctx context.Context, cfg *RedisConfig, blocks chan *algod.BlockWrap, status chan *algod.Status) error {

	var rc *redis.Client = nil

	if cfg == nil {
		return fmt.Errorf("redis config is missing")
	}
	rc = redis.NewClient(&redis.Options{
		Addr:       cfg.Addr,
		Password:   cfg.Password,
		Username:   cfg.Username,
		DB:         cfg.DB,
		MaxRetries: 0,
	})

	go func() {
		for {
			select {
			case s := <-status:
				if rc != nil {
					handleStatusUpdate(ctx, s, rc, cfg)
				}
			case b := <-blocks:
				for {
					//No OPA stuff yet - just populate REDIS streams
					err := handleBlockRedis(ctx, b, rc, cfg, len(blocks))
					if err == nil {
						// if len(blocks) == 0 {
						// 	os.Exit(0)
						// }
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

func RedisGetLastBlock(ctx context.Context, cfg *RedisConfig) (uint64, error) {

	if cfg == nil {
		return 0, fmt.Errorf("redis config is missing")
	}
	rc := redis.NewClient(&redis.Options{
		Addr:       cfg.Addr,
		Password:   cfg.Password,
		Username:   cfg.Username,
		DB:         cfg.DB,
		MaxRetries: 0,
	})

	msg, err := rc.XRevRangeN(ctx, "xblock-v2", "+", "-", 1).Result()
	if err != nil || len(msg) < 1 {
		return 0, fmt.Errorf("[REDIS] error getting last element \n", err)
	}
	a := strings.Split(msg[0].ID, "-")
	if len(a) < 1 {
		return 0, fmt.Errorf("[REDIS] error getting last element - invalid block id %s\n", msg[0].ID)
	}
	r, err := strconv.ParseUint(a[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("[REDIS] error getting last element - invalid block id %s\n", msg[0].ID)
	}

	return r, nil
}

func handleStatusUpdate(ctx context.Context, status *algod.Status, rc *redis.Client, cfg *RedisConfig) error {
	err := rc.HSet(ctx, "NS:"+status.NodeId,
		"round", uint64(status.LastRound),
		"lag", status.LagMs,
		"lcp", status.LastCP).Err()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Err: %s", err)
		return err
	}
	//18650000#YVYJHWA4AJS36CTISLIK7ZVYBMLWK3MFTE7UZGK7YBZBJKQISBWQ
	if status.LastCP != "" {
		a := strings.Split(status.LastCP, "#")
		if len(a) > 0 {
			if err := rc.XAdd(ctx, &redis.XAddArgs{
				Stream: "lcp",
				ID:     fmt.Sprintf("%s-0", a[0]),
				Values: map[string]interface{}{"last": status.LastCP, "time": time.Now()},
			}).Err(); err != nil {
				fmt.Fprintf(os.Stderr, "Err: %s", err)
				return err
			}
			rc.XTrimMaxLen(ctx, "lcp", 1000)
		}
	}
	return nil
}

func commitPaySet(ctx context.Context, b *algod.BlockWrap, rc *redis.Client) {

	pipe := rc.Pipeline()

	for i, txn := range b.Block.Payset {
		//I just love how easy is to get txId nowadays ;)
		_, txId, err := algod.DecodeSignedTxn(b.Block.BlockHeader, txn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Err: %s", err)
			continue
		}

		jTx, err := utils.EncodeJson(txn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Err: %s", err)
			continue
		}
		if err := pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "xtx-v2",
			ID:     fmt.Sprintf("%d-%d", b.Block.Round, i),
			MaxLen: 1000000,
			Approx: true,
			Values: map[string]interface{}{"json": string(jTx), "round": uint64(b.Block.Round), "intra": i, "txid": txId},
		}).Err(); err != nil {
			//fmt.Fprintf(os.Stderr, "Err: %s\n", err)
		}
	}
	pipe.Exec(ctx)
}

func commitBlock(ctx context.Context, b *algod.BlockWrap, rc *redis.Client) {
	if err := rc.XAdd(ctx, &redis.XAddArgs{
		Stream: "xblock-v2",
		ID:     fmt.Sprintf("%d-0", b.Block.Round),
		MaxLen: 100000,
		Approx: true,
		Values: map[string]interface{}{"msgpack": b.BlockRaw, "round": uint64(b.Block.Round)},
	}).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Err: %s\n", err)
	}
}

func handleBlockRedis(ctx context.Context, b *algod.BlockWrap, rc *redis.Client, cfg *RedisConfig, qlen int) error {
	// hash := fmt.Sprintf("b:%d", b.Block.Round/10000)
	// field := strconv.Itoa(int(b.Block.Round % 10000))

	// if err := rc.HSetNX(ctx, hash, field, b.BlockRaw).Err(); err != nil {
	// 	fmt.Printf("SET ERR %d: %s \n", uint64(b.Block.Round), err)
	// 	return err
	// }

	start := time.Now()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		commitPaySet(ctx, b, rc)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		commitBlock(ctx, b, rc)
	}()

	wg.Wait()

	fmt.Fprintf(os.Stderr, "[Redis] Block %d processed in %s. QLen:%d\n", uint64(b.Block.Round), time.Since(start), qlen)
	return nil
}
