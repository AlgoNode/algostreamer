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
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/algonode/algostreamer/internal/algod"
	"github.com/algonode/algostreamer/internal/utils"
	"github.com/algorand/go-algorand-sdk/types"

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
		PoolSize:   50,
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
		fmt.Fprintf(os.Stderr, "Err: %s\n", err)
		return err
	}
	if status.LastCP != "" {
		a := strings.Split(status.LastCP, "#")
		if len(a) > 0 {
			if err := rc.XAdd(ctx, &redis.XAddArgs{
				Stream: "lcp",
				ID:     fmt.Sprintf("%s-0", a[0]),
				Values: map[string]interface{}{"last": status.LastCP, "time": time.Now()},
			}).Err(); err != nil {
				fmt.Fprintf(os.Stderr, "Err: %s\n", err)
				return err
			}
			rc.XTrimMaxLen(ctx, "lcp", 1000)
		}
	}
	return nil
}

type TxWrap struct {
	TxId  string                  `json:"txid"`
	Txn   *types.SignedTxnInBlock `json:"txn"`
	Round uint64                  `json:"round"`
	Intra int                     `json:"intra"`
	Key   string                  `json:"xtx-v2"`
}

func getTopics(txw *TxWrap) []string {
	t := make(map[string]struct{})

	addACC := func(a types.Address) {
		if a.IsZero() {
			return
		}
		t["ACC:"+a.String()] = struct{}{}
	}

	addASA := func(a types.AssetIndex) {
		if a == 0 {
			return
		}
		t[fmt.Sprintf("ASA:%d", uint64(a))] = struct{}{}
	}

	addAPP := func(a types.AppIndex) {
		if a == 0 {
			return
		}
		t[fmt.Sprintf("APP:%d", uint64(a))] = struct{}{}
	}

	tx := &txw.Txn.Txn
	addACC(tx.Sender)
	addACC(txw.Txn.AuthAddr)
	switch tx.Type {
	case types.PaymentTx:
		addACC(tx.Receiver)
		addACC(tx.CloseRemainderTo)
	case types.AssetTransferTx:
		addACC(tx.AssetSender)
		addACC(tx.AssetReceiver)
		addACC(tx.CloseRemainderTo)
		addASA(tx.XferAsset)
	case types.AssetConfigTx:
		addACC(tx.AssetConfigTxnFields.AssetParams.Manager)
		addACC(tx.AssetConfigTxnFields.AssetParams.Reserve)
		addACC(tx.AssetConfigTxnFields.AssetParams.Clawback)
		addACC(tx.AssetConfigTxnFields.AssetParams.Freeze)
		addASA(tx.ConfigAsset)
	case types.AssetFreezeTx:
		addACC(tx.AssetFreezeTxnFields.FreezeAccount)
		addASA(tx.FreezeAsset)
	case types.ApplicationCallTx:
		addAPP(tx.ApplicationID)
		for i := range tx.ForeignApps {
			addAPP(tx.ForeignApps[i])
		}
		for i := range tx.ForeignAssets {
			addASA(tx.ForeignAssets[i])
		}
		for i := range tx.Accounts {
			addACC(tx.Accounts[i])
		}
	}
	//Allow subscriptions based on note prefix (up to 32 chars in base64)
	t["NOTE:"+getNotePrefix(txw, 32)] = struct{}{}
	if tx.Group != (types.Digest{}) {
		t["GRP:"+base64.StdEncoding.EncodeToString(tx.Group[:])] = struct{}{}
	}

	topics := make([]string, len(t))
	for k := range t {
		if len(k) > 0 {
			topics = append(topics, k)
		}
	}

	return topics
}
func getNotePrefix(txw *TxWrap, l int) string {
	nb64 := base64.StdEncoding.EncodeToString(txw.Txn.Txn.Note)
	if l > len(nb64) {
		return nb64
	}
	return nb64[:l]
}

func genTopic(txw *TxWrap) string {
	topics := getTopics(txw)
	sort.Strings(topics)
	return fmt.Sprintf("TX:%s;%s", txw.TxId, strings.Join(topics, ";"))
}

func commitPaySet(ctx context.Context, b *algod.BlockWrap, rc *redis.Client, publish bool) {
	if len(b.Block.Payset) == 0 {
		return
	}

	pipe := rc.Pipeline()

	for i := range b.Block.Payset {
		txn := &b.Block.Payset[i]
		//I just love how easy is to get txId nowadays ;)
		txId, err := algod.DecodeTxnId(b.Block.BlockHeader, txn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Err: %s\n", err)
			continue
		}

		txw := &TxWrap{
			TxId:  txId,
			Txn:   txn,
			Round: uint64(b.Block.Round),
			Intra: i,
			Key:   fmt.Sprintf("%d-%d", uint64(b.Block.Round), i),
		}
		jTx, err := utils.EncodeJson(txw)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Err: %s\n", err)
			continue
		}

		if err := pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "xtx-v2",
			ID:     fmt.Sprintf("%d-%d", b.Block.Round, i),
			MaxLen: 200_000,
			Approx: true,
			Values: map[string]interface{}{"json": string(jTx)},
		}).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Err: %s\n", err)
		}

		if publish {
			topic := genTopic(txw)
			pipe.Publish(ctx, topic, string(jTx))
		}

	}
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Err: %s\n", err)
	}
}

func commitBlock(ctx context.Context, b *algod.BlockWrap, rc *redis.Client) bool {
	if err := rc.XAdd(ctx, &redis.XAddArgs{
		Stream: "xblock-v2",
		ID:     fmt.Sprintf("%d-0", b.Block.Round),
		MaxLen: 100_000,
		Approx: true,
		Values: map[string]interface{}{"msgpack": b.BlockRaw, "round": uint64(b.Block.Round)},
	}).Err(); err != nil {
		return false
	}
	return true
}

func handleBlockRedis(ctx context.Context, b *algod.BlockWrap, rc *redis.Client, cfg *RedisConfig, qlen int) error {
	// hash := fmt.Sprintf("b:%d", b.Block.Round/10000)
	// field := strconv.Itoa(int(b.Block.Round % 10000))

	// if err := rc.HSetNX(ctx, hash, field, b.BlockRaw).Err(); err != nil {
	// 	fmt.Printf("SET ERR %d: %s \n", uint64(b.Block.Round), err)
	// 	return err
	// }

	start := time.Now()

	//Try to commit new block
	//If successful that we should broadcast to pub/sub
	publish := commitBlock(ctx, b, rc)
	commitPaySet(ctx, b, rc, publish)

	fmt.Fprintf(os.Stderr, "[Redis] Block %d processed in %s. QLen:%d\n", uint64(b.Block.Round), time.Since(start), qlen)
	return nil
}
