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

package redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/algonode/algostreamer/internal/algod"
	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	"github.com/algonode/algostreamer/internal/utils"
	"github.com/algorand/go-algorand-sdk/types"
	"github.com/sirupsen/logrus"

	"github.com/go-redis/redis/v8"
)

const (
	PFX_Node   = "nd:"
	MAX_Blocks = 320
	MAX_TXN    = 0
)

type RedisConfig struct {
	Addr     string `json:"addr"`
	Username string `json:"user"`
	Password string `json:"pass"`
	DB       int    `json:"db"`
}

type RedisSink struct {
	isink.SinkCommon
	cfg RedisConfig
	rc  *redis.Client
}

func Make(ctx context.Context, cfg *config.SinkDef, log *logrus.Logger) (isink.Sink, error) {

	var rs = &RedisSink{}

	if cfg == nil {
		return nil, errors.New("redis config is missing")
	}

	if err := json.Unmarshal(cfg.Cfg, &rs.cfg); err != nil {
		return nil, fmt.Errorf("redis config parsing error: %v", err)
	}

	rs.MakeDefault(log, cfg.Name)

	rs.rc = redis.NewClient(&redis.Options{
		Addr:       rs.cfg.Addr,
		Password:   rs.cfg.Password,
		Username:   rs.cfg.Username,
		DB:         rs.cfg.DB,
		MaxRetries: 0,
		PoolSize:   50,
	})

	return rs, nil

}

func (sink *RedisSink) Start(ctx context.Context) error {
	go func() {
		for {
			select {
			case s := <-sink.Status:
				if sink.rc != nil {
					handleStatusUpdate(ctx, s, sink.rc, &sink.cfg)
				}
			case b := <-sink.Blocks:
				for {
					err := sink.handleBlockRedis(ctx, b)
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

func (sink *RedisSink) GetLastBlock(ctx context.Context) (uint64, error) {

	rc := redis.NewClient(&redis.Options{
		Addr:       sink.cfg.Addr,
		Password:   sink.cfg.Password,
		Username:   sink.cfg.Username,
		DB:         sink.cfg.DB,
		MaxRetries: 0,
	})

	msg, err := rc.XRevRangeN(ctx, "xblock-v2", "+", "-", 1).Result()
	if err != nil || len(msg) < 1 {
		return 0, fmt.Errorf("[REDIS] error getting last element %v ", err)
	}
	a := strings.Split(msg[0].ID, "-")
	if len(a) < 1 {
		return 0, fmt.Errorf("[REDIS] error getting last element - invalid block id %s", msg[0].ID)
	}
	r, err := strconv.ParseUint(a[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("[REDIS] error getting last element - invalid block id %s", msg[0].ID)
	}

	return r, nil
}

func handleStatusUpdate(ctx context.Context, status *isink.Status, rc *redis.Client, cfg *RedisConfig) error {
	err := rc.HSet(ctx, "NS:"+status.NodeId,
		"round", uint64(status.LastRound),
		"lag", status.LagMs,
		"lcp", status.LastCP).Err()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[!ERR][REDIS] %s", err)
		return err
	}
	if status.LastCP != "" {
		a := strings.Split(status.LastCP, "#")
		if len(a) > 0 {
			if err := rc.XAdd(ctx, &redis.XAddArgs{
				Stream: "lcp",
				ID:     fmt.Sprintf("%s-0", a[0]),
				MaxLen: 1000,
				Approx: true,
				Values: map[string]interface{}{"last": status.LastCP, "time": time.Now()},
			}).Err(); err != nil {
				if !strings.HasPrefix(err.Error(), "ERR The ID specified in XADD") {
					fmt.Fprintf(os.Stderr, "[!ERR][REDIS] %s", err)
				}
				return err
			}
		}
	}
	return nil
}

func getTopics(txw *isink.TxWrap) []string {
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
func getNotePrefix(txw *isink.TxWrap, l int) string {
	nb64 := base64.StdEncoding.EncodeToString(txw.Txn.Txn.Note)
	if l > len(nb64) {
		return nb64
	}
	return nb64[:l]
}

func genTopic(txw *isink.TxWrap) string {
	topics := getTopics(txw)
	sort.Strings(topics)
	return fmt.Sprintf("TX:%s;%s", txw.TxId, strings.Join(topics, ";"))
}

func commitPaySet(ctx context.Context, b *isink.BlockWrap, rc *redis.Client, publish bool) {
	if len(b.Block.Payset) == 0 {
		return
	}

	pipe := rc.Pipeline()

	for i := range b.Block.Payset {
		txn := &b.Block.Payset[i]
		//I just love how easy is to get txId nowadays ;)
		txId, err := algod.DecodeTxnId(b.Block.BlockHeader, txn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[!ERR][REDIS] %s", err)
			continue
		}

		txw := &isink.TxWrap{
			TxId:  txId,
			Txn:   txn,
			Round: uint64(b.Block.Round),
			Intra: i,
			Key:   fmt.Sprintf("%d-%d", uint64(b.Block.Round), i),
		}
		jTx, err := utils.EncodeJson(txw)

		if err != nil {
			fmt.Fprintf(os.Stderr, "[!ERR][REDIS] %s", err)
			continue
		}

		if err := pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "xtx-v2",
			ID:     fmt.Sprintf("%d-%d", b.Block.Round, i),
			MaxLen: MAX_TXN,
			Approx: true,
			Values: map[string]interface{}{"json": string(jTx)},
		}).Err(); err != nil {
			if !strings.HasPrefix(err.Error(), "ERR The ID specified in XADD") {
				fmt.Fprintf(os.Stderr, "[!ERR][REDIS] %s", err)
			}
		}

		if publish {
			topic := genTopic(txw)
			pipe.Publish(ctx, topic, string(jTx))
		}

	}
	if _, err := pipe.Exec(ctx); err != nil {
		if !strings.HasPrefix(err.Error(), "ERR The ID specified in XADD") {
			fmt.Fprintf(os.Stderr, "[!ERR][REDIS] %s", err)
		}
	}
}

func updateStats(ctx context.Context, b *isink.BlockWrap, rc *redis.Client) {
	if len(b.Block.Payset) == 0 {
		return
	}

	today := time.Unix(b.Block.TimeStamp, 0).UTC().Format("20060102")
	todayC := "CD:" + today
	todayV := "VD:" + today

	asaC := make(map[uint64]int64)
	asaV := make(map[uint64]float64)
	pipe := rc.Pipeline()

	for i := range b.Block.Payset {
		txn := &b.Block.Payset[i]
		//Aggregate asset tx stats
		if txn.Txn.XferAsset > 0 {
			asaC[uint64(txn.Txn.XferAsset)]++
			asaV[uint64(txn.Txn.XferAsset)] += float64(txn.Txn.AssetAmount)
		}
		if txn.Txn.ConfigAsset > 0 {
			asaC[uint64(txn.Txn.ConfigAsset)]++
		}
		if txn.Txn.FreezeAsset > 0 {
			asaC[uint64(txn.Txn.FreezeAsset)]++
		}
	}

	for k := range asaC {
		hk := fmt.Sprintf("ASA:%d", k)
		pipe.HIncrBy(ctx, hk, todayC, asaC[k])
		if v, ok := asaV[k]; ok {
			pipe.HIncrByFloat(ctx, hk, todayV, v)
		}
	}

	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[!ERR][REDIS] %s", err)
	}
}

func commitBlock(ctx context.Context, b *isink.BlockWrap, rc *redis.Client) (first bool) {
	//var wg sync.WaitGroup
	first = false

	//Enqeue MSGPACK and JSON versions in paralell.
	//Check if we are the first to enqueue

	/*
		wg.Add(1)
		go func() {
			defer wg.Done()
			jBlock, err := utils.EncodeJson(b.Block)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[!ERR][REDIS] Error encoding block to json: %s", err)
			} else {
				if err := rc.XAdd(ctx, &redis.XAddArgs{
					Stream: "xblock-v2-json",
					ID:     fmt.Sprintf("%d-0", b.Block.Round),
					MaxLen: MAX_Blocks,
					Approx: true,
					Values: map[string]interface{}{"json": string(jBlock), "round": uint64(b.Block.Round)},
				}).Err(); err != nil {
					if !strings.HasPrefix(err.Error(), "ERR The ID specified in XADD") {
						fmt.Fprintf(os.Stderr, "[!ERR][REDIS] %s", err)
					}
					//do not bail out
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
	*/
	if err := rc.XAdd(ctx, &redis.XAddArgs{
		Stream: "xblock-v2",
		ID:     fmt.Sprintf("%d-0", b.Block.Round),
		MaxLen: MAX_Blocks,
		Approx: true,
		Values: map[string]interface{}{"msgpack": b.BlockRaw, "round": uint64(b.Block.Round)},
	}).Err(); err == nil {
		//We are first to commit this block to the store
		first = true
	}
	//	}()

	//	wg.Wait()
	return first
}

func (sink *RedisSink) handleBlockRedis(ctx context.Context, b *isink.BlockWrap) error {
	start := time.Now()

	//Try to commit new block
	//If successful than we should broadcast to pub/sub
	publish := commitBlock(ctx, b, sink.rc)
	if publish {
		go func() {
			updateStats(ctx, b, sink.rc)
		}()
	}
	//commitPaySet(ctx, b, rc, publish)

	p := "-"
	if publish {
		p = "+"
	}

	sink.Log.Infof("Block %d@%s processed(%s) in %s (%d txn). QLen:%d", uint64(b.Block.Round), time.Unix(b.Block.TimeStamp, 0).UTC().Format(time.RFC3339), p, time.Since(start), len(b.Block.Payset), len(sink.Blocks))
	return nil
}
