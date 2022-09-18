// Copyright (C) 2022 AlgoNode Org.
//
// algostreamer is free software: you can rmqtribute it and/or modify
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

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	"github.com/algonode/algostreamer/internal/utils"
	"github.com/sirupsen/logrus"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type RmqConfig struct {
	ClusterStr []string `json:"stream-cluster"`
	Cluster    []string `json:"cluster"`
	ClientName string   `json:"client"`
	BlockStr   string   `json:"block-str"`
	TxStr      string   `json:"tx-hdr"`
	StatusStr  string   `json:"status-str"`
}

type RmqSink struct {
	isink.SinkCommon
	cfg    RmqConfig
	env    *stream.Environment
	status *ReliableProducer
	block  *ReliableProducer
}

var counter int32 = 0
var fail int32 = 0

func Make(ctx context.Context, cfg *config.SinkDef, log *logrus.Logger) (isink.Sink, error) {

	var rs = &RmqSink{}

	handlePublishConfirm := func(messageStatus []*stream.ConfirmationStatus) {
		go func() {
			for _, message := range messageStatus {
				if message.IsConfirmed() {

					atomic.AddInt32(&counter, 1)
					log.Debugf("Confirmed %d messages, pid: %d", atomic.LoadInt32(&counter), message.GetPublishingId())
				} else {
					atomic.AddInt32(&fail, 1)
					log.Debugf("NOT Confirmed %d messages", atomic.LoadInt32(&fail))
				}

			}
		}()
	}

	if cfg == nil {
		return nil, errors.New("rabbitmq config is missing")
	}

	if err := json.Unmarshal(cfg.Cfg, &rs.cfg); err != nil {
		return nil, fmt.Errorf("rmq config parsing error: %v", err)
	}

	rs.MakeDefault(log, cfg.Name)

	var err error
	rs.env, err = stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUris(rs.cfg.ClusterStr))

	if err != nil {
		return nil, err
	}

	if err = rs.env.DeclareStream(rs.cfg.StatusStr,
		stream.NewStreamOptions().SetMaxLengthBytes(stream.ByteCapacity{}.KB(20)).SetMaxSegmentSizeBytes(stream.ByteCapacity{}.KB(10))); err != nil {
		return nil, fmt.Errorf("error declaring stream: %s : %v", rs.cfg.StatusStr, err)
	}

	rs.status, err = NewHAProducer(
		rs.env,
		rs.cfg.StatusStr,
		stream.NewProducerOptions().
			SetProducerName(rs.cfg.ClientName),
		handlePublishConfirm)

	if err != nil {
		return nil, fmt.Errorf("error creating stream producer : %s : %v", rs.cfg.StatusStr, err)
	}

	if err = rs.env.DeclareStream(rs.cfg.BlockStr,
		stream.NewStreamOptions().SetMaxAge(time.Hour*24*14)); err != nil {
		return nil, fmt.Errorf("error declaring stream: %s : %v", rs.cfg.BlockStr, err)
	}

	rs.block, err = NewHAProducer(
		rs.env,
		rs.cfg.BlockStr,
		stream.NewProducerOptions().
			SetProducerName(rs.cfg.ClientName),
		handlePublishConfirm)

	if err != nil {
		return nil, fmt.Errorf("error creating stream producer : %s : %v", rs.cfg.BlockStr, err)
	}

	// rs.tx, err = NewHAProducer(
	// 	rs.env,
	// 	rs.cfg.TxStr,
	// 	stream.NewProducerOptions().
	// 		SetProducerName(rs.cfg.ClientName).
	// 		SetCompression(stream.Compression{}.Lz4()),
	// 	handlePublishConfirm)

	// if err != nil {
	// 	return nil, err
	// }

	return rs, err

}

func (sink *RmqSink) Start(ctx context.Context) error {
	go func() {
		for {
			select {
			case s := <-sink.Status:
				for {
					err := sink.handleStatusUpdate(ctx, s)
					if err == nil {
						break
					}
					time.Sleep(time.Second)
				}
			case b := <-sink.Blocks:
				for {
					err := sink.handleBlockRmq(ctx, b)
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

func (sink *RmqSink) GetLastBlock(ctx context.Context) (uint64, error) {

	last, err := sink.block.GetLastPublishingId()
	if err != nil {
		return 0, err
	}

	return uint64(last), nil
}

func (sink *RmqSink) handleStatusUpdate(ctx context.Context, status *isink.Status) error {

	if sink.status == nil {
		err := fmt.Errorf("Status sink not ready")
		sink.Log.WithError(err).Error()
		return err
	}

	jmsg, err := json.Marshal(*status)
	if err != nil {
		sink.Log.WithError(err).Error("Error marshaling status update")
		return err
	}

	msg := amqp.NewMessage(jmsg)
	msg.SetPublishingId(int64(status.LastRound))
	if err := sink.status.Send(msg); err != nil {
		sink.Log.WithError(err).Error("Error marshaling status update")
	}

	return nil
}

/*
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

/*
func commitPaySet(ctx context.Context, b *isink.BlockWrap, rc *rmq.Client, publish bool) {
	if len(b.Block.Payset) == 0 {
		return
	}

	pipe := rc.Pipeline()

	for i := range b.Block.Payset {
		txn := &b.Block.Payset[i]
		//I just love how easy is to get txId nowadays ;)
		txId, err := algod.DecodeTxnId(b.Block.BlockHeader, txn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[!ERR][RabbitMQ] %s", err)
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
			fmt.Fprintf(os.Stderr, "[!ERR][RabbitMQ] %s", err)
			continue
		}

		if err := pipe.XAdd(ctx, &rmq.XAddArgs{
			Stream: "xtx-v2",
			ID:     fmt.Sprintf("%d-%d", b.Block.Round, i),
			MaxLen: MAX_TXN,
			Approx: true,
			Values: map[string]interface{}{"json": string(jTx)},
		}).Err(); err != nil {
			if !strings.HasPrefix(err.Error(), "ERR The ID specified in XADD") {
				fmt.Fprintf(os.Stderr, "[!ERR][RabbitMQ] %s", err)
			}
		}

		if publish {
			topic := genTopic(txw)
			pipe.Publish(ctx, topic, string(jTx))
		}

	}
	if _, err := pipe.Exec(ctx); err != nil {
		if !strings.HasPrefix(err.Error(), "ERR The ID specified in XADD") {
			fmt.Fprintf(os.Stderr, "[!ERR][RabbitMQ] %s", err)
		}
	}
}
*/

/*
func updateStats(ctx context.Context, b *isink.BlockWrap, rc *rmq.Client) {
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
		fmt.Fprintf(os.Stderr, "[!ERR][RabbitMQ] %s", err)
	}
}
*/

func (sink *RmqSink) commitBlock(ctx context.Context, b *isink.BlockWrap) (first bool) {

	jBlock, err := utils.EncodeJson(b.Block)
	if err != nil {
		sink.Log.WithError(err).Error("Error encoding block to json")
	} else {
		msg := amqp.NewMessage(jBlock)
		msg.SetPublishingId(int64(b.Block.Round()))
		if err := sink.block.Send(msg); err != nil {
			sink.Log.WithError(err).Error("Error marshaling status update")
			return false
		}
		//sink.Log.Debugf("Sent block %d, len %dB, id:%d", b.Block.Round(), len(jBlock), msg.GetPublishingId())

	}

	return true
}

func (sink *RmqSink) handleBlockRmq(ctx context.Context, b *isink.BlockWrap) error {
	start := time.Now()

	// Try to commit new block
	// If successful than we should broadcast to pub/sub
	publish := sink.commitBlock(ctx, b)
	// if publish {
	// 	go func() {
	// 		updateStats(ctx, b, sink.rc)
	// 	}()
	// }
	// commitPaySet(ctx, b, rc, publish)

	p := "-"
	if publish {
		p = "+"
	}

	sink.Log.Infof("Block %d@%s processed(%s) in %s (%d txn). QLen:%d", uint64(b.Block.BlockHeader.Round), time.Unix(b.Block.TimeStamp, 0).UTC().Format(time.RFC3339), p, time.Since(start), len(b.Block.Payset), len(sink.Blocks))
	return nil
}
