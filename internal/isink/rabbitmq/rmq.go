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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	"github.com/algonode/algostreamer/internal/utils"

	"github.com/sirupsen/logrus"

	"github.com/rabbitmq/amqp091-go"
	amqp91 "github.com/rabbitmq/amqp091-go"
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
	tx     *Channel
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
				} else {
					atomic.AddInt32(&fail, 1)
					log.Warnf("NOT Confirmed %d messages", atomic.LoadInt32(&fail))
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
		handlePublishConfirm, log)

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
		handlePublishConfirm, log)

	if err != nil {
		return nil, fmt.Errorf("error creating stream producer : %s : %v", rs.cfg.BlockStr, err)
	}

	config := amqp091.Config{
		Properties: amqp091.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName(rs.cfg.ClientName)

	conn, err := DialConfigCluster(rs.cfg.Cluster, config, log)

	if err != nil {
		return nil, fmt.Errorf("error dialing AMQP91 cluster : %v", err)
	}

	if rs.tx, err = conn.Channel(); err != nil {
		return nil, fmt.Errorf("error creating AMQP91 channel : %v", err)
	}

	if err := rs.tx.ExchangeDeclare(rs.cfg.TxStr, "headers", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("error declaring AMQP91 headers exchange : %v", err)
	}

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

func appendHeaders(tx *utils.Transaction, t amqp91.Table) {

	addACC := func(a *string) {
		if a == nil || *a == "" {
			return
		}
		t["ACC:"+*a] = 1
	}

	addASA := func(a *uint64) {
		if a == nil || *a == 0 {
			return
		}
		t[fmt.Sprintf("ASA:%d", a)] = 1
	}

	addAPP := func(a uint64) {
		if a == 0 {
			return
		}
		t[fmt.Sprintf("APP:%d", a)] = 1
	}

	addACC(&tx.Sender)
	addACC(tx.AuthAddr)
	switch tx.TxType {
	case "pay":
		addACC(&tx.Sender)
		addACC(&tx.PaymentTransaction.Receiver)
		addACC(tx.PaymentTransaction.CloseRemainderTo)
	case "axfer":
		addACC(&tx.Sender)
		addACC(&tx.AssetTransferTransaction.Receiver)
		addACC(tx.AssetTransferTransaction.CloseTo)
		addASA(&tx.AssetTransferTransaction.AssetId)
	case "acfg":
		addACC(tx.AssetConfigTransaction.Params.Manager)
		addACC(tx.AssetConfigTransaction.Params.Reserve)
		addACC(tx.AssetConfigTransaction.Params.Clawback)
		addACC(tx.AssetConfigTransaction.Params.Freeze)
		addASA(tx.AssetConfigTransaction.AssetId)
	case "afrz":
		addACC(&tx.AssetFreezeTransaction.Address)
		addASA(&tx.AssetFreezeTransaction.AssetId)
	case "appl":
		addAPP(tx.ApplicationTransaction.ApplicationId)
		if tx.ApplicationTransaction.ForeignApps != nil {
			for _, app := range *tx.ApplicationTransaction.ForeignApps {
				addAPP(app)
			}
		}
		if tx.ApplicationTransaction.ForeignAssets != nil {
			for _, asa := range *tx.ApplicationTransaction.ForeignAssets {
				addASA(&asa)
			}
		}
		if tx.ApplicationTransaction.Accounts != nil {
			for _, addr := range *tx.ApplicationTransaction.Accounts {
				addACC(&addr)
			}
		}
	}
	//Allow subscriptions based on note prefix (up to 32 chars in base64)
	appendNotePrefix(tx, 6, t)
	appendNotePrefix(tx, 8, t)
	appendNotePrefix(tx, 10, t)
}

func appendNotePrefix(tx *utils.Transaction, l int, t amqp91.Table) {
	if tx.Note == nil || len(*tx.Note) == 0 {
		return
	}
	len := len(*tx.Note)
	if len > l {
		len = l
	}
	key := fmt.Sprintf("Note-%d", l)
	nb64 := base64.StdEncoding.EncodeToString((*tx.Note)[:len])
	t[key] = nb64
}

func (sink *RmqSink) commitPaySet(ctx context.Context, b *isink.BlockWrap) {
	if b.BlockResponse.Transactions == nil || len(*b.BlockResponse.Transactions) == 0 {
		return
	}

	for i := range *b.BlockResponse.Transactions {
		txn := (*b.BlockResponse.Transactions)[i]

		jTx, err := utils.EncodeJson(txn)
		if err != nil {
			continue
		}

		hdrs := amqp91.Table{
			"round":        int64(b.Block.BlockHeader.Round),
			"txid":         *txn.Id,
			"intra":        i,
			"type":         txn.TxType,
			"publishingId": int64(b.Block.BlockHeader.Round)*100000 + int64(i),
		}

		appendHeaders(&txn, hdrs)

		msg := amqp91.Publishing{
			ContentType: "application/json",
			Headers:     hdrs,
			Body:        jTx,
		}

		err = sink.tx.ReliablePublishWithContext(ctx, sink.cfg.TxStr, "", false, false, msg)

		if err != nil {
			fmt.Fprintf(os.Stderr, "[!ERR][RabbitMQ] %s", err)
			continue
		}

	}
}

func (sink *RmqSink) commitBlock(ctx context.Context, b *isink.BlockWrap) (first bool) {

	msg := amqp.NewMessage([]byte(b.BlockJsonIDX))
	msg.SetPublishingId(int64(b.Block.BlockHeader.Round))
	if err := sink.block.Send(msg); err != nil {
		sink.Log.WithError(err).Error("Error marshaling status update")
		return false
	}
	//sink.Log.Debugf("Sent block %d, len %dB, id:%d", b.Block.Round(), len(jBlock), msg.GetPublishingId())

	return true
}

func (sink *RmqSink) handleBlockRmq(ctx context.Context, b *isink.BlockWrap) error {
	start := time.Now()

	// Try to commit new block
	// If successful than we should broadcast to pub/sub
	publish := sink.commitBlock(ctx, b)
	sink.commitPaySet(ctx, b)

	p := "-"
	if publish {
		p = "+"
	}

	sink.Log.Infof("Block %d@%s processed(%s) in %s (%d txn). QLen:%d", uint64(b.Block.BlockHeader.Round), time.Unix(b.Block.TimeStamp, 0).UTC().Format(time.RFC3339), p, time.Since(start), len(b.Block.Payset), len(sink.Blocks))
	return nil
}
