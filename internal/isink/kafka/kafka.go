// Copyright (C) 2022 AlgoNode Org.
//
// algostreamer is free software: you can kafkatribute it and/or modify
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

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/gzip"
	"github.com/sirupsen/logrus"
)

type KafkaConfig struct {
	Addr        string `json:"addr"`
	Username    string `json:"user"`
	Password    string `json:"pass"`
	Compression bool   `json:"compression"`
	Client      string `json:"client"`
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	Src         string `json:"src"`
}

type KafkaSink struct {
	isink.SinkCommon
	cfg KafkaConfig
	kc  *kafka.Writer
	cc  kafka.CompressionCodec
}

func Make(ctx context.Context, cfg *config.SinkDef, log *logrus.Logger) (isink.Sink, error) {

	var ks = &KafkaSink{}

	if cfg == nil {
		return nil, errors.New("kafka config is missing")
	}

	if err := json.Unmarshal(cfg.Cfg, &ks.cfg); err != nil {
		return nil, fmt.Errorf("kafka config parsing error: %v", err)
	}

	ks.MakeDefault(log, cfg.Name)

	ks.kc = &kafka.Writer{
		Addr:                   kafka.TCP(ks.cfg.Addr),
		ReadTimeout:            10 * time.Second,
		WriteTimeout:           10 * time.Second,
		MaxAttempts:            1000000,
		BatchSize:              1,
		Compression:            kafka.Lz4,
		ErrorLogger:            ks.Log,
		AllowAutoTopicCreation: true,
	}

	// if ks.cfg.Username != "" {
	// 	mechanism, err := scram.Mechanism(scram.SHA512, ks.cfg.Username, ks.cfg.Password)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	dialer.SASLMechanism = mechanism
	// }

	if ks.cfg.Compression {
		ks.cc = gzip.NewCompressionCodec()
	}

	// kc, err := dialer.DialContext(ctx, "tcp", ks.cfg.Addr)

	// if err != nil {
	// 	ks.Log.WithError(err).Panic()
	// 	return nil, err
	// }
	// ks.kc = kc

	return ks, nil

}

func (sink *KafkaSink) Start(ctx context.Context) error {
	go func() {
		defer sink.kc.Close()
		for {
			select {
			case <-sink.Status:
				//ignore status updates for now
			case b := <-sink.Blocks:
				for {
					err := sink.handleBlockkafka(ctx, b)
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

func (sink *KafkaSink) GetLastBlock(ctx context.Context) (uint64, error) {
	return 0, errors.New("TODO")
}

func (sink *KafkaSink) handleBlockkafka(ctx context.Context, b *isink.BlockWrap) error {
	start := time.Now()
	sink.Log.Infof("Block %d@%s, %d txn, size: %dkB QLen:%d", uint64(b.Block.BlockHeader.Round), time.Unix(b.Block.TimeStamp, 0).UTC().Format(time.RFC3339), len(b.Block.Payset), len(b.BlockJsonIDX)/1024, (sink.Blocks))
	//sink.kc.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err := sink.kc.WriteMessages(ctx, kafka.Message{
		Partition: sink.cfg.Partition,
		Topic:     sink.cfg.Topic,
		Key:       []byte(fmt.Sprintf("%d", b.Block.BlockHeader.Round)),
		Value:     []byte(b.BlockJsonIDX)})
	if err != nil {
		sink.Log.WithError(err).Error("writing message")
		return err
	}

	p := "+"
	sink.Log.Infof("Block %d@%s processed(%s) in %s (%d txn). QLen:%d", uint64(b.Block.BlockHeader.Round), time.Unix(b.Block.TimeStamp, 0).UTC().Format(time.RFC3339), p, time.Since(start), len(b.Block.Payset), len(sink.Blocks))
	return nil
}
