package isink

import (
	"context"
	"errors"
	"time"

	"github.com/algorand/go-algorand-sdk/types"
	"github.com/sirupsen/logrus"
)

type Status struct {
	LastRound uint64
	LagMs     int64
	NodeId    string
	LastCP    string
}

type BlockWrap struct {
	Block         *types.Block `json: "block"`
	BlockRaw      []byte       `json:"-"`
	BlockJsonNode string
	BlockJsonIDX  string
	Src           string    `json:"src"`
	Ts            time.Time `json:"ts"`
}

type TxWrap struct {
	TxId  string                  `json:"txid"`
	Txn   *types.SignedTxnInBlock `json:"txn"`
	Round uint64                  `json:"round"`
	Intra int                     `json:"intra"`
	Key   string                  `json:"xtx-v2"`
}

type SinkCommon struct {
	Blocks chan *BlockWrap
	Status chan *Status
	Tx     chan *TxWrap
	Log    *logrus.Entry
}

type Sink interface {
	Start(context.Context) error
	MakeDefault(*logrus.Logger, string)
	GetLastBlock(context.Context) (uint64, error)
	ProcessBlock(context.Context, *BlockWrap) error
	ProcessStatus(context.Context, *Status) error
	ProcessTx(context.Context, *TxWrap) error
}

func (sink *SinkCommon) MakeDefault(log *logrus.Logger, sinkName string) {
	sink.Blocks = make(chan *BlockWrap, 1000)
	sink.Status = make(chan *Status, 1000)
	sink.Tx = make(chan *TxWrap, 10000)
	sink.Log = log.WithFields(logrus.Fields{"sink": sinkName})
}

func (sink *SinkCommon) ProcessBlock(ctx context.Context, block *BlockWrap) error {
	select {
	case sink.Blocks <- block:
	case <-ctx.Done():
	}
	return nil
}

func (sink *SinkCommon) ProcessStatus(ctx context.Context, status *Status) error {
	select {
	case sink.Status <- status:
	case <-ctx.Done():
	}
	return nil
}

func (sink *SinkCommon) ProcessTx(ctx context.Context, tx *TxWrap) error {
	select {
	case sink.Tx <- tx:
	case <-ctx.Done():
	}
	return nil
}

func (sink *SinkCommon) GetLastBlock(ctx context.Context) (uint64, error) {
	return 0, errors.New("not implemented")
}
