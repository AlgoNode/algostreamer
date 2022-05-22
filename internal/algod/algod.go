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

package algod

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	"github.com/algonode/algostreamer/internal/utils"
	"github.com/algorand/go-algorand-sdk/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/client/v2/common/models"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-algorand/rpcs"
	"github.com/sirupsen/logrus"
)

//globalMaxBlock holds the highest read block across all connected nodes
//writes must use atomic interface
//reads are safe as the var is 64bit aligned
var globalMaxBlock uint64 = 0

func AlgoStreamer(ctx context.Context, acfg *config.AlgoConfig, log *logrus.Logger) (chan *isink.BlockWrap, chan *isink.Status, error) {
	qDepth := acfg.Queue
	if qDepth < 1 {
		qDepth = 100
	}
	bestbchan := make(chan *isink.BlockWrap, qDepth)
	bchan := make(chan *isink.BlockWrap, qDepth)
	schan := make(chan *isink.Status, qDepth)

	for idx := range acfg.ANodes {
		if err := algodStreamNode(ctx, acfg, log, idx, bchan, schan, acfg.FRound, acfg.LRound); err != nil {
			return nil, nil, err
		}
	}

	// filter duplicates, forward only first newer blocks.
	go func() {
		var maxBlock uint64 = math.MaxUint64
		var maxTs time.Time = time.Now()
		var maxLeader string = "'"
		for {
			select {
			case bw := <-bchan:
				round := uint64(bw.Block.BlockHeader.Round)
				if round > maxBlock || maxBlock == math.MaxUint64 {
					bestbchan <- bw
					maxBlock = round
					atomic.StoreUint64(&globalMaxBlock, maxBlock)
					maxTs = bw.Ts
					maxLeader = bw.Src
				} else {
					if maxBlock == round {
						log.Infof("Block from %s is %v behind %s", bw.Src, bw.Ts.Sub(maxTs), maxLeader)
					}
				}
			case <-ctx.Done():
			}
		}
	}()

	return bestbchan, schan, nil
}

func makeBlockWrap(rawBlock []byte, src string) (*isink.BlockWrap, error) {
	block := new(rpcs.EncodedBlockCert)
	err := protocol.Decode(rawBlock, block)
	if err != nil {
		return nil, fmt.Errorf("enqueueBlock() decode err: %w", err)
	}

	jBlock, err := utils.EncodeJson(block.Block)
	if err != nil {
		return nil, err
	}

	blockIdx, err := utils.GenerateBlock(&block.Block)
	if err != nil {
		return nil, err
	}

	idxJBlock, err := utils.EncodeJson(*blockIdx)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(idxJBlock))
	os.Exit(0)

	return &isink.BlockWrap{
		Block:         &block.Block,
		BlockRaw:      rawBlock,
		BlockJsonNode: string(jBlock),
		BlockJsonIDX:  string(idxJBlock),
		Ts:            time.Now(),
		Src:           src,
	}, nil
}

func algodStreamNode(ctx context.Context, acfg *config.AlgoConfig, olog *logrus.Logger, idx int, bchan chan *isink.BlockWrap, schan chan *isink.Status, start int64, stop int64) error {

	cfg := acfg.ANodes[idx]
	log := olog.WithFields(logrus.Fields{"nodeId": cfg.Id})
	// Create an algod client
	algodClient, err := algod.MakeClient(cfg.Address, cfg.Token)
	if err != nil {
		log.WithError(err).Errorf("failed to make algod client: %s", cfg.Id)
		return err
	}
	log.Infof("new algod client: %s", cfg.Address)

	//Loop until Algoverse gets cancelled
	go func() {

		var nodeStatus *models.NodeStatus = nil
		utils.Backoff(ctx, func(actx context.Context) error {
			ns, err := algodClient.Status().Do(actx)
			if err != nil {
				return err
			}
			nodeStatus = &ns
			return nil
		}, time.Second*10, time.Millisecond*100, time.Second*10)
		if nodeStatus == nil {
			log.Error("Unable to start node")
			return
		}
		schan <- &isink.Status{NodeId: cfg.Id, LastCP: nodeStatus.LastCatchpoint, LastRound: uint64(nodeStatus.LastRound), LagMs: int64(nodeStatus.TimeSinceLastRound) / int64(time.Millisecond)}

		var nextRound uint64 = 0
		if start < 0 {
			nextRound = nodeStatus.LastRound
			log.Warnf("Starting from last round : %d", nodeStatus.LastRound)
		} else {
			nextRound = uint64(start)
			log.Warnf("Starting from fixed round : %d", nextRound)
		}

		ustop := uint64(stop)
		for stop < 0 || nextRound <= ustop {
			for ; nextRound <= nodeStatus.LastRound; nextRound++ {
				err := utils.Backoff(ctx, func(actx context.Context) error {
					gMax := globalMaxBlock
					//skip old blocks in case other nodes are ahead of us
					if gMax > nextRound {
						log.Warnf("skipping ahead %d blocks to %d", gMax-nextRound, gMax)
						nextRound = globalMaxBlock
					}
					rawBlock, err := algodClient.BlockRaw(nextRound).Do(ctx)
					if err != nil {
						return err
					}
					bw, err := makeBlockWrap(rawBlock, cfg.Id)
					if err != nil {
						return err
					}
					//log.Infof("got block %d, queue %d", block.Round, len(bchan))
					select {
					case bchan <- bw:
					case <-ctx.Done():
					}
					return ctx.Err()
				}, time.Second*10, time.Millisecond*100, time.Second*10)
				if err != nil || nextRound >= ustop {
					return
				}
			}

			err := utils.Backoff(ctx, func(actx context.Context) error {
				newStatus, err := algodClient.StatusAfterBlock(nodeStatus.LastRound).Do(actx)
				if err != nil {
					return err
				}
				nodeStatus = &newStatus
				//log.Infof("algod last round: %d, lag: %s", nodeStatus.LastRound, time.Duration(nodeStatus.TimeSinceLastRound)*time.Nanosecond)
				schan <- &isink.Status{NodeId: cfg.Id, LastRound: uint64(nodeStatus.LastRound), LagMs: int64(nodeStatus.TimeSinceLastRound) / int64(time.Millisecond)}
				return nil
			}, time.Second*10, time.Millisecond*100, time.Second*10)

			if err != nil {
				return
			}

		}
	}()

	return nil
}
