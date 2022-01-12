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
	"time"

	"github.com/algonode/algostreamer/internal/utils"
	"github.com/algorand/go-algorand-sdk/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/types"
)

type AlgoNodeConfig struct {
	Address string `json:"address"`
	Token   string `json:"token"`
	Id      string `json:"id"`
}

type AlgoConfig struct {
	ANodes []*AlgoNodeConfig `json:"nodes"`
	Queue  int               `json:"queue"`
	FRound int64             `json:"first"`
	LRound int64             `json:"last"`
}

type Status struct {
	LastRound uint64
	LagMs     int64
}

type BlockWrap struct {
	Block *types.Block
	Src   string
	Ts    time.Time
}

func AlgoStreamer(ctx context.Context, acfg *AlgoConfig) (chan *BlockWrap, chan *Status, error) {
	qDepth := acfg.Queue
	if qDepth < 1 {
		qDepth = 100
	}
	bestbchan := make(chan *BlockWrap, qDepth)
	bchan := make(chan *BlockWrap, qDepth)
	schan := make(chan *Status, qDepth)

	for idx := range acfg.ANodes {
		if err := algodStreamNode(ctx, acfg, idx, bchan, schan, acfg.FRound, acfg.LRound); err != nil {
			return nil, nil, err
		}
	}

	// filter duplicates, forward only first newwer block.
	go func() {
		var maxBlock uint64 = math.MaxUint64

		for {
			select {
			case bw := <-bchan:
				if uint64(bw.Block.Round) > maxBlock || maxBlock == math.MaxUint64 {
					bestbchan <- bw
					maxBlock = uint64(bw.Block.Round)
				}
			case <-ctx.Done():
			}
		}
	}()

	return bestbchan, schan, nil
}

func algodStreamNode(ctx context.Context, acfg *AlgoConfig, idx int, bchan chan *BlockWrap, schan chan *Status, start int64, stop int64) error {

	cfg := acfg.ANodes[idx]
	// Create an algod client
	algodClient, err := algod.MakeClient(cfg.Address, cfg.Token)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to make algod client: %s\n", err)
		return err
	}
	fmt.Fprintf(os.Stderr, "Algo client: %s\n", cfg.Address)

	var nodeStatus *models.NodeStatus = nil
	utils.Backoff(ctx, func(actx context.Context) error {
		ns, err := algodClient.Status().Do(actx)
		if err != nil {
			return err
		}
		nodeStatus = &ns
		return nil
	}, time.Second, time.Millisecond*100, time.Second*5)
	schan <- &Status{LastRound: uint64(nodeStatus.LastRound), LagMs: int64(nodeStatus.TimeSinceLastRound) / int64(time.Millisecond)}

	var nextRound uint64 = 0
	if start < 0 {
		nextRound = nodeStatus.LastRound
		fmt.Fprintf(os.Stderr, "Starting from last round : %d\n", nodeStatus.LastRound)
	} else {
		nextRound = uint64(start)
		fmt.Fprintf(os.Stderr, "Starting from fixed round : %d\n", nextRound)
	}

	//Loop until Algoverse gets canelled
	go func() {
		for stop < 0 || nextRound < uint64(stop) {
			for ; nextRound <= nodeStatus.LastRound; nextRound++ {
				err := utils.Backoff(ctx, func(actx context.Context) error {
					block, err := algodClient.Block(nextRound).Do(ctx)
					if err != nil {
						return err
					}
					fmt.Fprintf(os.Stderr, "got block %d, queue %d\n", block.Round, len(bchan))
					select {
					case bchan <- &BlockWrap{
						Block: &block,
						Ts:    time.Now(),
						Src:   cfg.Id,
					}:
					case <-ctx.Done():
					}
					return ctx.Err()
				}, time.Second, time.Millisecond*100, time.Second*15)
				if err != nil {
					return
				}
			}

			err := utils.Backoff(ctx, func(actx context.Context) error {
				newStatus, err := algodClient.StatusAfterBlock(nodeStatus.LastRound).Do(actx)
				if err != nil {
					return err
				}
				nodeStatus = &newStatus
				fmt.Fprintf(os.Stderr, "algod last round: %d, lag: %s\n", nodeStatus.LastRound, time.Duration(nodeStatus.TimeSinceLastRound)*time.Nanosecond)
				schan <- &Status{LastRound: uint64(nodeStatus.LastRound), LagMs: int64(nodeStatus.TimeSinceLastRound) / int64(time.Millisecond)}
				return nil
			}, time.Second*10, time.Millisecond*100, time.Second*10)

			if err != nil {
				return
			}

		}
	}()

	return nil
}
