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
	"os"
	"time"

	"github.com/algonode/algostreamer/internal/utils"
	"github.com/algorand/go-algorand-sdk/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/types"
)

type AlgoConfig struct {
	Address string `json:"address"`
	Token   string `json:"token"`
	Queue   int    `json:"queue"`
	FRound  int64  `json:"-"`
}

type Status struct {
	LastRound uint64
	LagMs     int64
}

func AlgodStream(ctx context.Context, cfg *AlgoConfig) (chan *types.Block, chan *Status, error) {

	// Create an algod client
	algodClient, err := algod.MakeClient(cfg.Address, cfg.Token)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to make algod client: %s\n", err)
		return nil, nil, err
	}
	fmt.Fprintf(os.Stderr, "Algo client: %s\n", cfg.Address)

	qDepth := cfg.Queue
	if qDepth < 1 {
		qDepth = 100
	}
	bchan := make(chan *types.Block, qDepth)
	schan := make(chan *Status, qDepth)

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

	fmt.Fprintf(os.Stderr, "algod last round: %d\n", nodeStatus.LastRound)
	var nextRound uint64 = 0
	if cfg.FRound < 0 {
		nextRound = nodeStatus.LastRound
	} else {
		nextRound = uint64(cfg.FRound)
	}

	//Loop until Algoverse gets canelled
	go func() {
		for {
			for ; nextRound <= nodeStatus.LastRound; nextRound++ {
				err := utils.Backoff(ctx, func(actx context.Context) error {
					block, err := algodClient.Block(nextRound).Do(ctx)
					if err != nil {
						return err
					}
					fmt.Fprintf(os.Stderr, "got block %d, queue %d\n", block.Round, len(bchan))
					select {
					case bchan <- &block:
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

	return bchan, schan, nil
}
