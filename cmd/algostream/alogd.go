package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/algorand/go-algorand-sdk/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/types"
)

type AlgoConfig struct {
	Address string `json:"address"`
	Token   string `json:"token"`
	Queue   int    `json:"queue"`
}

func algodStream(ctx context.Context, cfg *SteramerConfig) (chan *types.Block, error) {

	// Create an algod client
	algodClient, err := algod.MakeClient(cfg.Algod.Address, cfg.Algod.Token)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to make algod client: %s\n", err)
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "Algo client: %s\n", cfg.Algod.Address)

	qDepth := cfg.Algod.Queue
	if qDepth < 1 {
		qDepth = 100
	}
	bchan := make(chan *types.Block, qDepth)

	var nodeStatus *models.NodeStatus = nil
	Backoff(ctx, func(actx context.Context) error {
		ns, err := algodClient.Status().Do(actx)
		if err != nil {
			return err
		}
		nodeStatus = &ns
		return nil
	}, time.Second, time.Millisecond*100, time.Second*5)

	fmt.Fprintf(os.Stderr, "algod last round: %d\n", nodeStatus.LastRound)
	var nextRound uint64 = 0
	if *firstRound < 0 {
		nextRound = nodeStatus.LastRound
	} else {
		nextRound = uint64(*firstRound)
	}

	//Loop until Algoverse gets canelled
	go func() {
		for {
			for ; nextRound <= nodeStatus.LastRound; nextRound++ {
				err := Backoff(ctx, func(actx context.Context) error {
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

			err := Backoff(ctx, func(actx context.Context) error {
				newStatus, err := algodClient.StatusAfterBlock(nodeStatus.LastRound).Do(actx)
				if err != nil {
					return err
				}
				nodeStatus = &newStatus
				fmt.Fprintf(os.Stderr, "algod last round: %d, lag: %s\n", nodeStatus.LastRound, time.Duration(nodeStatus.TimeSinceLastRound)*time.Nanosecond)
				return nil
			}, time.Second*10, time.Millisecond*100, time.Second*10)
			if err != nil {
				return
			}

		}
	}()

	return bchan, nil
}
