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

package mqtt

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/algonode/algostreamer/internal/algod"
	"github.com/algonode/algostreamer/internal/utils"
)

func MQTTPusher(ctx context.Context, cfg *MQTTConfig, blocks chan *algod.BlockWrap, status chan *algod.Status) error {
	client, err := startClient(cfg)
	if err != nil {
		return err
	}

	fmt.Printf("[INFO][MQTT] Attempting to connect to the broker\n")
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Fprintf(os.Stderr, "[!ERR][MQTT] Connect error: %s\n", token.Error())
		return token.Error()
	}

	// Publish messages until we receive a signal
	publisherDone := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for {
			select {
			case s := <-status:
				if s.LastCP != "" {
					fmt.Printf("[INFO][MQTT] ns: %s, round: %d, lag: %d, lcp: %s\n", s.NodeId, uint64(s.LastRound), s.LagMs, s.LastCP)
				}
			case b := <-blocks:
				start := time.Now()
				p := "-"
				jBlock, err := utils.EncodeJson(b.Block)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[!ERR][MQTT] Error encoding block to json: %s\n", err)
					return
				}
				cfg.Payload = string(jBlock)
				token := client.Publish(cfg.Topic, byte(cfg.Qos), cfg.Retained, cfg.Payload)
				// Handle the token in a go routine so this loop keeps sending messages regardless of delivery status
				go func() {
					if token.Wait() && token.Error() != nil {
						fmt.Printf("[!ERR][MQTT] ERROR PUBLISHING: %s\n", err)
					} else {
						p = "+"
					}
					fmt.Fprintf(os.Stderr,
						"[INFO][MQTT] Block %d@%s processed(%s) in %s (%d txn).\n",
						uint64(b.Block.Round),
						time.Unix(b.Block.TimeStamp, 0).UTC().Format(time.RFC3339),
						p,
						time.Since(start),
						len(b.Block.Payset),
					)
				}()
			case <-publisherDone:
				fmt.Println("[INFO][MQTT] publisher done")
				wg.Done()
				return
			}
		}
	}()

	<-ctx.Done()
	close(publisherDone)
	wg.Wait()
	client.Disconnect(250)
	if !client.IsConnectionOpen() {
		fmt.Println("[INFO][MQTT] publisher shutdown complete")
	}
	return nil
}
