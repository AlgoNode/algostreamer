package main

import (
	"context"
	"fmt"
	"time"

	"github.com/algorand/go-algorand-sdk/client/v2/algod"

	"flag"

	"github.com/algorand/go-algorand/util/codecs"
)

var cfgFile = flag.String("f", "config.json", "config file")

// ConfigFilename is the name of algoh's config file
const ConfigFilename = "host-config.json"

// HostConfig is algoh's configuration structure
type SteramerConfig struct {
	AlgodAddress string `json:"algod_address"`
	AlgodToken   string `json:"algod_token"`
}

var defaultConfig = SteramerConfig{
	AlgodAddress: "http://localhost:8081",
	AlgodToken:   "",
}

// LoadConfigFromFile loads the configuration from the specified file, merging into the default configuration.
func LoadConfigFromFile(file string) (cfg SteramerConfig, err error) {
	cfg = defaultConfig
	err = codecs.LoadObjectFromFile(file, &cfg)
	return cfg, err
}

func main() {

	flag.Parse()
	cfg, err := LoadConfigFromFile(*cfgFile)
	if err != nil {
		fmt.Printf("failed to parse config file %s : %s\n", *cfgFile, err)
	}

	// Create an algod client
	algodClient, err := algod.MakeClient(cfg.AlgodAddress, cfg.AlgodToken)
	if err != nil {
		fmt.Printf("failed to make algod client: %s\n", err)
		return
	}

	// Print algod status
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2)*time.Second)
	nodeStatus, err := algodClient.Status().Do(ctx)
	cancel()
	if err != nil {
		fmt.Printf("error getting algod status: %s\n", err)
		return
	}
	fmt.Printf("algod last round: %d\n", nodeStatus.LastRound)

	for {

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(15)*time.Second)
		newStatus, err := algodClient.StatusAfterBlock(nodeStatus.LastRound).Do(ctx)
		cancel()
		if err != nil {
			fmt.Printf("error getting algod status: %s\n", err)
			time.Sleep(time.Duration(1000) * time.Millisecond)
			continue
		}
		nodeStatus = newStatus
		fmt.Printf("algod last round: %d, lag: %s\n", nodeStatus.LastRound, time.Duration(nodeStatus.TimeSinceLastRound)*time.Nanosecond)

		bctx, cancel := context.WithTimeout(context.Background(), time.Duration(15)*time.Second)
		block, err := algodClient.Block(nodeStatus.LastRound).Do(bctx)
		cancel()
		if err != nil {
			fmt.Printf("error getting algod status: %s\n", err)
			time.Sleep(time.Duration(1000) * time.Millisecond)
			continue
		}
		fmt.Printf("%v\n", block)
	}
}
