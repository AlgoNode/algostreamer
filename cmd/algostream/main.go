package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	//load config
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %s", err)
		return
	}

	//make us a nice cancellable context
	//set Ctrl-C as the cancell trigger
	ctx, cf := context.WithCancel(context.Background())
	defer cf()
	{
		cancelCh := make(chan os.Signal, 1)
		signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT)
		go func() {
			<-cancelCh
			fmt.Fprintf(os.Stderr, "Stopping streamer.\n")
			cf()
		}()
	}

	//spawn a block stream fetcher that never fails
	blocks, err := algodStream(ctx, &cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting algod stream: %s", err)
		return
	}

	//spawn a redis pusher
	err = redisPusher(ctx, &cfg, blocks)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error setting up redis: %s", err)
		return
	}

	//Wait for the end of the Algoverse
	<-ctx.Done()

}
