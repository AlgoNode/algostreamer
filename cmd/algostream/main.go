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

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/algonode/algostreamer/internal/algod"
	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	_ "github.com/algonode/algostreamer/internal/isink/kafka"
	_ "github.com/algonode/algostreamer/internal/isink/redis"
	_ "github.com/algonode/algostreamer/internal/isink/stdout"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)
}

func main() {

	slog := log.StandardLogger()

	//load config
	cfg, err := config.LoadConfig()
	if err != nil {
		log.WithError(err).Error()
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
			log.Error("stopping streamer")
			cf()
		}()
	}

	sinks := make([]isink.Sink, 0)

	for name, def := range cfg.Sinks {
		def.Name = name
		if !def.Enabled {
			log.Infof("Skipping disabled sink '%s'", name)
			continue
		}
		sink, err := isink.SinkByName(ctx, def.Type, &def, slog)
		if err != nil {
			log.WithError(err).Error("Exiting")
			return
		}
		sinks = append(sinks, sink)
		if cfg.Algod.FRound < 0 {
			if lr, err := sink.GetLastBlock(ctx); err == nil {
				log.Infof("LastRound for sink '%s' is %d", name, lr)
				cfg.Algod.FRound = int64(lr)
			}
		} else {
			if lr, err := sink.GetLastBlock(ctx); err == nil {
				log.Infof("LastRound for sink '%s' is %d", name, lr)
				if int64(lr) > cfg.Algod.FRound {
					cfg.Algod.FRound = int64(lr)
				}
			}
		}
		sink.Start(ctx)
	}
	log.Infof("Resuming from round %d", cfg.Algod.FRound)

	//spawn a block stream fetcher that never fails
	blocks, status, err := algod.AlgoStreamer(ctx, cfg.Algod, slog)
	if err != nil {
		log.WithError(err).Error("Exiting")
		return
	}

TheLoop:
	for {
		select {
		case block := <-blocks:
			for _, s := range sinks {
				if err := s.ProcessBlock(ctx, block, !cfg.NoBlock); err != nil {
					log.WithError(err).Error("Exiting")
					break TheLoop
				}
			}
		case status := <-status:
			for _, s := range sinks {
				if err := s.ProcessStatus(ctx, status, !cfg.NoBlock); err != nil {
					log.WithError(err).Error("Exiting")
					break TheLoop
				}
			}
		case <-ctx.Done():
			break TheLoop
		}
	}
	log.Error("Bye!")

}
