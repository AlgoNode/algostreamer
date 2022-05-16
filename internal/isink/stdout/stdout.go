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

package stdout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/algonode/algostreamer/internal/config"
	"github.com/algonode/algostreamer/internal/isink"
	"github.com/algorand/go-algorand/protocol"
	"github.com/sirupsen/logrus"

	"github.com/algorand/go-codec/codec"
)

type StdoutConfig struct {
}

func handleBlockStdOut(b *isink.BlockWrap) error {
	var output []byte
	enc := codec.NewEncoderBytes(&output, protocol.JSONStrictHandle)
	err := enc.Encode(b)
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func (sink *StdoutSink) Start(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-sink.Status:
				//noop
			case b := <-sink.Blocks:
				handleBlockStdOut(b)
			case <-ctx.Done():
			}

		}
	}()
	return nil
}

type StdoutSink struct {
	isink.SinkCommon
	cfg StdoutConfig
}

func Make(ctx context.Context, cfg *config.SinkDef, log *logrus.Logger) (isink.Sink, error) {
	var ss = &StdoutSink{}

	if cfg == nil {
		return nil, errors.New("stdout config is missing")
	}

	if err := json.Unmarshal(cfg.Cfg, &ss.cfg); err != nil {
		return nil, fmt.Errorf("redis config paring error: %v", err)
	}

	ss.MakeDefault(log, cfg.Name)

	return ss, nil

}
