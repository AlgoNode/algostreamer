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

package config

import (
	"flag"

	"github.com/algonode/algostreamer/internal/algod"
	"github.com/algonode/algostreamer/internal/rdb"
	"github.com/algonode/algostreamer/internal/rego"
	"github.com/algonode/algostreamer/internal/utils"
)

var cfgFile = flag.String("f", "config.jsonc", "config file")
var firstRound = flag.Int64("r", -1, "first round to start [-1 = latest]")
var stdoutFlag = flag.Bool("s", false, "dump blocks to stdout instead of redis")

type SinksCfg struct {
	Redis *rdb.RedisConfig `json:"redis"`
}

// HostConfig is algoh's configuration structure
type SteramerConfig struct {
	Algod  []*algod.AlgoConfig `json:"algod"`
	Sinks  SinksCfg            `json:"sinks"`
	Rego   *rego.OpaConfig     `json:"opa"`
	stdout bool
}

var defaultConfig = SteramerConfig{
	Algod: []*algod.AlgoConfig{
		{
			Address: "http://localhost:8081",
			Queue:   100,
		},
	},
	Sinks: SinksCfg{
		Redis: &rdb.RedisConfig{
			Addr:     "127.0.0.1",
			Username: "",
			Password: "",
			DB:       0,
		},
	},
	Rego: &rego.OpaConfig{
		MyID: "localhost",
		Rules: rego.RegoRulesMap{
			Status: "",
			Block:  "",
			Tx:     "",
		},
	},

	stdout: false,
}

// loadConfig loads the configuration from the specified file, merging into the default configuration.
func LoadConfig() (cfg SteramerConfig, err error) {
	flag.Parse()
	cfg = defaultConfig
	err = utils.LoadJSONCFromFile(*cfgFile, &cfg)
	cfg.stdout = *stdoutFlag
	//not tracking sync position per sink yet
	//start at the same block for all sinks
	for i := range cfg.Algod {
		cfg.Algod[i].FRound = *firstRound
	}
	return cfg, err
}
