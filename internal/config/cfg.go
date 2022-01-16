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
	"fmt"

	"github.com/algonode/algostreamer/internal/algod"
	"github.com/algonode/algostreamer/internal/rdb"
	"github.com/algonode/algostreamer/internal/rego"
	"github.com/algonode/algostreamer/internal/utils"
)

var cfgFile = flag.String("f", "config.jsonc", "config file")
var firstRound = flag.Int64("r", -1, "first round to start [-1 = latest]")
var lastRound = flag.Int64("l", -1, "last round to read [-1 = no limit]")
var simpleFlag = flag.Bool("s", false, "simple mode - just sending blocks in JSON format to stdout")

type SinksCfg struct {
	Redis *rdb.RedisConfig `json:"redis"`
}

type SteramerConfig struct {
	Algod  *algod.AlgoConfig `json:"algod"`
	Sinks  SinksCfg          `json:"sinks"`
	Rego   *rego.OpaConfig   `json:"opa"`
	Stdout bool              `json:"stdout"`
}

var defaultConfig = SteramerConfig{}

// loadConfig loads the configuration from the specified file, merging into the default configuration.
func LoadConfig() (cfg SteramerConfig, err error) {
	flag.Parse()
	cfg = defaultConfig
	err = utils.LoadJSONCFromFile(*cfgFile, &cfg)

	if cfg.Algod == nil {
		return cfg, fmt.Errorf("Missing algod config")
	}
	if len(cfg.Algod.ANodes) == 0 {
		return cfg, fmt.Errorf("Configure at least one node")
	}
	cfg.Algod.FRound = *firstRound
	cfg.Algod.LRound = *lastRound
	cfg.Stdout = *simpleFlag

	return cfg, err
}
