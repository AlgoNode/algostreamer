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
	"flag"

	"github.com/algorand/go-algorand/util/codecs"
)

var cfgFile = flag.String("f", "config.json", "config file")
var firstRound = flag.Int64("r", -1, "first round to start [-1 = latest]")
var stdoutFlag = flag.Bool("s", false, "dump blocks to stdout instead of redis")

// ConfigFilename is the name of algoh's config file
const ConfigFilename = "host-config.json"

// HostConfig is algoh's configuration structure
type SteramerConfig struct {
	Algod  *AlgoConfig  `json:"algod"`
	Redis  *RedisConfig `json:"redis"`
	stdout bool
}

var defaultConfig = SteramerConfig{
	Algod: &AlgoConfig{
		Address: "http://localhost:8081",
		Queue:   100,
	},
	Redis: &RedisConfig{
		Addr:     "127.0.0.1",
		Username: "",
		Password: "",
		DB:       0,
	},
	stdout: false,
}

// loadConfig loads the configuration from the specified file, merging into the default configuration.
func loadConfig() (cfg SteramerConfig, err error) {
	flag.Parse()
	cfg = defaultConfig
	err = codecs.LoadObjectFromFile(*cfgFile, &cfg)
	cfg.stdout = *stdoutFlag
	return cfg, err
}
