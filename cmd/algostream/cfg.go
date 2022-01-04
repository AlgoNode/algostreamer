package main

import (
	"flag"

	"github.com/algorand/go-algorand/util/codecs"
)

var cfgFile = flag.String("f", "config.json", "config file")
var firstRound = flag.Int64("r", -1, "first round to start [-1 = latest]")

// ConfigFilename is the name of algoh's config file
const ConfigFilename = "host-config.json"

// HostConfig is algoh's configuration structure
type SteramerConfig struct {
	Algod *AlgoConfig  `json:"alogd"`
	Redis *RedisConfig `json:"redis"`
}

var gConfig = SteramerConfig{
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
}

// loadConfig loads the configuration from the specified file, merging into the default configuration.
func loadConfig() (cfg *SteramerConfig, err error) {
	flag.Parse()
	cfg = &gConfig
	err = codecs.LoadObjectFromFile(*cfgFile, cfg)
	return cfg, err
}
