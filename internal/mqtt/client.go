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
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type MQTTConfig struct {
	Enable    bool   `json:"enable"`
	Broker    string `json:"broker"`
	Port      int    `json:"port"`
	ClientId  string `json:"clientid"`
	Username  string `json:"user"`
	Password  string `json:"pass"`
	Topic     string `json:"topic"`
	Qos       int    `json:"qos"`
	Retained  bool   `json:"retained"`
	CleanSess bool   `json:"cleansess"`
	Payload   string
}

// Set some sane defaults for unspecified config
func (cfg *MQTTConfig) defaults() {
	uuid := uuid.New()

	if cfg.Broker == "" {
		cfg.Broker = "localhost"
	}
	if cfg.Port < 1 {
		cfg.Port = 1883
	}
	if cfg.ClientId != "" {
		cfg.ClientId = fmt.Sprintf("%s-%s", cfg.ClientId, uuid.String())
	} else {
		cfg.ClientId = uuid.String()
	}
	if cfg.Qos < 0 || cfg.Qos > 2 {
		fmt.Printf("[WARN][MQTT] QOS in range 0 - 2 only; defaulting to: 0\n")
		cfg.Qos = 0
	}
	// TODO: topic regex
}

func startClient(cfg *MQTTConfig) (mqtt.Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("[!ERR][MQTT] mqtt config is missing")
	}
	cfg.defaults()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", cfg.Broker, cfg.Port))
	opts.SetClientID(cfg.ClientId)
	opts.SetCleanSession(cfg.CleanSess) // true
	opts.SetUsername(cfg.Username)
	opts.SetPassword(cfg.Password)
	opts.OnConnect = func(client mqtt.Client) {
		fmt.Printf("[INFO][MQTT] Connected\n")
	}
	opts.OnConnectionLost = func(client mqtt.Client, err error) {
		fmt.Printf("[WARN][MQTT] Connection lost: %v\n", err)
	}
	opts.OnReconnecting = func(mqtt.Client, *mqtt.ClientOptions) {
		fmt.Printf("[WARN][MQTT] Attempting to reconnect\n")
	}
	opts.ConnectTimeout = time.Second // Minimal delays on connect
	opts.WriteTimeout = time.Second   // Minimal delays on writes
	opts.KeepAlive = 10               // Keepalive every 10 seconds so we quickly detect network outages
	opts.PingTimeout = time.Second    // local broker so response should be quick

	// Automate connection management (will keep trying to connect and will reconnect if network drops)
	opts.ConnectRetry = true
	opts.AutoReconnect = true

	client := mqtt.NewClient(opts)
	return client, nil
}
