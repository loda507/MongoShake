package conf

import (
	"fmt"
	"strings"
)

type Configuration struct {
	Tunnel        string `config:"tunnel"`
	TunnelAddress string `config:"tunnel.address"`
	SystemProfile int    `config:"system_profile"`
	LogLevel      string `config:"log_level"`
	LogFileName   string `config:"log_file"`
	LogBuffer     bool   `config:"log_buffer"`
	ReplayerNum   int    `config:"replayer"`
	Target        string `config:"target.address"`
}

type KafkaMsg struct {
	Timestamp     int64    					`json:"ts"`
	Operation     string   					`json:"op"`
	Namespace     string   					`json:"ns"`
	Object        map[string]interface{}    `json:"o"`
	Query         map[string]interface{}    `json:"o2"`
}

var Options Configuration


// parse the address (topic@broker1,broker2,...)
func ParseTarget(address string) (string, []string, error) {
	arr := strings.Split(address, "@")
	l := len(arr)
	if l == 0 || l > 2 {
		return "", nil, fmt.Errorf("address format error")
	}

	topic := "main-oplog"
	if l == 2 {
		topic = arr[0]
	}

	brokers := strings.Split(arr[l - 1], ",")
	return topic, brokers, nil
}