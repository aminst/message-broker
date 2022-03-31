package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type ConnectionDetails struct {
	Host string
	Port int
}

type Config struct {
	RPCService    ConnectionDetails
	PubSubService ConnectionDetails
}

func readConfig() Config {
	f, err := os.Open("../config.json")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	byteValue, _ := ioutil.ReadAll(f)
	var config Config
	json.Unmarshal(byteValue, &config)
	return config
}

func GetRPCHost() string {
	return readConfig().RPCService.Host
}

func GetRPCPort() int {
	return readConfig().RPCService.Port
}

func GetPubSubHost() string {
	return readConfig().PubSubService.Host
}

func GetPubSubPort() int {
	return readConfig().PubSubService.Port
}
