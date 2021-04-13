package rabbitmqout

import (
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
)

type config struct {
	Codec      codec.Config `config:"codec"`
	Host       string       `config:"host"`
	Port       uint         `config:"port"`
	Username   string       `config:"username"`
	Password   string       `config:"password"`
	Vhost      string       `config:"vhost"`
	Exchange   string       `config:"exchange"`
	RoutingKey string       `config:"routing_key"`
}

var (
	defaultConfig = config{
		Host:       "de02-sevan2",
		Port:       5672,
		Username:   "elk",
		Password:   "r@bbit4elk",
		Vhost:      "elk",
		Exchange:   "elk-test",
		RoutingKey: "elk-test-key",
	}
)

func (c *config) Validate() error {
	return nil
}
