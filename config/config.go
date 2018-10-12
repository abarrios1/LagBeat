// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import (
	"time"
)

type Config struct {
	Period time.Duration `config:"period"`
	Group string         `config:group`
	Topic string	     `config:"topic"`
	Brokers []string     `config:"brokers"`
	Zookeepers []string  `config:"zookeepers"` 
}



var DefaultConfig = Config{
	Period: 1 * time.Second,
	Group: "",
	Topic: "",
	Brokers: []string{""},
	Zookeepers: []string{""},
}
