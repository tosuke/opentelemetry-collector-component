package downsamplingprocessor

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

const (
	defaultInterval         = 15 * time.Second
	defaultCardinalityLimit = 8192
)

type Config struct {
	Interval         time.Duration `mapstructure:"interval"`
	CardinalityLimit uint32        `mapstructure:"cardinality_limit"`
}

var (
	_ component.Config = (*Config)(nil)
)

func createDefaultConfig() component.Config {
	return &Config{
		Interval:         defaultInterval,
		CardinalityLimit: defaultCardinalityLimit,
	}
}

func (c *Config) Validate() error {
	if c.Interval <= 0 {
		return errors.New("interval must be greater than 0")
	}
	if c.CardinalityLimit == 0 {
		return errors.New("cardinality_limit must be greater than 0")
	}
	return nil
}
