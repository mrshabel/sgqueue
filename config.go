package queue

import "time"

type Config struct {
	DatabaseURL string
	PoolSize    int
	// maximum time for when a message can stay processed
	VisibilityWindow time.Duration
}

const (
	DefaultPoolSize         = 10
	DefaultVisibilityWindow = 5 * time.Minute
)

func (c *Config) New() *Config {
	if c.PoolSize == 0 {
		c.PoolSize = DefaultPoolSize
	}
	if c.VisibilityWindow == 0 {
		c.VisibilityWindow = DefaultVisibilityWindow
	}

	return c
}
