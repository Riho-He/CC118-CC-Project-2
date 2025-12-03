package pkg

import (
	"fmt"
	"os"
	"time"
)

// Config defines the various settings for a Raft cluster.
type Config struct {
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	ClusterSize      int
	NodeIDSize       int
	LogPath          string
	InMemory         bool
}

// DefaultConfig returns the default config for a Raft cluster. By default
// config uses in-memory storage
func DefaultConfig() Config {
	return Config{
		ClusterSize:      3,
		ElectionTimeout:  time.Millisecond * 150,
		HeartbeatTimeout: time.Millisecond * 50,
		NodeIDSize:       2,
		LogPath:          os.TempDir(),
		InMemory:         true,
	}
}

// CheckConfig checks if a provided Raft config is valid.
func CheckConfig(config Config) error {
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("Heartbeat timeout is too low")
	}

	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("Election timeout is too low")
	}

	if config.ElectionTimeout < config.HeartbeatTimeout {
		return fmt.Errorf("The election timeout (%v) is less than the heartbeat timeout (%v)", config.ElectionTimeout, config.HeartbeatTimeout)
	}

	if config.ClusterSize <= 0 {
		return fmt.Errorf("The cluster size must be positive")
	}

	if config.NodeIDSize <= 0 {
		return fmt.Errorf("The node id size must be positive")
	}

	return nil
}
