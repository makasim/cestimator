package main

import (
	"fmt"
	"os"
	"sort"
	"time"

	"gopkg.in/yaml.v2"
)

// Config represents the cestorage configuration.
type Config struct {
	Streams []EstimatorConfig `yaml:"streams"`
}

// EstimatorConfig represents a single cardinality estimator configuration.
type EstimatorConfig struct {
	Group    []string          `yaml:"group"`    // optional: label names to split cardinality by
	Labels   map[string]string `yaml:"labels"`   // optional: extra labels added to output metrics
	Interval time.Duration     `yaml:"interval"` // optional: how often to rotate (reset) counters; 0 means no rotation
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read config file %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("cannot parse config file %q: %w", path, err)
	}
	for _, stream := range cfg.Streams {
		sort.Strings(stream.Group)
	}

	return &cfg, nil
}
