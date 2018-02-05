package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/sync_check/util"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("wormholeTest", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "config", "", "Config file")
	fs.IntVar(&cfg.WorkerCount, "c", 1, "parallel worker count")
	fs.IntVar(&cfg.JobCount, "n", 1, "total job count")
	fs.IntVar(&cfg.Batch, "b", 1, "insert batch commit count")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.JobType, "jobType", "", "jobType: createDataTube, loadData, check, clear")
	fs.IntVar(&cfg.Delay, "delay", 5, "check data 5 second befor")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", 1000, "diff check chunk size")
	fs.IntVar(&cfg.Sample, "sample", 100, "the percent of sampling check")
	fs.IntVar(&cfg.CheckThCount, "check-thcount", 1, "the count of check thread count")

	return cfg
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel string `toml:"log-level" json:"log-level"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`

	JobCount int `toml:"job-count" json:"job-count"`

	Batch int `toml:"batch" json:"batch"`

	SourceDBCfg util.DBConfig `toml:"source-db" json:"source-db"`

	TargetDBCfg util.DBConfig `toml:"target-db" json:"target-db"`

	ConfigFile string

	JobType string `toml:"job-type" json:"job-type"`

	Delay        int `toml:"delay" json:"delay"`
	ChunkSize    int `toml:"chunk-size" json:"chunk-size"`
	Sample       int `toml:"sample" json:"sample"`
	CheckThCount int `toml:"check-thcount" json:"check-thcount"`
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	host := os.Getenv("MYSQL_HOST")
	if host != "" {
		c.SourceDBCfg.Host = host
	}

	return nil
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
