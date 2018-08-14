package checksum

import (
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/model"
)

// TableCheckCfg is the config of table to be checked. It's used for calculate_checksum program.
type TableCheckCfg struct {
	// table name
	Name string `toml:"name"`
	// Schema is seted in SourceDBCfg
	Schema string
	// field should be the primary key, unique key or field with index
	Field string `toml:"index-field"`
	// select range, for example: "age > 10 AND age < 20"
	Range string `toml:"range"`
	Info  *model.TableInfo
}

// CompareConfig is used for compare_checksum program.
type CompareConfig struct {
	From []string `toml:"from" json:"from"`
	To   []string `toml:"to" json:"to"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("checksum", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "config", "config.toml", "Config file")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file name")
	fs.IntVar(&cfg.ProfilePort, "profile-port", 0, "the port for go pprof")

	return cfg
}

type Config struct {
	*flag.FlagSet `json:"-"`

	// log level
	LogLevel string `toml:"log-level" json:"log-level"`
	LogFile  string `toml:"log-file" json:"log-file"`

	// source database's config
	SourceDBCfg dbutil.DBConfig `toml:"source-db" json:"source-db"`

	// // target database's config
	// TargetDBCfg dbutil.DBConfig `toml:"target-db" json:"target-db"`

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int `toml:"chunk-size" json:"chunk-size"`

	// sampling check percent, for example 10 means only check 10% data
	Sample int `toml:"sample-percent" json:"sample-percent"`

	// how many goroutines are created to check data
	CheckThreadCount int `toml:"check-thread-count" json:"check-thread-count"`

	// the config of table to be checked
	Tables []*TableCheckCfg `toml:"check-table" json:"check-table"`

	Compares []*CompareConfig `toml:"compare" json:"compare"`
	// config file
	ConfigFile string

	ProfilePort int
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
