package checksum

import (
	"bytes"
	"io/ioutil"
	"strconv"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// Comparer compares checksum.
type Comparer struct {
	cfg []*CompareConfig
}

// NewComparer creates an Comparer
func NewComparer(cfg []*CompareConfig) *Comparer {
	return &Comparer{
		cfg: cfg,
	}
}

// Compare compares from-checksum and to-checksum.
func (c *Comparer) Compare() error {
	for _, cfg := range c.cfg {
		fromChecksum, err := c.xorChecksum(cfg.From)
		if err != nil {
			log.Errorf(errors.ErrorStack(err))
			continue
		}

		toChecksum, err := c.xorChecksum(cfg.To)
		if err != nil {
			log.Errorf(errors.ErrorStack(err))
			continue
		}

		if fromChecksum != toChecksum {
			log.Errorf("checksum mismatch from vs to: %d vs %d, from: %v, to: %v,", fromChecksum, toChecksum, cfg.From, cfg.To)
			continue
		}
		log.Infof("checksum pass. from: %v, to: %v", cfg.From, cfg.To)
	}

	return nil
}

func (c *Comparer) xorChecksum(files []string) (uint64, error) {
	var totalChecksum uint64

	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			return 0, errors.Trace(err)
		}

		data = bytes.TrimSpace(data)
		checksum, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return 0, errors.Errorf("parse checksum %s err %v", string(data), err)
		}
		totalChecksum ^= checksum
	}
	return totalChecksum, nil
}
