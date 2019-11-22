// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package reader

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	KafkaWaitTimeout = 10 * time.Minute
)

// Config for Reader
type Config struct {
	KafkaAddr []string
	// the CommitTs of binlog return by reader will bigger than the config CommitTs
	CommitTS int64
	Offset   int64 // start at kafka offset
	// if Topic is empty, use the default name in drainer <ClusterID>_obinlog
	Topic     string
	ClusterID string
	// buffer size of messages of the reader internal.
	// default value 1.
	// suggest only setting the buffer size of this if you want the reader to buffer
	// message internal and leave `SaramaBufferSize` as 1 default.
	MessageBufferSize int
	// the sarama internal buffer size of messages.
	SaramaBufferSize int
}

func (c *Config) getSaramaBuffserSize() int {
	if c.SaramaBufferSize > 0 {
		return c.SaramaBufferSize
	}

	return 1
}

func (c *Config) getMessageBufferSize() int {
	if c.MessageBufferSize > 0 {
		return c.MessageBufferSize
	}

	return 1
}

func (c *Config) valid() error {
	if len(c.Topic) == 0 && len(c.ClusterID) == 0 {
		return errors.New("Topic or ClusterID must be set")
	}

	return nil
}

// Message read from reader
type Message struct {
	Binlog *pb.Binlog
	Offset int64 // kafka offset
}

// Reader to read binlog from kafka
type Reader struct {
	cfg       *Config
	clientCfg kafka.ReaderConfig

	msgs      chan *Message
	stop      chan struct{}
	clusterID string
}

func (r *Reader) getTopic() (string, int32) {
	if r.cfg.Topic != "" {
		return r.cfg.Topic, 0
	}

	return r.cfg.ClusterID + "_obinlog", 0
}

// NewReader creates an instance of Reader
func NewReader(cfg *Config) (r *Reader, err error) {
	err = cfg.valid()
	if err != nil {
		errors.Trace(err)
	}

	r = &Reader{
		cfg:       cfg,
		stop:      make(chan struct{}),
		msgs:      make(chan *Message, cfg.getMessageBufferSize()),
		clusterID: cfg.ClusterID,
	}

	topic, partition := r.getTopic()
	r.clientCfg = kafka.ReaderConfig{
		Brokers:   cfg.KafkaAddr,
		Topic:     topic,
		Partition: int(partition),
		MinBytes:  10e3, // 1KB
		MaxBytes:  10e6, // 1MB
	}

	if r.cfg.CommitTS > 0 {
		r.cfg.Offset, err = r.getOffsetByTS(r.cfg.CommitTS)
		if err != nil {
			err = errors.Trace(err)
			r = nil
			return
		}
		log.Debug("set offset to", zap.Int64("offset", r.cfg.Offset))
	}

	go r.run()

	return
}

// Close shuts down the reader
func (r *Reader) Close() {
	close(r.stop)
}

// Messages returns a chan that contains unread buffered message
func (r *Reader) Messages() (msgs <-chan *Message) {
	return r.msgs
}

func (r *Reader) getOffsetByTS(ts int64) (offset int64, err error) {
	seeker, err := NewKafkaSeeker(r.clientCfg)
	if err != nil {
		return 0, errors.Trace(err)
	}

	log.Debug("get offset",
		zap.String("topic", r.clientCfg.Topic),
		zap.Int("partition", r.clientCfg.Partition),
		zap.Int64("ts", ts))
	offsets, err := seeker.Seek(r.clientCfg.Topic, ts, []int32{int32(r.clientCfg.Partition)})
	if err != nil {
		err = errors.Trace(err)
		return
	}

	offset = offsets[0]

	return
}

func (r *Reader) run() {
	offset := r.cfg.Offset
	log.Debug("start at", zap.Int64("offset", offset))

	consumer := kafka.NewReader(r.clientCfg)
	err := consumer.SetOffset(offset)
	if err != nil {
		log.Fatal("consumer set offset failed", zap.Error(err))
	}

	defer consumer.Close()

	// add select to avoid message blocking while reading
	for {
		select {
		case <-r.stop:
			// clean environment
			consumer.Close()
			close(r.msgs)
			log.Info("reader stop to run")
			return

		default:
			ctx, cancel := context.WithTimeout(context.Background(), KafkaWaitTimeout)
			kmsg, err := consumer.ReadMessage(ctx)
			cancel()
			if err != nil {
				log.Warn("unmarshal binlog failed", zap.Error(err))
				continue
			}
			log.Debug("get kafka message", zap.Int64("offset", kmsg.Offset))
			binlog := new(pb.Binlog)
			err = binlog.Unmarshal(kmsg.Value)
			if err != nil {
				log.Warn("unmarshal binlog failed", zap.Error(err))
				continue
			}
			if r.cfg.CommitTS > 0 && binlog.CommitTs <= r.cfg.CommitTS {
				log.Warn("skip binlog CommitTs", zap.Int64("commitTS", binlog.CommitTs))
				continue
			}

			msg := &Message{
				Binlog: binlog,
				Offset: kmsg.Offset,
			}
			select {
			case r.msgs <- msg:
			case <-r.stop:
				// In the next iteration, the <-r.stop would match again and prepare to quit
				continue
			}
		}
	}
}
