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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/3pointer/tidb-tools/tidb-binlog/driver/reader/kafka_consumer"
	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
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

	// which kafka client to use, support sarama/kafka-go for now
	ClientType string
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

func (c *Config) GetTopic() (string, int32) {
	if c.Topic != "" {
		return c.Topic, 0
	}
	return c.ClusterID + "_obinlog", 0
}

// Message read from reader
type Message struct {
	Binlog *pb.Binlog
	Offset int64 // kafka offset
}

// Reader to read binlog from kafka
type Reader struct {
	cfg    *Config
	client kafka_consumer.Consumer

	msgs      chan *Message
	stop      chan struct{}
	clusterID string
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

	topic, partition := cfg.GetTopic()
	r.client, err = kafka_consumer.NewConsumer(&kafka_consumer.KafkaConfig{
		ClientType:       cfg.ClientType,
		Addr:             cfg.KafkaAddr,
		Topic:            topic,
		Partition:        partition,
		SaramaBufferSize: cfg.getSaramaBuffserSize(),
	})

	if r.cfg.CommitTS > 0 {
		r.cfg.Offset, err = r.getOffsetByTS(r.cfg.CommitTS, topic, []int32{partition})
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

func (r *Reader) getOffsetByTS(ts int64, topic string, partitions []int32) (offset int64, err error) {
	offsets, err := r.client.SeekOffsetFromTS(ts, topic, partitions)
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
	consumerChan := make(chan *kafka_consumer.KafkaMsg)
	done := make(chan struct{})

	go func() {
		// add select to avoid message blocking while reading
		for {
			select {
			case <-r.stop:
				// clean environment
				r.client.Close()
				close(r.msgs)
				close(done)
				log.Info("reader stop to run")
				return

			case kmsg := <-consumerChan:
				err := kafka_consumer.TransformMSG(r.client.ConsumerType(), kmsg)
				if err != nil {
					log.Warn("transform message failed", zap.Error(err))
					continue
				}
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
					Offset: binlog.CommitTs,
				}
				select {
				case r.msgs <- msg:
				case <-r.stop:
					// In the next iteration, the <-r.stop would match again and prepare to quit
					continue
				}
			}
		}
	}()

	err := r.client.ConsumeFromOffset(offset, consumerChan, done)
	if err != nil {
		log.Error("consume from offset failed",
			zap.String("client", r.cfg.ClientType),
			zap.Int64("offset", offset),
			zap.Error(err))
		r.Close()
	}
}
