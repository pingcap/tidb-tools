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

package kafka_consumer

import (
	"strings"
	"time"

	"github.com/pingcap/errors"

	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

const (
	KafkaWaitTimeout = 10 * time.Minute
)

type KafkaConfig struct {
	ClientType       string
	Addr             []string
	Topic            string
	Partition        int32
	SaramaBufferSize int
}

type KafkaMsg struct {
	Offset int64
	Value  []byte
}

type Consumer interface {
	// ConsumeFromOffset consume message from specify offset
	ConsumeFromOffset(offset int64, consumerChan chan<- *KafkaMsg, done <-chan struct{}) error

	// SeekOffsetFromTS seeks the first offset which binlog CommitTs bigger than ts
	SeekOffsetFromTS(ts int64, topic string, partitions []int32) ([]int64, error)

	// ConsumerType return type of consumer
	ConsumerType() string

	// Close release resource of this consumer
	Close()
}

func getTSFromMSG(consumerType string, msg *KafkaMsg) (ts int64, err error) {
	err = TransformMSG(consumerType, msg)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	binlog := new(pb.Binlog)
	err = binlog.Unmarshal(msg.Value)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return binlog.CommitTs, nil
}

func TransformMSG(consumerType string, msg *KafkaMsg) error {
	// TODO extract msg from kafka msg
	switch consumerType {
	case saramaType:
		// parse msg to (*sarama.ConsumerMessage) then extract
	case kafkaGOType:
		// parse msg to (*kafka.Message) then extract
	default:
	}
	return nil
}

func NewConsumer(cfg *KafkaConfig) (Consumer, error) {
	switch strings.ToLower(cfg.ClientType) {
	case "sarama":
		return NewSaramaConsumer(cfg)
	case "kafka-go":
		return NewKafkaGoConsumer(cfg)
	default:
		return nil, errors.Errorf("Not support client:%s for now, please try sarama/kafka-go", cfg.ClientType)
	}
}
