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
	"github.com/pingcap/log"
	"go.uber.org/zap"

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

func seekOffset(topic string, partition int32, start int64, end int64, ts int64, getTS func(string, int32, int64) (int64, error)) (offset int64, err error) {
	startTS, err := getTS(topic, partition, start)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if ts < startTS {
		log.Warn("given ts is smaller than oldest message's ts, some binlogs may lose", zap.Int64("given ts", ts), zap.Int64("oldest ts", startTS))
		offset = start
		return
	} else if ts == startTS {
		offset = start + 1
		return
	}

	for start < end {
		mid := (end-start)/2 + start
		var midTS int64
		midTS, err = getTS(topic, partition, mid)
		if err != nil {
			err = errors.Trace(err)
			return
		}

		if midTS < ts {
			start = mid + 1
		} else if midTS > ts {
			end = mid
		} else {
			return mid, nil
		}
	}

	var endTS int64
	endTS, err = getTS(topic, partition, end)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if endTS <= ts {
		return end + 1, nil
	}

	return end, nil
}

func TransformMSG(consumerType string, msg *KafkaMsg) error {
	// TODO extract msg from kafka msg
	switch consumerType {
	case SaramaType:
		// parse msg to (*sarama.ConsumerMessage) then extract
	case KafkaGOType:
		// parse msg to (*kafka.Message) then extract
	default:
	}
	return nil
}

func NewConsumer(cfg *KafkaConfig) (Consumer, error) {
	switch strings.ToLower(cfg.ClientType) {
	case SaramaType:
		return NewSaramaConsumer(cfg)
	case KafkaGOType:
		return NewKafkaGoConsumer(cfg)
	default:
		return nil, errors.Errorf("Unsupported client:%s for now, please try sarama/kafka-go", cfg.ClientType)
	}
}
