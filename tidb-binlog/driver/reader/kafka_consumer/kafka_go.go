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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	kafkaGOType = "kafka-go"
)

type KafkaGO struct {
	// client is high level api, only can consumer from offset
	client *kafka.Reader
	// conn is low level api, which has createTopic DeleteTopic and other more function than *kafka.Reader
	conn *kafka.Conn

	ctx    context.Context
	cancel context.CancelFunc
}

// NewKafkaGoConsumer return kafka-go consumer on specify topic and partition
func NewKafkaGoConsumer(cfg *KafkaConfig) (Consumer, error) {
	if len(cfg.Addr) == 0 {
		return nil, errors.New("no available kafka address")
	}
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := kafka.DialLeader(ctx, "tcp", cfg.Addr[0], cfg.Topic, int(cfg.Partition))
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	return &KafkaGO{
		client: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   cfg.Addr,
			Topic:     cfg.Topic,
			Partition: int(cfg.Partition),
			MinBytes:  10e3, // 1KB
			MaxBytes:  10e6, // 1MB
		}),
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// ConsumerFromOffset implements Consumer.ConsumerFromOffset
func (k *KafkaGO) ConsumeFromOffset(offset int64, consumerChan chan<- *KafkaMsg, done <-chan struct{}) error {
	err := k.client.SetOffset(offset)
	if err != nil {
		return errors.Trace(err)
	}
	for {
		select {
		case <-done:
			log.Info("finished consumer")
			return nil
		default:
			ctx, cancel := context.WithTimeout(k.ctx, KafkaWaitTimeout)
			kmsg, err := k.client.ReadMessage(ctx)
			cancel()
			if err != nil {
				log.Warn("kafka-go consume from offset failed",
					zap.Int64("offset", k.client.Offset()),
					zap.Error(err))
				return errors.Trace(err)
			}
			msg := &KafkaMsg{
				Value:  kmsg.Value,
				Offset: kmsg.Offset,
			}
			consumerChan <- msg
		}
	}
}

// SeekOffsetFromTS implements Consumer.SeekOffsetFromTS
func (k *KafkaGO) SeekOffsetFromTS(ts int64, topic string, partitions []int32) ([]int64, error) {
	if len(partitions) == 0 {
		pts, err := k.conn.ReadPartitions(topic)
		if err != nil {
			log.Error("get partitions from topic failed", zap.String("topic", topic), zap.Error(err))
			return nil, errors.Trace(err)
		}
		for _, pt := range pts {
			partitions = append(partitions, int32(pt.ID))
		}
	}
	return k.seekOffsets(topic, partitions, ts)
}

// seekOffsets returns all valid offsets in partitions
func (k *KafkaGO) seekOffsets(topic string, partitions []int32, pos int64) ([]int64, error) {
	offsets := make([]int64, len(partitions))
	for _, partition := range partitions {
		start, err := k.conn.ReadFirstOffset()
		if err != nil {
			return nil, errors.Trace(err)
		}

		end, err := k.conn.ReadLastOffset()
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Info("seek offsets in",
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Int64("start", start),
			zap.Int64("end", end),
			zap.Int64("target ts", pos))

		offset, err := k.seekOffset(topic, partition, start, end-1, pos)
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Info("seek offset success", zap.Int64("offset", offset), zap.Int64("target ts", pos))
		offsets[partition] = offset
	}

	return offsets, nil
}

func (k *KafkaGO) seekOffset(topic string, partition int32, start int64, end int64, ts int64) (offset int64, err error) {
	startTS, err := k.getTSAtOffset(topic, partition, start)
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
		midTS, err = k.getTSAtOffset(topic, partition, mid)
		if err != nil {
			err = errors.Trace(err)
			return
		}

		if midTS <= ts {
			start = mid + 1
		} else {
			end = mid
		}
	}

	var endTS int64
	endTS, err = k.getTSAtOffset(topic, partition, end)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if endTS <= ts {
		return end + 1, nil
	}

	return end, nil
}

func (k *KafkaGO) getTSAtOffset(topic string, partition int32, offset int64) (ts int64, err error) {
	log.Debug("start consumer on kafka",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset))

	err = k.client.SetOffset(offset)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), KafkaWaitTimeout)
	defer cancel()
	msg, err := k.client.ReadMessage(ctx)
	ts, err = getTSFromMSG(k.ConsumerType(), &KafkaMsg{
		Offset: msg.Offset,
		Value:  msg.Value,
	})

	if err == nil {
		log.Debug("get ts at offset success",
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Int64("ts", ts),
			zap.Int64("at offset", offset))
	}

	return
}

// ConsumerType implements Consumer.ConsumerType
func (k *KafkaGO) ConsumerType() string {
	return kafkaGOType
}

// Close release resource of this consumer
func (k *KafkaGO) Close() {
	k.client.Close()
	k.conn.Close()
	k.cancel()
}
