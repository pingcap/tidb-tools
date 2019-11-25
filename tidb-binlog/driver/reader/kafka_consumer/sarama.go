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
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	saramaType = "sarama"
)

type Sarama struct {
	client    sarama.Client
	consumer  sarama.Consumer
	topic     string
	partition int32
}

func NewSaramaConsumer(cfg *KafkaConfig) (Consumer, error) {
	conf := sarama.NewConfig()
	// set to 10 minutes to prevent i/o timeout when reading huge message
	conf.Net.ReadTimeout = KafkaWaitTimeout
	if cfg.SaramaBufferSize > 0 {
		conf.ChannelBufferSize = cfg.SaramaBufferSize
	}

	client, err := sarama.NewClient(cfg.Addr, conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Sarama{
		client:    client,
		consumer:  consumer,
		topic:     cfg.Topic,
		partition: cfg.Partition,
	}, nil
}

// ConsumeFromOffset implements kafka.Consumer.ConsumerFromOffset
func (s *Sarama) ConsumeFromOffset(offset int64, consumerChan chan<- *KafkaMsg) error {
	partitionConsumer, err := s.consumer.ConsumePartition(s.topic, s.partition, offset)
	if err != nil {
		return errors.Trace(err)

	}
	defer partitionConsumer.Close()

	for {
		kmsg := <-partitionConsumer.Messages()
		msg := &KafkaMsg{
			Value:  kmsg.Value,
			Offset: kmsg.Offset,
		}
		consumerChan <- msg
	}
}

// SeekOffsetFromTS implements kafka.Consumer.SeekOffsetFromTS
func (s *Sarama) SeekOffsetFromTS(ts int64, topic string, partitions []int32) ([]int64, error) {
	var err error
	if len(partitions) == 0 {
		partitions, err = s.consumer.Partitions(topic)
		if err != nil {
			log.Error("get partitions from topic failed", zap.String("topic", topic), zap.Error(err))
			return nil, errors.Trace(err)
		}
	}

	return s.seekOffsets(topic, partitions, ts)
}

// seekOffsets returns all valid offsets in partitions
func (s *Sarama) seekOffsets(topic string, partitions []int32, pos int64) ([]int64, error) {
	offsets := make([]int64, len(partitions))
	for _, partition := range partitions {
		start, err := s.client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			err = errors.Trace(err)
			return nil, err
		}

		end, err := s.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			err = errors.Trace(err)
			return nil, err
		}

		log.Info("seek offsets in",
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Int64("start", start),
			zap.Int64("end", end),
			zap.Int64("target ts", pos))

		offset, err := s.seekOffset(topic, partition, start, end-1, pos)
		if err != nil {
			err = errors.Trace(err)
			return nil, err
		}

		log.Info("seek offset success", zap.Int64("offset", offset), zap.Int64("target ts", pos))
		offsets[partition] = offset
	}

	return offsets, nil
}

func (s *Sarama) seekOffset(topic string, partition int32, start int64, end int64, ts int64) (offset int64, err error) {
	startTS, err := s.getTSAtOffset(topic, partition, start)
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
		midTS, err = s.getTSAtOffset(topic, partition, mid)
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
	endTS, err = s.getTSAtOffset(topic, partition, end)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if endTS <= ts {
		return end + 1, nil
	}

	return end, nil
}

func (s *Sarama) getTSAtOffset(topic string, partition int32, offset int64) (ts int64, err error) {
	log.Debug("sarama start consumer on kafka",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset))

	pc, err := s.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	defer pc.Close()

	errorCnt := 0
	for {
		select {
		case msg := <-pc.Messages():
			ts, err = getTSFromMSG(s.ConsumerType(), &KafkaMsg{
				Offset: msg.Offset,
				Value:  msg.Value,
			})

			if err == nil {
				log.Debug("sarama get ts at offset success",
					zap.String("topic", topic),
					zap.Int32("partition", partition),
					zap.Int64("ts", ts),
					zap.Int64("at offset", offset))
			}

			err = errors.Trace(err)
			return

		case msg := <-pc.Errors():
			err = msg.Err
			log.Error("sarama get ts at offset failed",
				zap.String("topic", topic),
				zap.Int32("partition", partition),
				zap.Int64("ts", ts),
				zap.Int64("at offset", offset))
			time.Sleep(time.Second)
			errorCnt++
			if errorCnt > 10 {
				return
			}

		case <-time.After(KafkaWaitTimeout):
			return 0, errors.Errorf("sarama timeout to consume from kafka, topic:%s, partition:%d, offset:%d", topic, partition, offset)
		}
	}
}

// ConsumerType implements kafka.Consumer.Consumer.Type
func (s *Sarama) ConsumerType() string {
	return saramaType
}

// Close implements kafka.Consumer.Close
func (s *Sarama) Close() {
	s.client.Close()
}
