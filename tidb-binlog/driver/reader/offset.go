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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

// KafkaSeeker seeks offset in kafka topics by given condition
type KafkaSeeker struct {
	conn   *kafka.Conn
	reader *kafka.Reader
}

// NewKafkaSeeker creates an instance of KafkaSeeker
func NewKafkaSeeker(readCfg kafka.ReaderConfig) (*KafkaSeeker, error) {
	if len(readCfg.Brokers) == 0 {
		return nil, errors.New("there is no broker set in config")
	}
	conn, err := kafka.Dial("tcp", readCfg.Brokers[0])
	if err != nil {
		return nil, errors.Trace(err)
	}

	reader := kafka.NewReader(readCfg)
	s := &KafkaSeeker{
		conn:   conn,
		reader: reader,
	}
	return s, nil
}

// Close releases resources of KafkaSeeker
func (ks *KafkaSeeker) Close() {
	ks.conn.Close()
}

// Seek seeks the first offset which binlog CommitTs bigger than ts
func (ks *KafkaSeeker) Seek(topic string, ts int64, partitions []int32) (offsets []int64, err error) {
	if len(partitions) == 0 {
		pts, err := ks.conn.ReadPartitions(topic)
		if err != nil {
			log.Error("get partitions from topic failed", zap.String("topic", topic), zap.Error(err))
			return nil, errors.Trace(err)
		}
		for _, pt := range pts {
			partitions = append(partitions, int32(pt.ID))
		}
	}

	offsets, err = ks.seekOffsets(topic, partitions, ts)
	if err != nil {
		err = errors.Trace(err)
		log.Error("seek offsets failed", zap.Error(err))
	}
	return
}

func (ks *KafkaSeeker) getTSFromMSG(msg kafka.Message) (ts int64, err error) {
	binlog := new(pb.Binlog)
	err = binlog.Unmarshal(msg.Value)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	return binlog.CommitTs, nil
}

// seekOffsets returns all valid offsets in partitions
func (ks *KafkaSeeker) seekOffsets(topic string, partitions []int32, pos int64) ([]int64, error) {
	offsets := make([]int64, len(partitions))
	for _, partition := range partitions {
		start, err := ks.conn.ReadFirstOffset()
		if err != nil {
			err = errors.Trace(err)
			return nil, err
		}

		end, err := ks.conn.ReadLastOffset()
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

		offset, err := ks.seekOffset(topic, partition, start, end-1, pos)
		if err != nil {
			err = errors.Trace(err)
			return nil, err
		}

		log.Info("seek offset success", zap.Int64("offset", offset), zap.Int64("target ts", pos))
		offsets[partition] = offset
	}

	return offsets, nil
}

func (ks *KafkaSeeker) seekOffset(topic string, partition int32, start int64, end int64, ts int64) (offset int64, err error) {
	startTS, err := ks.getTSAtOffset(topic, partition, start)
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
		midTS, err = ks.getTSAtOffset(topic, partition, mid)
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
	endTS, err = ks.getTSAtOffset(topic, partition, end)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if endTS <= ts {
		return end + 1, nil
	}

	return end, nil
}

func (ks *KafkaSeeker) getTSAtOffset(topic string, partition int32, offset int64) (ts int64, err error) {
	log.Debug("start consumer on kafka",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset))

	err = ks.reader.SetOffset(offset)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), KafkaWaitTimeout)
	defer cancel()
	msg, err := ks.reader.ReadMessage(ctx)
	ts, err = ks.getTSFromMSG(msg)

	if err == nil {
		log.Debug("get ts at offset success",
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Int64("ts", ts),
			zap.Int64("at offset", offset))
	}

	return
}
