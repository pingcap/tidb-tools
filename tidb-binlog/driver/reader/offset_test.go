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
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/segmentio/kafka-go"

	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct {
	producer  *kafka.Conn
	addr      string
	available bool
	topic     string
}

func (to *testOffsetSuite) SetUpSuite(c *C) {
	to.topic = "test"
	to.addr = "192.168.198.61"
	if os.Getenv("HOSTIP") != "" {
		to.addr = os.Getenv("HOSTIP")
	}
	to.addr = to.addr + ":9092"

	var err error
	if to.producer, err = kafka.Dial("tcp", to.addr); err == nil {
		to.available = true
	}
}

func (to *testOffsetSuite) deleteTopic(c *C) {
	conn, err := kafka.Dial("tcp", to.addr)
	c.Assert(err, IsNil)
	defer conn.Close()

	c.Assert(conn.DeleteTopics(to.topic), IsNil)
}

func (to *testOffsetSuite) TestOffset(c *C) {
	if !to.available {
		c.Skip("no kafka available")
	}

	to.deleteTopic(c)
	defer to.deleteTopic(c)

	topic := to.topic
	err := to.producer.CreateTopics(kafka.TopicConfig{
		Topic: topic,
	})
	c.Assert(err, IsNil)

	readCfg := kafka.ReaderConfig{
		Brokers:   []string{to.addr},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3,
		MaxBytes:  10e6,
	}
	sk, err := NewKafkaSeeker(readCfg)
	c.Assert(err, IsNil)

	var testPoss = map[int64]int64{
		10: 0,
		20: 0,
		30: 0,
	}
	for ts := range testPoss {
		testPoss[ts], err = to.produceMessage(ts, topic)
		c.Assert(err, IsNil)
		// c.Log("produce ", ts, " at ", testPoss[ts])
	}

	var testCases = map[int64]int64{
		1:  testPoss[10],
		10: testPoss[20],
		15: testPoss[20],
		20: testPoss[30],
		35: testPoss[30] + 1,
	}
	for ts, offset := range testCases {
		offsetFounds, err := sk.Seek(topic, ts, []int32{0})
		c.Log("check: ", ts)
		c.Assert(err, IsNil)
		c.Assert(offsetFounds, HasLen, 1)
		c.Assert(offsetFounds[0], Equals, offset)
	}
}

func (to *testOffsetSuite) produceMessage(ts int64, topic string) (offset int64, err error) {
	binlog := new(pb.Binlog)
	binlog.CommitTs = ts
	var data []byte
	data, err = binlog.Marshal()
	if err != nil {
		return
	}

	msg := kafka.Message{
		Topic:     topic,
		Partition: 0,
		Key:       []byte(""),
		Value:     data,
	}

	_, _, offset, _, err = to.producer.WriteCompressedMessagesAt(nil, msg)
	if err != nil {
		return
	}
	return
}
