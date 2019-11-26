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
	"os"
	"sort"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/pingcap/check"
	"github.com/segmentio/kafka-go"

	pb "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct {
	saramaProducer sarama.SyncProducer
	kafkaProducer  *kafka.Conn
	config         *sarama.Config
	addr           string
	available      bool
	topic          string
}

func (to *testOffsetSuite) SetUpSuite(c *C) {
	to.topic = "test"
	to.addr = "192.168.198.61"
	if os.Getenv("HOSTIP") != "" {
		to.addr = os.Getenv("HOSTIP")
	}

	to.addr += ":9092"
	to.config = sarama.NewConfig()
	to.config.Producer.Partitioner = sarama.NewManualPartitioner
	to.config.Producer.Return.Successes = true
	to.config.Net.DialTimeout = time.Second * 3
	to.config.Net.ReadTimeout = time.Second * 3
	to.config.Net.WriteTimeout = time.Second * 3
	// need at least version to delete topic
	to.config.Version = sarama.V0_10_1_0
	var err error
	to.saramaProducer, err = sarama.NewSyncProducer([]string{to.addr}, to.config)
	if err == nil {
		to.available = true
	}
}

func (to *testOffsetSuite) deleteTopic(c *C, topic string) {
	broker := sarama.NewBroker(to.addr)
	err := broker.Open(to.config)
	c.Assert(err, IsNil)
	connected, err := broker.Connected()
	c.Assert(connected, IsTrue)
	c.Assert(err, IsNil)

	defer broker.Close()
	_, err = broker.DeleteTopics(&sarama.DeleteTopicsRequest{Topics: []string{topic}, Timeout: 10 * time.Second})
	c.Assert(err, IsNil)
}

func (to *testOffsetSuite) createTopic(c *C, topic string) {
	broker := sarama.NewBroker(to.addr)
	err := broker.Open(to.config)
	c.Assert(err, IsNil)
	connected, err := broker.Connected()
	c.Assert(connected, IsTrue)
	c.Assert(err, IsNil)

	defer broker.Close()
	_, err = broker.CreateTopics(&sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			topic: {
				NumPartitions:     1,
				ReplicationFactor: 1,
				ConfigEntries:     make(map[string]*string),
			},
		}, Timeout: 10 * time.Second, ValidateOnly: false})
	c.Assert(err, IsNil)
}

func (to *testOffsetSuite) TestSaramaOffset(c *C) {
	if !to.available {
		c.Skip("no kafka available")
	}

	topic := to.topic + "_sarama"

	to.deleteTopic(c, topic)
	to.createTopic(c, topic)
	defer to.deleteTopic(c, topic)

	sc, err := NewConsumer(&KafkaConfig{
		ClientType: saramaType,
		Addr:       []string{to.addr},
		Topic:      topic,
		Partition:  0,
	})
	c.Assert(err, IsNil)

	to.saramaProducer, err = sarama.NewSyncProducer([]string{to.addr}, to.config)
	c.Assert(err, IsNil)
	defer to.saramaProducer.Close()

	var testPoss = map[int64]int64{
		10: 0,
		20: 0,
		30: 0,
	}

	keys := make([]int64, 0)
	for k := range testPoss {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, ts := range keys {
		testPoss[ts], err = to.produceMessage(saramaType, ts, topic)
		c.Assert(err, IsNil)
		c.Log("produce ", ts, " at ", testPoss[ts])
	}

	var testCases = map[int64]int64{
		1:  testPoss[10],
		10: testPoss[20],
		15: testPoss[20],
		20: testPoss[30],
		35: testPoss[30] + 1,
	}
	for ts, offset := range testCases {
		offsetFounds, err := sc.SeekOffsetFromTS(ts, topic, []int32{0})
		c.Log("check: ", ts)
		c.Assert(err, IsNil)
		c.Assert(offsetFounds, HasLen, 1)
		c.Assert(offsetFounds[0], Equals, offset)
	}
}

func (to *testOffsetSuite) TestSaramaConsumer(c *C) {
	if !to.available {
		c.Skip("no kafka available")
	}
	var err error
	topic := to.topic + "_consumer_sarama"
	to.saramaProducer, err = sarama.NewSyncProducer([]string{to.addr}, to.config)
	c.Assert(err, IsNil)
	defer to.saramaProducer.Close()

	var testPoss = map[int64]int64{
		10: 0,
		20: 0,
		30: 0,
	}

	keys := make([]int64, 0)
	for k := range testPoss {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	offset := int64(-1)
	for _, ts := range keys {
		testPoss[ts], err = to.produceMessage(saramaType, ts, topic)
		if offset == -1 {
			offset = testPoss[ts]
		}
		c.Assert(err, IsNil)
		c.Log("produce ", ts, " at ", testPoss[ts])
	}

	c.Log("start consumer from", offset)
	kc, err := NewConsumer(&KafkaConfig{
		ClientType: saramaType,
		Addr:       []string{to.addr},
		Topic:      topic,
		Partition:  0,
	})
	c.Assert(err, IsNil)

	consumerChan := make(chan *KafkaMsg, 1)
	done := make(chan struct{})
	msgCnt := 0
	go func() {
		for {
			msg := <-consumerChan
			ts, err := getTSFromMSG(saramaType, msg)
			c.Assert(err, IsNil)
			c.Assert(ts, Equals, testPoss[msg.Offset])
			msgCnt++
			if msgCnt >= len(testPoss) {
				close(done)
				break
			}
		}
	}()
	err = kc.ConsumeFromOffset(offset, consumerChan, done)
	c.Assert(err, IsNil)
	c.Assert(testPoss, HasLen, msgCnt)
}

func (to *testOffsetSuite) TestKafkaGoOffset(c *C) {
	if !to.available {
		c.Skip("no kafka available")
	}

	topic := to.topic + "_kafka"

	to.deleteTopic(c, topic)
	to.createTopic(c, topic)
	defer to.deleteTopic(c, topic)

	kc, err := NewConsumer(&KafkaConfig{
		ClientType: kafkaGOType,
		Addr:       []string{to.addr},
		Topic:      topic,
		Partition:  0,
	})
	c.Assert(err, IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	to.kafkaProducer, err = kafka.DialLeader(ctx, "tcp", to.addr, topic, 0)
	c.Assert(err, IsNil)
	defer to.kafkaProducer.Close()

	var testPoss = map[int64]int64{
		10: 0,
		20: 0,
		30: 0,
	}

	keys := make([]int64, 0)
	for k := range testPoss {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, ts := range keys {
		testPoss[ts], err = to.produceMessage(kafkaGOType, ts, topic)
		c.Assert(err, IsNil)
		c.Log("produce ", ts, " at ", testPoss[ts])
	}

	var testCases = map[int64]int64{
		1:  testPoss[10],
		10: testPoss[20],
		15: testPoss[20],
		20: testPoss[30],
		35: testPoss[30] + 1,
	}
	for ts, offset := range testCases {
		offsetFounds, err := kc.SeekOffsetFromTS(ts, topic, []int32{0})
		c.Log("check: ", ts)
		c.Assert(err, IsNil)
		c.Assert(offsetFounds, HasLen, 1)
		c.Assert(offsetFounds[0], Equals, offset)
	}
}

func (to *testOffsetSuite) TestKafkaConsumer(c *C) {
	if !to.available {
		c.Skip("no kafka available")
	}
	var err error
	topic := to.topic + "_consumer"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	to.kafkaProducer, err = kafka.DialLeader(ctx, "tcp", to.addr, topic, 0)
	c.Assert(err, IsNil)
	defer to.kafkaProducer.Close()

	var testPoss = map[int64]int64{
		10: 0,
		20: 0,
		30: 0,
	}

	keys := make([]int64, 0)
	for k := range testPoss {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	offset := int64(-1)

	for _, ts := range keys {
		testPoss[ts], err = to.produceMessage(kafkaGOType, ts, topic)
		if offset == -1 {
			offset = testPoss[ts]
		}

		c.Assert(err, IsNil)
		c.Log("produce ", ts, " at ", testPoss[ts])
	}

	c.Log("start consumer from", offset)
	kc, err := NewConsumer(&KafkaConfig{
		ClientType: kafkaGOType,
		Addr:       []string{to.addr},
		Topic:      topic,
		Partition:  0,
	})
	c.Assert(err, IsNil)

	consumerChan := make(chan *KafkaMsg, 1)
	done := make(chan struct{})

	msgCnt := 0
	go func() {
		for {
			msg := <-consumerChan
			ts, err := getTSFromMSG(saramaType, msg)
			c.Assert(err, IsNil)
			c.Assert(ts, Equals, testPoss[msg.Offset])
			msgCnt++
			if msgCnt >= len(testPoss) {
				close(done)
				break
			}
		}
	}()
	err = kc.ConsumeFromOffset(offset, consumerChan, done)
	c.Assert(err, IsNil)
	c.Assert(testPoss, HasLen, msgCnt)
}

func (to *testOffsetSuite) produceMessage(clientType string, ts int64, topic string) (offset int64, err error) {
	binlog := new(pb.Binlog)
	binlog.CommitTs = ts
	var data []byte
	data, err = binlog.Marshal()
	if err != nil {
		return
	}

	switch clientType {
	case saramaType:
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(0),
			Key:       sarama.StringEncoder("key"),
			Value:     sarama.ByteEncoder(data),
		}
		_, offset, err = to.saramaProducer.SendMessage(msg)
		if err != nil {
			return
		}

	case kafkaGOType:
		_, _, offset, _, err = to.kafkaProducer.WriteCompressedMessagesAt(nil, kafka.Message{
			Value: data,
		})
		if err != nil {
			return
		}
	}

	return offset, err
}
