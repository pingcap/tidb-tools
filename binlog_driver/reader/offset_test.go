package reader

import (
	"os"
	"testing"

	"github.com/Shopify/sarama"
	. "github.com/pingcap/check"
	pb "github.com/pingcap/tidb-tools/binlog_proto/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testOffsetSuite{})

type testOffsetSuite struct {
	producer  sarama.SyncProducer
	config    *sarama.Config
	addr      string
	available bool
}

func (to *testOffsetSuite) SetUpSuite(c *C) {
	to.addr = "127.0.0.1"
	if os.Getenv("HOSTIP") != "" {
		to.addr = os.Getenv("HOSTIP")
	}
	to.config = sarama.NewConfig()
	to.config.Producer.Partitioner = sarama.NewManualPartitioner
	to.config.Producer.Return.Successes = true
	var err error
	to.producer, err = sarama.NewSyncProducer([]string{to.addr + ":9092"}, to.config)
	if err == nil {
		to.available = true
	}
}

func (to *testOffsetSuite) TestOffset(c *C) {
	if !to.available {
		c.Skip("no kafka available")
	}

	topic := "test"

	sk, err := NewKafkaSeeker([]string{to.addr + ":9092"}, to.config)
	c.Assert(err, IsNil)

	to.producer, err = sarama.NewSyncProducer([]string{to.addr + ":9092"}, to.config)
	c.Assert(err, IsNil)
	defer to.producer.Close()

	var testPoss = map[int64]int64{
		10: 0,
		20: 0,
		30: 0,
	}
	for ts := range testPoss {
		testPoss[ts], err = to.procudeMessage(ts, topic)
		c.Assert(err, IsNil)
	}

	var testCases = map[int64]int64{
		1:  testPoss[10],
		10: testPoss[20],
		15: testPoss[20],
		20: testPoss[30],
		35: sarama.OffsetNewest,
	}
	for ts, offset := range testCases {
		offsetFounds, err := sk.Seek(topic, ts, []int32{0})
		c.Log("check: ", ts)
		c.Assert(err, IsNil)
		c.Assert(offsetFounds, HasLen, 1)
		c.Assert(offsetFounds[0], Equals, offset)
	}
}

func (to *testOffsetSuite) procudeMessage(ts int64, topic string) (offset int64, err error) {
	binlog := new(pb.Binlog)
	binlog.CommitTs = ts
	var data []byte
	data, err = binlog.Marshal()
	if err != nil {
		return
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(0),
		Key:       sarama.StringEncoder("key"),
		Value:     sarama.ByteEncoder(data),
	}
	_, offset, err = to.producer.SendMessage(msg)
	if err == nil {
		return
	}

	return offset, err
}
