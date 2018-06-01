package reader

import (
	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/obinlog/go-binlog"
)

func init() {
	// log.SetLevel(log.LOG_LEVEL_NONE)
}

// Config for Reader
type Config struct {
	KafakaAddr []string
	// if ConmmitTs is specified, reader will start return the binlog which the ts bigger than the config ts
	CommitTS  int64
	Offset    int64 // start at kafka offset
	ClusterID string
}

// Message read from reader
type Message struct {
	Binlog *obinlog.Binlog
	Offset int64 // kafka offset
}

// Reader to read binlog from kafka
type Reader struct {
	cfg    *Config
	client sarama.Client

	msgs      chan *Message
	stop      chan struct{}
	clusterID string
}

func (r *Reader) getTopic() string {
	return r.cfg.ClusterID + "_obinlog"
}

// NewReader return a instance of Reader
func NewReader(cfg *Config) (r *Reader, err error) {
	r = &Reader{
		cfg:       cfg,
		stop:      make(chan struct{}),
		msgs:      make(chan *Message, 1024),
		clusterID: cfg.ClusterID,
	}

	r.client, err = sarama.NewClient(r.cfg.KafakaAddr, nil)
	if err != nil {
		r = nil
		return
	}

	if r.cfg.CommitTS > 0 {
		r.cfg.Offset, err = r.getOffsetByTS(r.cfg.CommitTS)
		if err != nil {
			r = nil
			return
		}
		log.Debug("set offset to: ", r.cfg.Offset)
	}

	go r.run()

	return
}

// Close shuts down the reader
func (r *Reader) Close() {
	close(r.stop)

	r.client.Close()
}

// Messages return a chan to read message
func (r *Reader) Messages() (msgs <-chan *Message) {
	return r.msgs
}

func (r *Reader) getOffsetByTS(ts int64) (offset int64, err error) {
	seeker, err := NewKafkaSeeker(r.cfg.KafakaAddr, nil)
	if err != nil {
		return
	}

	offsets, err := seeker.Seek(r.getTopic(), ts, []int32{0})
	if err != nil {
		return
	}

	offset = offsets[0]

	return
}

func (r *Reader) run() {
	offset := r.cfg.Offset
	log.Debug("start at offset: ", offset)

	consumer, err := sarama.NewConsumerFromClient(r.client)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(r.getTopic(), 0, offset)
	if err != nil {
		log.Fatal(err)
	}
	defer partitionConsumer.Close()

	for {
		select {
		case <-r.stop:
			partitionConsumer.Close()
			log.Debug("reader stop to run")
			return
		case kmsg := <-partitionConsumer.Messages():
			log.Debug("get kmsg offset: ", kmsg.Offset)
			binlog := new(obinlog.Binlog)
			err := binlog.Unmarshal(kmsg.Value)
			if err != nil {
				log.Warn(err)
				continue
			}
			if r.cfg.CommitTS > 0 && binlog.CommitTs <= r.cfg.CommitTS {
				continue
			}

			r.msgs <- &Message{
				Binlog: binlog,
				Offset: kmsg.Offset,
			}
		}

	}
	return
}
