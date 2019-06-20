package kafka

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
)

type ClusterReader struct {
	brokers   []string
	topic     string
	partition int32

	Consumer       *cluster.Consumer
	messageChannel chan *Message
}

func NewClusterReader(address string) (*ClusterReader, error) {
	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions
	config.Version = sarama.V2_0_0_0

	topic, brokers, err := parse(address)
	if err != nil {
		return nil, err
	}

	// init consumer
	consumer, err := cluster.NewConsumer(brokers, "mongoshake-main-consumer", []string{topic}, config)
	if err != nil {
		return nil, err
	}

	// pay attention: we fetch data from oldest offset when starting by default, so a lot data will be
	// replay when receiver restarts.

	r := &ClusterReader {
		brokers:        brokers,
		topic:          topic,
		partition:      defaultPartition,
		Consumer:       consumer,
		messageChannel: make(chan *Message),
	}

	go r.send()
	return r, nil
}

func (r *ClusterReader) MarkOffset(offset int64) {
	r.Consumer.MarkPartitionOffset(r.topic, r.partition, offset, "")
}

func (r *ClusterReader) Read() chan *Message {
	return r.messageChannel
}

func (r *ClusterReader) send() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	for {
		select {
		case part, ok := <-r.Consumer.Partitions():
			if !ok {
				return
			}

			// 全部使用 Partition 0
			if 0 != part.Partition() {
				continue
			}

			// start a separate goroutine to consume messages
			go func(pc cluster.PartitionConsumer) {
				for msg := range pc.Messages() {
					r.messageChannel <- &Message{
						Key:       msg.Key,
						Value:     msg.Value,
						Offset:    msg.Offset,
						TimeStamp: msg.Timestamp,
					}
				}
			}(part)
		case <-signals:
			return
		}
	}
}