package tunnel

import (
	"bytes"
	"encoding/binary"
	"mongoshake/tunnel/kafka"

	LOG "github.com/vinllen/log4go"
)

type KafkaClusterReader struct {
	address string
	reader *kafka.ClusterReader
	replayer []Replayer
}

func (tunnel *KafkaClusterReader) Link(replayer []Replayer) error {
	reader, err := kafka.NewClusterReader(tunnel.address)
	if err != nil {
		LOG.Critical("KafkaReader link[%v] create reader error[%v]", tunnel.address, err)
		return err
	}

	tunnel.reader = reader
	tunnel.replayer = replayer
	go tunnel.replay()
	return nil
}

func (tunnel *KafkaClusterReader) MarkOffset(offset int64) {
	tunnel.reader.MarkOffset(offset)
}

func (tunnel *KafkaClusterReader) replay() {
	var message *kafka.Message
	var toRetry *kafka.Message

	for {
		if toRetry != nil {
			message = toRetry
		} else {
			// get one message
			message = <-tunnel.reader.Read()
		}

		byteBuffer := bytes.NewBuffer(message.Value)
		var checksum, tag, hashShard, compress uint32
		binary.Read(byteBuffer, binary.BigEndian, &checksum)
		binary.Read(byteBuffer, binary.BigEndian, &tag)
		binary.Read(byteBuffer, binary.BigEndian, &hashShard)
		binary.Read(byteBuffer, binary.BigEndian, &compress)

		var logCount uint32
		binary.Read(byteBuffer, binary.BigEndian, &logCount)

		var length uint32
		oplogs := [][]byte{}
		for logCount > 0 {
			binary.Read(byteBuffer, binary.BigEndian, &length)
			buffer := make([]byte, length)
			binary.Read(byteBuffer, binary.BigEndian, &buffer)
			oplogs = append(oplogs, buffer)
			logCount--
		}

		newLogs := &TMessage{Checksum: checksum, Tag: tag, Shard: hashShard, Compress: compress, RawLogs: oplogs}

		// re-sharding
		if newLogs.Shard >= uint32(len(tunnel.replayer)) {
			newLogs.Shard %= uint32(len(tunnel.replayer))
		}

		if toRetry != nil {
			newLogs.Tag |= MsgRetransmission
		}
		toRetry = nil

		replay := tunnel.replayer[newLogs.Shard]
		if replay.Sync(newLogs, func(context *kafka.Message) func() {
			return func() {
				// user can add the ack mechanism so that send ack
				// to kafka to move kafka offset forward. We don't offer this
				// code in current open source version.
				tunnel.reader.MarkOffset(message.Offset)
			}
		}(message)) < 0 {
			// bad information in message. need to retry
			toRetry = message
		}
	}
}