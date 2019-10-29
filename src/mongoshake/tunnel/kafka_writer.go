package tunnel

import (
	"bytes"
	"encoding/binary"
	"mongoshake/metrics"

	"mongoshake/tunnel/kafka"

	LOG "github.com/vinllen/log4go"
)

type KafkaWriter struct {
	RemoteAddr string
	writer     *kafka.SyncWriter
}

func (tunnel *KafkaWriter) Prepare() bool {
	writer, err := kafka.NewSyncWriter(tunnel.RemoteAddr)
	if err != nil {
		LOG.Critical("KafkaWriter prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
		return false
	}
	if err := writer.Start(); err != nil {
		LOG.Critical("KafkaWriter prepare[%v] start writer error[%v]", tunnel.RemoteAddr, err)
		return false
	}
	tunnel.writer = writer
	return true
}

func (tunnel *KafkaWriter) Send(message *WMessage) int64 {
	if len(message.RawLogs) == 0 || message.Tag&MsgProbe != 0 {
		return 0
	}

	message.Tag |= MsgPersistent

	byteBuffer := bytes.NewBuffer([]byte{})
	// checksum
	binary.Write(byteBuffer, binary.BigEndian, uint32(message.Checksum))
	// tag
	binary.Write(byteBuffer, binary.BigEndian, uint32(message.Tag))
	// shard
	binary.Write(byteBuffer, binary.BigEndian, uint32(message.Shard))
	// compressor
	binary.Write(byteBuffer, binary.BigEndian, uint32(message.Compress))
	// serialize log count
	binary.Write(byteBuffer, binary.BigEndian, uint32(len(message.RawLogs)))

	// serialize logs
	for _, log := range message.RawLogs {
		binary.Write(byteBuffer, binary.BigEndian, uint32(len(log)))
		binary.Write(byteBuffer, binary.BigEndian, log)
	}
	err := tunnel.writer.SimpleWrite(byteBuffer.Bytes())

	if err != nil {
		metrics.AddCollectFailed(message.ParsedLogs[0].Namespace, len(message.RawLogs))
		LOG.Error("size:[%v], len:[%v]", len(byteBuffer.Bytes()), len(message.RawLogs))
		LOG.Error("KafkaWriter send[%v][%v] to [%v] error[%v]", message.ParsedLogs[0].Namespace, message.ParsedLogs[0].Query, tunnel.RemoteAddr, err)
		// TODO(shushi): 如果存在超过M的数据，写入失败继续
		return 0
	}

	metrics.AddCollectSuccess(message.ParsedLogs[0].Namespace, len(message.RawLogs))
	// KafkaWriter.AckRequired() is always false, return 0 directly
	return 0
}

func (tunnel *KafkaWriter) AckRequired() bool {
	return false
}

func (tunnel *KafkaWriter) ParsedLogsRequired() bool {
	return false
}
