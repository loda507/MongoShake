package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// collector的计数监控
	collectCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mongo-shake",
			Subsystem: "collector",
			Name:      "input_msg",
			Help:      "Total number of oplogs get by the collector",
		},
		// We will want to monitor the worker ID that processed the
		// job, and the type of job that was processed
		[]string{"collection", "type"},
	)

	// receiver的计数监控
	receiveCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mongo-shake",
			Subsystem: "receiver",
			Name:      "output_msg",
			Help:      "Total number of oplogs get by the receiver",
		},
		// We will want to monitor the worker ID that processed the
		// job, and the type of job that was processed
		[]string{"collection", "type"},
	)

	// checkpoint
	checkpointGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mongo-shake",
			Subsystem: "collector",
			Name:      "oplog checkpoint",
			Help:      "checkpoint of oplogs by the collector",
		},
		[]string{"instance"},
	)
)

func Init() {
	// register with the prometheus collector
	prometheus.MustRegister(collectCounterVec)
	prometheus.MustRegister(receiveCounterVec)
	prometheus.MustRegister(checkpointGaugeVec)
}

func AddCollectGet(instance string) {
	collectCounterVec.WithLabelValues(instance, "get").Inc()
}

func AddCollectSuccess(namespace string, count int) {
	collectCounterVec.WithLabelValues(namespace, "success").Add(float64(count))
}

func AddCollectFailed(instance string, count int) {
	collectCounterVec.WithLabelValues(instance, "failed").Add(float64(count))
}

func AddReceiveGet(collection string, count int) {
	receiveCounterVec.WithLabelValues(collection, "get").Add(float64(count))
}

func AddReceiveSuccess(namespace string) {
	receiveCounterVec.WithLabelValues(namespace, "success").Inc()
}

func AddReceiveFailed(namespace string) {
	receiveCounterVec.WithLabelValues(namespace, "failed").Inc()
}

func SetCheckpoint(instance string, ckpt int64) {
	checkpointGaugeVec.WithLabelValues(instance).Set(float64(ckpt))
}
