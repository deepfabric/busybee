package metric

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	inputEventQueueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "event",
			Name:      "input_queue_size",
			Help:      "Total size of input event queue size.",
		}, []string{"tenant"})

	outputEventQueueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "event",
			Name:      "output_queue_size",
			Help:      "Total size of oupt event queue size.",
		}, []string{"tenant"})
)

// SetEventQueueSize set event queue size
func SetEventQueueSize(value uint64, tenant string, group metapb.Group) {
	switch group {
	case metapb.TenantInputGroup:
		inputEventQueueSizeGauge.WithLabelValues(tenant).Set(float64(value))
	case metapb.TenantOutputGroup:
		outputEventQueueSizeGauge.WithLabelValues(tenant).Set(float64(value))
	}
}
