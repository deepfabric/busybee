package metric

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	requestReceivedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "busybee",
			Subsystem: "api",
			Name:      "request_received_total",
			Help:      "Total number of request received.",
		}, []string{"type"})

	requestResultCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "busybee",
			Subsystem: "api",
			Name:      "request_result_total",
			Help:      "Total number of request handled result.",
		}, []string{"type", "result"})

	storageFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "busybee",
			Subsystem: "storage",
			Name:      "failed_total",
			Help:      "Total number of request storage failed.",
		})

	inputEventAddedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "busybee",
			Subsystem: "event",
			Name:      "input_total",
			Help:      "Total number of output event added.",
		}, []string{"tenant"})

	outputEventAddedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "busybee",
			Subsystem: "event",
			Name:      "output_total",
			Help:      "Total number of input event added.",
		}, []string{"tenant"})

	inputEventHandledCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "busybee",
			Subsystem: "event",
			Name:      "input_handled_total",
			Help:      "Total number of event handled.",
		}, []string{"tenant"})

	outputEventHandledCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "busybee",
			Subsystem: "event",
			Name:      "input_handled_total",
			Help:      "Total number of event handled.",
		}, []string{"tenant"})
)

// IncRequestReceived inc request received
func IncRequestReceived(t string) {
	requestReceivedCounter.WithLabelValues(t).Inc()
}

// IncRequestSucceed inc request handled succeed
func IncRequestSucceed(t string) {
	requestResultCounter.WithLabelValues(t, "succeed").Inc()
}

// IncRequestFailed inc request handled failed
func IncRequestFailed(t string) {
	requestResultCounter.WithLabelValues(t, "failed").Inc()
}

// IncEventAdded inc event queue addded
func IncEventAdded(value int, tenant string, group metapb.Group) {
	switch group {
	case metapb.TenantInputGroup:
		inputEventAddedCounter.WithLabelValues(tenant).Add(float64(value))
	case metapb.TenantOutputGroup:
		outputEventAddedCounter.WithLabelValues(tenant).Add(float64(value))
	}
}

// IncEventHandled inc event queue handled
func IncEventHandled(value int, tenant string, group metapb.Group) {
	switch group {
	case metapb.TenantInputGroup:
		inputEventHandledCounter.WithLabelValues(tenant).Add(float64(value))
	case metapb.TenantOutputGroup:
		outputEventHandledCounter.WithLabelValues(tenant).Add(float64(value))
	}
}

// IcrStorageFailed storage failed
func IcrStorageFailed() {
	storageFailedCounter.Inc()
}
