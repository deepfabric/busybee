package metric

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/prometheus/client_golang/prometheus"
)

var (
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
