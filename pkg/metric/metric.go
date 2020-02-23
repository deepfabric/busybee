package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	registry = prometheus.NewRegistry()
)

func init() {
	registry.MustRegister(inputEventAddedCounter)
	registry.MustRegister(outputEventAddedCounter)
	registry.MustRegister(inputEventHandledCounter)
	registry.MustRegister(outputEventHandledCounter)

	registry.MustRegister(inputEventQueueSizeGauge)
	registry.MustRegister(outputEventQueueSizeGauge)
}
