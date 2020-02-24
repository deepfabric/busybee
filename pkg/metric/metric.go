package metric

import (
	p "github.com/deepfabric/beehive/metric"
)

func init() {
	p.MustRegister(inputEventAddedCounter)
	p.MustRegister(outputEventAddedCounter)
	p.MustRegister(inputEventHandledCounter)
	p.MustRegister(outputEventHandledCounter)
	p.MustRegister(requestReceivedCounter)
	p.MustRegister(requestResultCounter)

	p.MustRegister(inputEventQueueSizeGauge)
	p.MustRegister(outputEventQueueSizeGauge)
}
