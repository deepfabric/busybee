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
	p.MustRegister(storageFailedCounter)
	p.MustRegister(workerFailedCounter)
	p.MustRegister(userMovedCounter)

	p.MustRegister(inputEventQueueSizeGauge)
	p.MustRegister(outputEventQueueSizeGauge)
	p.MustRegister(workflowShardsCountGauge)
	p.MustRegister(workflowCountGauge)
	p.MustRegister(runnersCountGauge)
}
