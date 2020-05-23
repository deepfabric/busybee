package metric

import (
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	workflowCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "engine",
			Name:      "workflow_total",
			Help:      "Total number of workflow instance.",
		}, []string{"status"})

	workflowShardsCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "engine",
			Name:      "workflow_shard_total",
			Help:      "Total number of workflow instance shards.",
		})

	runnersCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "engine",
			Name:      "runner_total",
			Help:      "Total number of runners.",
		})

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

// SetWorkflowCount set workflow count
func SetWorkflowCount(starting, started, stopping, stopped int) {
	workflowCountGauge.WithLabelValues("starting").Set(float64(starting))
	workflowCountGauge.WithLabelValues("running").Set(float64(started))
	workflowCountGauge.WithLabelValues("stopping").Set(float64(stopping))
	workflowCountGauge.WithLabelValues("stopped").Set(float64(stopped))
}

// SetWorkflowShardsCount set workflow shard count
func SetWorkflowShardsCount(value int) {
	workflowShardsCountGauge.Set(float64(value))
}

// SetRunnersCount set runners count
func SetRunnersCount(value int) {
	runnersCountGauge.Set(float64(value))
}
