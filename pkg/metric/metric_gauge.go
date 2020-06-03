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

	inputQueueMaxGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "queue",
			Name:      "input_max",
			Help:      "Value of input event queue max offset.",
		}, []string{"tenant", "partition"})

	inputQueueRemovedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "queue",
			Name:      "input_removed",
			Help:      "Value of input event queue removed offset.",
		}, []string{"tenant", "partition"})

	inputQueueConsumerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "queue",
			Name:      "input_consumer",
			Help:      "Value of consumer already completed offset.",
		}, []string{"tenant", "partition", "consumer"})

	outputQueueMaxGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "queue",
			Name:      "output_max",
			Help:      "Value of output event queue max offset.",
		}, []string{"tenant", "partition"})

	outputQueueRemovedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "queue",
			Name:      "output_removed",
			Help:      "Value of output event queue removed offset.",
		}, []string{"tenant", "partition"})

	outputQueueConsumerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "busybee",
			Subsystem: "queue",
			Name:      "output_consumer",
			Help:      "Value of consumer already completed offset.",
		}, []string{"tenant", "partition", "consumer"})
)

// SetEventQueueSize set event queue size
func SetEventQueueSize(max, removed uint64, tenant string, partition string, group metapb.Group) {
	switch group {
	case metapb.TenantInputGroup:
		inputQueueMaxGauge.WithLabelValues(tenant, partition).Set(float64(max))
		inputQueueRemovedGauge.WithLabelValues(tenant, partition).Set(float64(removed))
	case metapb.TenantOutputGroup:
		outputQueueMaxGauge.WithLabelValues(tenant, partition).Set(float64(max))
		outputQueueRemovedGauge.WithLabelValues(tenant, partition).Set(float64(removed))
	}
}

// SetQueueConsumerCompleted set completed offset
func SetQueueConsumerCompleted(value uint64, tenant, partition, consumer string, group metapb.Group) {
	switch group {
	case metapb.TenantInputGroup:
		inputQueueConsumerGauge.WithLabelValues(tenant, partition, consumer).Set(float64(value))
	case metapb.TenantOutputGroup:
		outputQueueConsumerGauge.WithLabelValues(tenant, partition, consumer).Set(float64(value))
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
