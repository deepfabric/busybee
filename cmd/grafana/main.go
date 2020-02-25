package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/K-Phoen/grabana"
	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/graph"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/table"
	"github.com/K-Phoen/grabana/target/prometheus"
	"github.com/K-Phoen/grabana/variable/interval"
	"github.com/deepfabric/beehive/grafana"
	"github.com/fagongzi/log"
)

var (
	addr   = flag.String("grafana", "http://127.0.0.1:3000", "Grafana api address")
	key    = flag.String("key", "eyJrIjoiZ3BnN3NxRjVFcnVoSGkxZTJXT2lWQVhJZ0dpNXR6QWIiLCJuIjoidGVzdCIsImlkIjoxfQ==", "Grafana api key")
	folder = flag.String("folder", "Busybee", "Busybee dashboard folder")
	ds     = flag.String("ds", "Prometheus", "Prometheus datasource name")
)

func main() {
	flag.Parse()

	err := createBeehive()
	if err != nil {
		log.Fatalf("create beehive dashboard failed with %+v", err)
	}
	log.Infof("create beehive dashboard successful")

	err = createBusybee()
	if err != nil {
		log.Fatalf("create busybee dashboard failed with %+v", err)
	}
	log.Infof("create busybee dashboard successful")
	os.Exit(0)
}

func createBeehive() error {
	db := grafana.NewDashboardCreator(*addr, *key, *ds)
	return db.Create()
}

func createBusybee() error {
	cli := grabana.NewClient(http.DefaultClient, *addr, *key)
	f, err := createFolder(cli)
	if err != nil {
		return err
	}

	return createDashboard(cli, f)
}

func createFolder(cli *grabana.Client) (*grabana.Folder, error) {
	f, err := cli.GetFolderByTitle(context.Background(), *folder)
	if err != nil && err != grabana.ErrFolderNotFound {
		return nil, err
	}

	if f == nil {
		f, err = cli.CreateFolder(context.Background(), *folder)
		if err != nil {
			return nil, err
		}
	}

	return f, nil
}

func createDashboard(cli *grabana.Client, f *grabana.Folder) error {
	db := grabana.NewDashboardBuilder("Busybee Status",
		grabana.AutoRefresh("5s"),
		grabana.Tags([]string{"generated"}),
		grabana.VariableAsInterval(
			"interval",
			interval.Values([]string{"30s", "1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		),
		workflowRow(),
		requestRow(),
		errorRow(),
		eventRow())

	_, err := cli.UpsertDashboard(context.Background(), f, db)
	return err
}

func requestRow() grabana.DashboardBuilderOption {
	height := 400
	return grabana.Row(
		"Request status",
		withGraph("Requests received", height, 6,
			"sum(rate(busybee_api_request_received_total[$interval]))",
			"Received"),
		withGraph("Request result", height, 6,
			"sum(rate(busybee_api_request_result_total[$interval])) by (type, result)",
			"{{type}}({{result}})"),
	)
}

func errorRow() grabana.DashboardBuilderOption {
	height := 300
	return grabana.Row(
		"Error status",
		withGraph("Storage Error", height, 6,
			"sum(rate(busybee_storage_failed_total[$interval]))",
			"Error"),
		withGraph("Worker Error", height, 6,
			"sum(rate(busybee_engine_worker_failed_total[$interval]))",
			"Error"),
	)
}

func eventRow() grabana.DashboardBuilderOption {
	height := 400
	return grabana.Row(
		"Tenant input and output queue status",
		withGraph("Input event added", height, 3,
			"sum(rate(busybee_event_input_total[$interval]))",
			"Input added"),
		withGraph("Input event handled", height, 3,
			"sum(rate(busybee_event_input_handled_total[$interval]))",
			"Input handled"),
		withGraph("Output notify added", height, 3,
			"sum(rate(busybee_event_output_total[$interval]))",
			"Output added"),
		withGraph("Output notify handled", height, 3,
			"sum(rate(busybee_event_output_handled_total[$interval]))",
			"Output handled"),
	)
}

func workflowRow() grabana.DashboardBuilderOption {
	height := 400
	return grabana.Row(
		"Tenant workflow status",
		withTable("workflow count", height, 3,
			"sum(busybee_engine_workflow_total) by (status)",
			"{{ status }}"),
		withTable("workflow shard count", height, 3,
			"sum(busybee_engine_workflow_shard_total) by (status)",
			"{{ status }}"),

		withTable("Input queue size", height, 3,
			"sum(busybee_event_input_queue_size) by (tenant)",
			"tenant-{{ tenant }}"),
		withTable("Output queue size", height, 3,
			"sum(busybee_event_output_queue_size) by (tenant)",
			"tenant-{{ tenant }}"),
	)
}

func withGraph(title string, height int, span float32, pql string, legend string, opts ...axis.Option) row.Option {
	return row.WithGraph(
		title,
		graph.Span(span),
		graph.Height(fmt.Sprintf("%dpx", height)),
		graph.DataSource(*ds),
		graph.WithPrometheusTarget(
			pql,
			prometheus.Legend(legend),
		),
		graph.LeftYAxis(opts...),
	)
}

func withTable(title string, height int, span float32, pql string, legend string) row.Option {
	return row.WithTable(
		title,
		table.Span(span),
		table.Height(fmt.Sprintf("%dpx", height)),
		table.DataSource(*ds),
		table.WithPrometheusTarget(
			pql,
			prometheus.Legend(legend)),
		table.AsTimeSeriesAggregations([]table.Aggregation{
			{Label: "Current", Type: table.Current},
		}),
	)
}
