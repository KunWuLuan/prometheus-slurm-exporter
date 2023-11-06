/* Copyright 2017 Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"io/ioutil"
	"log"
	"os/exec"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type QueueMetrics struct {
	pending     map[string]float64
	pending_dep map[string]float64
	running     map[string]float64
	suspended   map[string]float64
	cancelled   map[string]float64
	completing  map[string]float64
	completed   map[string]float64
	configuring map[string]float64
	failed      map[string]float64
	timeout     map[string]float64
	preempted   map[string]float64
	node_fail   map[string]float64
}

// Returns the scheduler metrics
func QueueGetMetrics() *QueueMetrics {
	return ParseQueueMetrics(QueueData(), PartitionInfo())
}

func PartitionInfo() []string {
	cmd := exec.Command("sinfo", "--noheader", "-o %P")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	lines := strings.Split(string(out), "\n")
	partitions := make([]string, len(lines))
	for i, line := range lines {
		partitions[i] = strings.TrimSpace(strings.TrimSuffix(line, "*"))
	}
	return partitions
}

func initMap(keys []string) map[string]float64 {
	out := map[string]float64{}
	for _, k := range keys {
		out[k] = 0
	}
	return out
}

func ParseQueueMetrics(input []byte, partition []string) *QueueMetrics {
	var qm QueueMetrics = QueueMetrics{
		pending:     initMap(partition),
		pending_dep: initMap(partition),
		running:     initMap(partition),
		suspended:   initMap(partition),
		cancelled:   initMap(partition),
		completing:  initMap(partition),
		completed:   initMap(partition),
		configuring: initMap(partition),
		failed:      initMap(partition),
		timeout:     initMap(partition),
		preempted:   initMap(partition),
		node_fail:   initMap(partition),
	}
	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if strings.Contains(line, ",") {
			splitted := strings.Split(line, ",")
			state := splitted[1]
			partition := splitted[3]
			switch state {
			case "PENDING":
				qm.pending[partition]++
				if len(splitted) > 2 && splitted[2] == "Dependency" {
					qm.pending_dep[partition]++
				}
			case "RUNNING":
				qm.running[partition]++
			case "SUSPENDED":
				qm.suspended[partition]++
			case "CANCELLED":
				qm.cancelled[partition]++
			case "COMPLETING":
				qm.completing[partition]++
			case "COMPLETED":
				qm.completed[partition]++
			case "CONFIGURING":
				qm.configuring[partition]++
			case "FAILED":
				qm.failed[partition]++
			case "TIMEOUT":
				qm.timeout[partition]++
			case "PREEMPTED":
				qm.preempted[partition]++
			case "NODE_FAIL":
				qm.node_fail[partition]++
			}
		}
	}
	return &qm
}

// Execute the squeue command and return its output
func QueueData() []byte {
	cmd := exec.Command("squeue", "-a", "-r", "-h", "-o %A,%T,%r,%P", "--states=all")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm queue metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewQueueCollector() *QueueCollector {
	return &QueueCollector{
		pending:     prometheus.NewDesc("slurm_queue_pending", "Pending jobs in queue", []string{"partition"}, nil),
		pending_dep: prometheus.NewDesc("slurm_queue_pending_dependency", "Pending jobs because of dependency in queue", []string{"partition"}, nil),
		running:     prometheus.NewDesc("slurm_queue_running", "Running jobs in the cluster", []string{"partition"}, nil),
		suspended:   prometheus.NewDesc("slurm_queue_suspended", "Suspended jobs in the cluster", []string{"partition"}, nil),
		cancelled:   prometheus.NewDesc("slurm_queue_cancelled", "Cancelled jobs in the cluster", []string{"partition"}, nil),
		completing:  prometheus.NewDesc("slurm_queue_completing", "Completing jobs in the cluster", []string{"partition"}, nil),
		completed:   prometheus.NewDesc("slurm_queue_completed", "Completed jobs in the cluster", []string{"partition"}, nil),
		configuring: prometheus.NewDesc("slurm_queue_configuring", "Configuring jobs in the cluster", []string{"partition"}, nil),
		failed:      prometheus.NewDesc("slurm_queue_failed", "Number of failed jobs", []string{"partition"}, nil),
		timeout:     prometheus.NewDesc("slurm_queue_timeout", "Jobs stopped by timeout", []string{"partition"}, nil),
		preempted:   prometheus.NewDesc("slurm_queue_preempted", "Number of preempted jobs", []string{"partition"}, nil),
		node_fail:   prometheus.NewDesc("slurm_queue_node_fail", "Number of jobs stopped due to node fail", []string{"partition"}, nil),
	}
}

type QueueCollector struct {
	pending     *prometheus.Desc
	pending_dep *prometheus.Desc
	running     *prometheus.Desc
	suspended   *prometheus.Desc
	cancelled   *prometheus.Desc
	completing  *prometheus.Desc
	completed   *prometheus.Desc
	configuring *prometheus.Desc
	failed      *prometheus.Desc
	timeout     *prometheus.Desc
	preempted   *prometheus.Desc
	node_fail   *prometheus.Desc
}

func (qc *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- qc.pending
	ch <- qc.pending_dep
	ch <- qc.running
	ch <- qc.suspended
	ch <- qc.cancelled
	ch <- qc.completing
	ch <- qc.completed
	ch <- qc.configuring
	ch <- qc.failed
	ch <- qc.timeout
	ch <- qc.preempted
	ch <- qc.node_fail
}

func (qc *QueueCollector) collect(metric *prometheus.Desc, ch chan<- prometheus.Metric, value map[string]float64) {
	for k, v := range value {
		ch <- prometheus.MustNewConstMetric(metric, prometheus.GaugeValue, v, k)
	}
}

func (qc *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	qm := QueueGetMetrics()
	qc.collect(qc.pending, ch, qm.pending)
	qc.collect(qc.pending_dep, ch, qm.pending_dep)
	qc.collect(qc.running, ch, qm.running)
	qc.collect(qc.suspended, ch, qm.suspended)
	qc.collect(qc.cancelled, ch, qm.cancelled)
	qc.collect(qc.completing, ch, qm.completing)
	qc.collect(qc.completed, ch, qm.completed)
	qc.collect(qc.configuring, ch, qm.configuring)
	qc.collect(qc.failed, ch, qm.failed)
	qc.collect(qc.timeout, ch, qm.timeout)
	qc.collect(qc.preempted, ch, qm.preempted)
	qc.collect(qc.node_fail, ch, qm.node_fail)
}
