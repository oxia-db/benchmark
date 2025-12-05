// Copyright 2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log/slog"
	"os"
	drivers2 "oxia-benchmark/drivers"
	runner2 "oxia-benchmark/runner"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/logging"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/spf13/cobra"
)

var (
	driverCfgPath   string
	workloadCfgPath string
	metricsAddr     string

	cmd = &cobra.Command{
		Use:   "oxia-benchmark",
		Short: "Distributed KV load generator",
		RunE:  runBenchmark,
	}
)

func init() {
	cmd.Flags().StringVar(&driverCfgPath, "driver-config", "", "Path to driver config YAML")
	cmd.Flags().StringVar(&workloadCfgPath, "workload", "", "Path to workload YAML")
	_ = cmd.MarkFlagRequired("driver-config")
	_ = cmd.MarkFlagRequired("workload")

	cmd.Flags().StringVarP(&metricsAddr, "metrics-addr", "m", fmt.Sprintf("0.0.0.0:%d", constant.DefaultMetricsPort), "Metrics service bind address")
}

func runBenchmark(cmd *cobra.Command, args []string) error {
	logging.ConfigureLogger()
	log := slog.With()

	metrics, err := metric.Start(metricsAddr)
	if err != nil {
		return err
	}
	defer metrics.Close()

	driverConf, err := drivers2.LoadConfig(driverCfgPath)
	if err != nil {
		return err
	}
	log.Info("Load driver configuration", slog.Any("driverConfig", driverConf))
	wl, err := runner2.Load(workloadCfgPath)
	if err != nil {
		return err
	}
	log.Info("Load workload configuration", slog.Any("workloadConfig", wl))
	drv, err := drivers2.Build(driverConf)
	if err != nil {
		return err
	}

	defer drv.Close()
	return runner2.Run(wl, drv)
}

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
