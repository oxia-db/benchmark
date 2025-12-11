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
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/logging"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/spf13/cobra"
)

var (
	driverCfgPath    string
	workloadsCfgPath string
	metricsAddr      string

	cmd = &cobra.Command{
		Use:   "oxia-benchmark",
		Short: "Distributed KV load generator",
		RunE:  runBenchmark,
	}
)

func init() {
	cmd.Flags().StringVar(&driverCfgPath, "driver-config", "", "Path to driver config YAML")
	cmd.Flags().StringVar(&workloadsCfgPath, "workloads", "", "Path to workload YAML")
	_ = cmd.MarkFlagRequired("driver-config")
	_ = cmd.MarkFlagRequired("workloads")

	cmd.Flags().StringVarP(&metricsAddr, "metrics-addr", "m", fmt.Sprintf("0.0.0.0:%d", constant.DefaultMetricsPort), "Metrics service bind address")
}

func runBenchmark(*cobra.Command, []string) error {
	logging.ConfigureLogger()
	log := slog.With()

	process.PprofBindAddress = "127.0.0.1:6060"
	process.PprofEnable = true
	profiling := process.RunProfiling()
	defer profiling.Close()

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
	wls, err := runner2.Load(workloadsCfgPath)
	if err != nil {
		return err
	}
	log.Info("Load workloads configuration", slog.Any("workloadConfig", wls))
	drv, err := drivers2.Build(driverConf)
	if err != nil {
		return err
	}

	defer drv.Close()

	metadata := wls.Metadata
	for idx := range wls.Items {
		workload := wls.Items[idx]
		log.Info("Starting workload ", slog.Any("workload", workload))
		_ = backoff.RetryNotify(func() error {
			return runner2.Run(metadata, &workload, drv)
		}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
			log.Error("Workflow pipeline interrupted by error. ", slog.Any("workload", workload), slog.Any("err", err), slog.Any("retry-after", duration))
		})
		log.Info("Finish workload, try next one.", slog.Any("workload", workload))
	}
	log.Info("All workload finished. ")
	if !wls.ExitWhenFinish {
		waiter := sync.WaitGroup{}
		waiter.Add(1)
		waiter.Wait()
	}
	return nil
}

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
