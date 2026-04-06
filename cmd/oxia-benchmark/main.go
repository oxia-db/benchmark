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
	"net"
	"net/http"
	"os"
	"os/signal"
	drivers2 "oxia-benchmark/drivers"
	runner2 "oxia-benchmark/runner"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/oxia-db/oxia/common/process"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
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

	cmd.Flags().StringVarP(&metricsAddr, "metrics-addr", "m", "0.0.0.0:8080", "Metrics service bind address")
}

func startMetrics(bindAddress string) (*http.Server, error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	_ = provider

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", bindAddress, err)
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: time.Second,
	}

	port := listener.Addr().(*net.TCPAddr).Port
	slog.Info(fmt.Sprintf("Serving Prometheus metrics at http://localhost:%d/metrics", port))

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			slog.Error("Failed to serve metrics", slog.Any("error", err))
		}
	}()

	return server, nil
}

func runBenchmark(*cobra.Command, []string) error {
	log := slog.With()

	process.PprofBindAddress = "127.0.0.1:6060"
	process.PprofEnable = true
	profiling := process.RunProfiling()
	defer profiling.Close()

	metricsServer, err := startMetrics(metricsAddr)
	if err != nil {
		return err
	}
	defer metricsServer.Close()

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
	log.Info("All workloads finished.")
	if !wls.ExitWhenFinish {
		log.Info("Waiting for signal to exit (SIGINT/SIGTERM)...")
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Info("Received signal, shutting down.", slog.String("signal", sig.String()))
	}
	return nil
}

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
