/*
 * Copyright 2025 The Oxia Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.oxia.benchmark;

import io.oxia.benchmark.driver.DriverConfig;
import io.oxia.benchmark.driver.DriverFactory;
import io.oxia.benchmark.driver.KVStoreDriver;
import io.oxia.benchmark.runner.BenchmarkRunner;
import io.oxia.benchmark.runner.Workload;
import io.oxia.benchmark.runner.Workloads;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "oxia-benchmark",
    mixinStandardHelpOptions = true,
    description = "Distributed KV load generator")
public class BenchmarkMain implements Callable<Integer> {

  private static final Logger log = LoggerFactory.getLogger(BenchmarkMain.class);

  @Option(
      names = "--driver-config",
      required = true,
      description = "Path to driver config YAML")
  private Path driverConfigPath;

  @Option(
      names = "--workloads",
      required = true,
      description = "Path to workload YAML")
  private Path workloadsPath;

  @Option(
      names = {"-m", "--metrics-addr"},
      defaultValue = "0.0.0.0:8080",
      description = "Metrics service bind address (default: ${DEFAULT-VALUE})")
  private String metricsAddr;

  @Override
  public Integer call() {
    try {
      // Start Prometheus metrics server
      String[] parts = metricsAddr.split(":");
      int port = Integer.parseInt(parts[parts.length - 1]);
      HTTPServer metricsServer =
          HTTPServer.builder().port(port).buildAndStart();
      log.info("Serving Prometheus metrics at http://localhost:{}/metrics", port);

      DriverConfig driverConf = DriverConfig.load(driverConfigPath);
      log.info("Loaded driver configuration: {}", driverConf);

      Workloads wls = Workloads.load(workloadsPath);
      log.info("Loaded workloads configuration");

      try (KVStoreDriver driver = DriverFactory.build(driverConf)) {
        for (int i = 0; i < wls.items().size(); i++) {
          Workload workload = wls.items().get(i);
          log.info("Starting workload {}", workload);

          boolean success = false;
          int maxRetries = 5;
          for (int attempt = 0; attempt < maxRetries && !success; attempt++) {
            try {
              new BenchmarkRunner(workload, driver).run();
              success = true;
            } catch (Exception e) {
              log.error(
                  "Workload interrupted by error, retrying (attempt {}/{})",
                  attempt + 1,
                  maxRetries,
                  e);
              Thread.sleep(Math.min(1000L * (1 << attempt), 30_000));
            }
          }

          log.info("Finished workload, moving to next.");
        }

        log.info("All workloads finished.");

        if (!wls.exitWhenFinish()) {
          log.info("Waiting for signal to exit (SIGINT/SIGTERM)...");
          Thread.currentThread().join();
        }
      }

      metricsServer.close();
      return 0;
    } catch (Exception e) {
      log.error("Benchmark failed", e);
      return 1;
    }
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new BenchmarkMain()).execute(args);
    System.exit(exitCode);
  }
}
