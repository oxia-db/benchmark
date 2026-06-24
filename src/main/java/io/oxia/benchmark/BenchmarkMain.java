/*
 * Copyright © 2025 The Oxia Authors
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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxia.benchmark.driver.DriverConfig;
import io.oxia.benchmark.driver.DriverFactory;
import io.oxia.benchmark.driver.KVStoreDriver;
import io.oxia.benchmark.report.ReportCommand;
import io.oxia.benchmark.report.WorkloadResult;
import io.oxia.benchmark.runner.BenchmarkRunner;
import io.oxia.benchmark.runner.Workload;
import io.oxia.benchmark.runner.Workloads;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;
import lombok.CustomLog;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@CustomLog
@Command(
        name = "oxia-benchmark",
        mixinStandardHelpOptions = true,
        description = "Distributed KV load generator",
        subcommands = {ReportCommand.class})
public class BenchmarkMain implements Callable<Integer> {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Option(names = "--driver-config", description = "Path to driver config YAML")
    private Path driverConfigPath;

    @Option(names = "--workloads", description = "Path to workload YAML")
    private Path workloadsPath;

    @Option(
            names = {"-m", "--metrics-addr"},
            defaultValue = "0.0.0.0:8080",
            description = "Metrics service bind address (default: ${DEFAULT-VALUE})")
    private String metricsAddr;

    @Option(
            names = "--results-dir",
            description = "Directory to write per-workload result files (enables the report pipeline)")
    private Path resultsDir;

    @Option(
            names = "--instance-id",
            description = "This worker's id in aggregated reports (default: $HOSTNAME or hostname)")
    private String instanceId;

    @Override
    public Integer call() {
        if (driverConfigPath == null || workloadsPath == null) {
            log.error("--driver-config and --workloads are required to run a benchmark");
            return 2;
        }
        try {
            // Start Prometheus metrics server
            String[] parts = metricsAddr.split(":");
            int port = Integer.parseInt(parts[parts.length - 1]);
            HTTPServer metricsServer = HTTPServer.builder().port(port).buildAndStart();
            log.infof("Serving Prometheus metrics at http://localhost:%d/metrics", port);

            DriverConfig driverConf = DriverConfig.load(driverConfigPath);
            log.info().attr("driverConfig", driverConf).log("Loaded driver configuration");

            Workloads wls = Workloads.load(workloadsPath);
            log.info("Loaded workloads configuration");

            String iid = resultsDir != null ? instanceId() : null;

            try (KVStoreDriver driver = DriverFactory.build(driverConf)) {
                for (int i = 0; i < wls.items().size(); i++) {
                    Workload workload = wls.items().get(i);
                    log.info().attr("workload", workload).log("Starting workload");

                    boolean success = false;
                    int maxRetries = 5;
                    for (int attempt = 0; attempt < maxRetries && !success; attempt++) {
                        try {
                            WorkloadResult result = new BenchmarkRunner(workload, driver).run();
                            success = true;
                            if (resultsDir != null) {
                                result.index = i;
                                result.instanceId = iid;
                                writeResult(result);
                            }
                        } catch (Exception e) {
                            log.error()
                                    .attr("attempt", attempt + 1)
                                    .attr("maxRetries", maxRetries)
                                    .exception(e)
                                    .log("Workload interrupted by error, retrying");
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
            log.error().exception(e).log("Benchmark failed");
            return 1;
        }
    }

    private void writeResult(WorkloadResult r) throws IOException {
        Files.createDirectories(resultsDir);
        String line = JSON.writeValueAsString(r) + System.lineSeparator();
        Files.writeString(
                resultsDir.resolve(r.instanceId + ".jsonl"),
                line,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    private String instanceId() {
        if (instanceId != null && !instanceId.isEmpty()) {
            return instanceId;
        }
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isEmpty()) {
            return env;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "worker";
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new BenchmarkMain()).execute(args);
        System.exit(exitCode);
    }
}
