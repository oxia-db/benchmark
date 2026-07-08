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
import io.oxia.benchmark.driver.session.SessionDriver;
import io.oxia.benchmark.report.ReportCommand;
import io.oxia.benchmark.report.SessionReportCommand;
import io.oxia.benchmark.report.WorkloadResult;
import io.oxia.benchmark.runner.BenchmarkRunner;
import io.oxia.benchmark.runner.Workload;
import io.oxia.benchmark.runner.Workloads;
import io.oxia.benchmark.session.SessionExperiment;
import io.oxia.benchmark.session.SessionExperimentRunner;
import io.oxia.benchmark.session.SessionExperiments;
import io.oxia.benchmark.session.SessionResult;
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
        subcommands = {ReportCommand.class, SessionReportCommand.class})
public class BenchmarkMain implements Callable<Integer> {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Option(names = "--driver-config", description = "Path to driver config YAML")
    private Path driverConfigPath;

    @Option(names = "--workloads", description = "Path to workload YAML")
    private Path workloadsPath;

    @Option(
            names = "--session-experiments",
            description = "Path to session/ephemeral experiment YAML (runs S1-S4 instead of workloads)")
    private Path sessionExperimentsPath;

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
        if (driverConfigPath == null || (workloadsPath == null && sessionExperimentsPath == null)) {
            log.error(
                    "--driver-config and one of --workloads / --session-experiments are required to run a"
                            + " benchmark");
            return 2;
        }
        try {
            DriverConfig driverConf = DriverConfig.load(driverConfigPath);
            log.info().attr("driverConfig", driverConf).log("Loaded driver configuration");

            String iid = resultsDir != null ? instanceId() : null;

            if (sessionExperimentsPath != null) {
                return runSessionExperiments(driverConf, iid);
            }

            Workloads wls = Workloads.load(workloadsPath);
            log.info("Loaded workloads configuration");

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

            return 0;
        } catch (Exception e) {
            log.error().exception(e).log("Benchmark failed");
            return 1;
        }
    }

    /**
     * Session/ephemeral suite (S1-S4). Mirrors the workload path but drives a {@link SessionDriver}.
     */
    private int runSessionExperiments(DriverConfig driverConf, String iid) throws IOException {
        SessionExperiments exps = SessionExperiments.load(sessionExperimentsPath);
        log.info("Loaded session experiments configuration");

        String label = driverConf.label();

        try (SessionDriver driver = DriverFactory.buildSession(driverConf)) {
            for (int i = 0; i < exps.items().size(); i++) {
                SessionExperiment exp = exps.items().get(i);
                log.info().attr("experiment", exp).log("Starting session experiment");

                // No retry: a half-run experiment leaves sessions/keys behind on the server, and a
                // rerun against that residue would skew the numbers. Log and move to the next one.
                try {
                    SessionResult result = new SessionExperimentRunner(exp, driver, iid).run();
                    if (label != null && !label.isEmpty()) {
                        result.driver = label; // same role as LabeledDriver on the workload path
                    }
                    if (resultsDir != null) {
                        result.index = i;
                        result.instanceId = iid;
                        writeSessionResult(result);
                    }
                } catch (Exception e) {
                    log.error().exception(e).log("Session experiment failed, moving to next");
                }
            }

            log.info("All session experiments finished.");
            if (!exps.exitWhenFinish()) {
                log.info("Waiting for signal to exit (SIGINT/SIGTERM)...");
                Thread.currentThread().join();
            }
        } catch (Exception e) {
            log.error().exception(e).log("Session experiments failed");
            return 1;
        }
        return 0;
    }

    private void writeSessionResult(SessionResult r) throws IOException {
        Files.createDirectories(resultsDir);
        String line = JSON.writeValueAsString(r) + System.lineSeparator();
        // Distinct suffix so the workload `report` command skips these SessionResult lines.
        Files.writeString(
                resultsDir.resolve(r.instanceId + "-session.jsonl"),
                line,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
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
