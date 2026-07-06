# Oxia Benchmark

Oxia Benchmark is a versatile and extensible framework for benchmarking distributed key-value stores. It is designed to facilitate performance comparisons between systems like [Oxia](https://github.com/oxia-db/oxia), [etcd](https://etcd.io/), [Apache ZooKeeper](https://zookeeper.apache.org/), [Consul](https://www.consul.io/), [Redis](https://redis.io/), and [TiKV](https://tikv.org/).

## Features

- **Service-Agnostic:** Compare the performance of different key-value stores using the same workloads and methodology, ensuring fair apples-to-apples comparisons.
- **Coordinated Omission Correction:** Latency measurements use intended send times rather than actual dispatch times, avoiding the [coordinated omission](https://www.scylladb.com/2021/04/22/on-coordinated-omission/) problem that plagues most benchmarking tools. When the system under test slows down, the queuing delay is correctly reflected in percentile latencies.
- **Distributed Load Generation:** Scale beyond single-node bottlenecks by deploying multiple worker pods on Kubernetes, each generating load against the same cluster. Each worker independently tracks its own target rate and latency measurements.
- **YCSB-Style Workloads:** Run industry-standard [YCSB](https://github.com/brianfrankcooper/YCSB) workload scenarios (A, C, D, X) with configurable read/write ratios, key distributions (uniform, zipf, sequential), and key space sizes.
- **Extensible Plugin Architecture:** Add support for new key-value stores by implementing the `KVStoreDriver` Java interface, without modifying the core benchmark code.
- **Kubernetes-Native Deployment:** Deploy the complete benchmark environment — including the systems under test — using Helm charts with pre-configured cluster topologies (3, 6, 12 servers).
- **Exact Aggregated Results:** Each worker records latency as an HdrHistogram; the `report` subcommand merges them across workers for exact aggregated percentiles and an HTML report.
- **Rate-Limited Traffic Generation:** Per-worker token bucket rate limiting ensures precise control over target throughput, independent of system response times.

## Supported Backends

The benchmark drives each system through a common `KVStoreDriver` interface, selected via a driver config in `conf/` and deployable via the Helm chart:

| Backend | Type | Driver config | Java client |
|---------|------|---------------|-------------|
| [Oxia](https://github.com/oxia-db/oxia) | Scalable metadata store (sharded, replicated) | `conf/driver-oxia.yaml` | `oxia-client` |
| [etcd](https://etcd.io/) | Distributed KV / coordination (Raft) | `conf/driver-etcd.yaml` | `jetcd` |
| [Apache ZooKeeper](https://zookeeper.apache.org/) | Coordination service (ZAB) | `conf/driver-zookeeper.yaml` | `zookeeper` |
| [Consul](https://www.consul.io/) | KV store (HTTP API) | `conf/driver-consul.yaml` | JDK `HttpClient` |
| [Redis](https://redis.io/) | In-memory KV (speed-ceiling baseline) | `conf/driver-redis.yaml` | `lettuce` |
| [TiKV](https://tikv.org/) | Distributed transactional KV (Raft + PD) | `conf/driver-tikv.yaml` | `tikv-client-java` |

Add a new backend by implementing the `KVStoreDriver` interface — see [Extending the Benchmark](#extending-the-benchmark).

## Getting Started

### Prerequisites

- Java 17+
- Docker (for building the container image)
- `make` (optional, for convenience targets)

## Local Usage

### Build

To build the benchmark tool as a shadow JAR (fat JAR with all dependencies):

```bash
make build
```

Or using Gradle directly:

```bash
./gradlew shadowJar
```

This creates the `oxia-benchmark-*-all.jar` in `build/libs/`.

### Lint

To check code formatting:

```bash
make lint
```

### Test

To run all unit tests:

```bash
make test
```

### Run

To execute a benchmark, you need to provide a driver configuration file and a workload configuration file.

```bash
java -jar build/libs/oxia-benchmark-*-all.jar --driver-config conf/driver-oxia.yaml --workloads conf/workload-ycsb.yaml
```

Example output:

```
2025/06/15 10:00:00 INFO Running workload {readRatio=0.0, keyspaceSize=100000, keyDistribution=order, valueSize=64, targetRate=40000, duration=10s, parallelism=1}
2025/06/15 10:00:10 INFO Stats - Total ops: 39850.2 ops/s - Failed ops:    0.0 ops/s
                Write ops 39850.2 w/s  Latency ms: 50%   0.3 - 95%   0.8 - 99%   1.2 - 99.9%   2.5 - max    5.1
                Read  ops    0.0 r/s  Latency ms: 50%   0.0 - 95%   0.0 - 99%   0.0 - 99.9%   0.0 - max    0.0
```

To clean up build artifacts:

```bash
make clean
```

### Collecting and reporting results

Pass `--results-dir <dir>` to write a per-workload result file for each worker — one JSON line per
workload, with the latency distribution serialized as a compressed HdrHistogram:

```bash
java -jar build/libs/oxia-benchmark-*-all.jar \
  --driver-config conf/driver-oxia.yaml --workloads conf/workload-ycsb.yaml \
  --results-dir results --instance-id "$(hostname)"
```

Then aggregate the results into per-workload metrics — merging the histograms across all workers for
**exact** percentiles (averaging per-worker percentiles is not statistically valid) — and render a
report:

```bash
java -jar build/libs/oxia-benchmark-*-all.jar report --results-dir results --out report
```

This writes three files to `report/`:

- `summary.csv` and `summary.json` — the raw per-workload metrics (throughput, p50/p95/p99/p99.9/max
  for reads and writes, failures), for building your own charts.
- `report.html` — a self-contained chart of throughput, latency percentiles, and the
  HdrHistogram-style latency-by-percentile distribution per workload.
- `workload-<i>-<write|read>.hgrm` — HdrHistogram percentile-distribution files, for the official
  plotter at <https://hdrhistogram.github.io/HdrHistogram/>.

For a distributed run, gather every worker's `*.jsonl` file into a single directory (e.g. via a
shared volume or `kubectl cp`) before running `report`.

## Kubernetes Deployment

The `charts` directory contains a Helm chart for deploying the benchmark to a Kubernetes cluster. The `benchmark-stack` chart deploys the entire benchmark environment: the systems under test (see [Supported Backends](#supported-backends)) and the benchmark workers that generate load against them.

### Architecture

In a Kubernetes deployment, the benchmark runs as a distributed system:

- **Workers** are deployed as Kubernetes Deployments with configurable replica counts. Each worker pod runs an independent instance of the benchmark, generating traffic at the configured target rate. For example, 6 worker replicas each targeting 40,000 ops/s produce an aggregate load of 240,000 ops/s against the cluster.
- **Workloads** are shared across all workers via a ConfigMap, ensuring consistent test scenarios.
- **Driver configs** are per-worker, allowing simultaneous benchmarking of different systems (e.g., Oxia, etcd, ZooKeeper) in the same deployment.
- Each worker writes its own result file, which the `report` subcommand merges for a cluster-wide view.

### Chart Configuration

The chart is configured using values files. There are several pre-configured values files for different scenarios:

- `values-3-server.yaml`: Deploys a 3-server cluster of each system.
- `values-6-server.yaml`: Deploys a 6-server cluster of each system.
- `values-12-server.yaml`: Deploys a 12-server cluster of each system.
- `values-3-server-latency.yaml`: Deploys a 3-server cluster of each system and configures the benchmark for latency testing.
- `values-consul.yaml`, `values-redis.yaml`, `values-tikv.yaml`: Deploy and benchmark a single backend (Consul, Redis, or TiKV) using its official upstream image.

You can customize these values files to match your requirements. For example, to disable the deployment of a specific system, set the `enabled` flag to `false` in the corresponding section.

The **workload** to run is not part of the values files — it is supplied at install time from a file under `conf/` via `--set-file workloadsYaml=conf/<file>.yaml`, so workload definitions live in one place and are never duplicated. Use `conf/workload-ycsb.yaml` (the standard YCSB-style set), `conf/workload-test.yaml` (a quick single-workload smoke), `conf/workload-comparison.yaml` (the cross-driver write/read pair), or your own.

### Deploying the Benchmark

To deploy the benchmark, use the `helm` command.

Example for deploying the Oxia benchmark with a 3-server cluster:

```bash
helm install benchmark charts/benchmark-stack \
  -f charts/benchmark-stack/values-3-server.yaml \
  --set-file workloadsYaml=conf/workload-ycsb.yaml \
  --set zookeeper.enabled=false \
  --set etcd.enabled=false
```

This command will deploy a 3-node Oxia cluster and the benchmark workers configured to connect to it. The ZooKeeper and etcd deployments will be disabled.

You can then see the benchmark driver logs by running:

```bash
kubectl logs -f -lapp.kubernetes.io/name=oxia-benchmark-driver
```

## Configuration

### Driver Configuration

The driver configuration specifies the target system to benchmark. Configuration files for the supported drivers are located in the `conf` directory:

- `conf/driver-oxia.yaml`
- `conf/driver-etcd.yaml`
- `conf/driver-zookeeper.yaml`
- `conf/driver-consul.yaml`
- `conf/driver-redis.yaml`
- `conf/driver-tikv.yaml`

### Workload Configuration

The workload configuration defines the benchmark scenarios. See `conf/workload-ycsb.yaml` for an example.

Key workload parameters:

- `readRatio`: The proportion of read operations (0.0 for write-only, 1.0 for read-only).
- `keyspaceSize`: The number of keys to use in the benchmark.
- `keyDistribution`: The distribution of keys (`order` for sequential, `zipf` for hotspot, `uniform` for random).
- `valueSize`: The size of the values in bytes.
- `targetRate`: The desired rate of operations per second per worker.
- `duration`: The duration of the workload (e.g., `10s`, `15m`).
- `parallelism`: The number of concurrent clients within each worker.

Multiple workloads can be defined in a single file and will be executed sequentially.

## Results

Per-worker latency/throughput are collected as HdrHistograms and written to the results directory (`--results-dir`); the `report` subcommand aggregates them into `report.html`. See "Cross-driver comparison".

## Extending the Benchmark

To add support for a new key-value store:

1.  Create a new class implementing the `KVStoreDriver` interface in `src/main/java/io/oxia/benchmark/driver/`.
2.  Register the driver in `DriverFactory.java`.
3.  Add any required client library dependencies to `build.gradle.kts`.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
