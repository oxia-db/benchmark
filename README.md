# Oxia Benchmark

Oxia Benchmark is a versatile and extensible framework for benchmarking distributed key-value stores. It is designed to facilitate performance comparisons between systems like [Oxia](https://github.com/oxia-db/oxia), [etcd](https://etcd.io/), and [Apache Zookeeper](https://zookeeper.apache.org/).

## Features

- **Service-Agnostic:** Easily compare the performance of different key-value stores.
- **Extensible:** A plugin-based architecture for drivers makes it simple to add support for new systems.
- **Configurable Workloads:** Define a variety of benchmark scenarios with customizable parameters to simulate different access patterns.
- **Kubernetes-Native:** Deploy and manage the entire benchmark environment on Kubernetes using the provided Helm charts.
- **Prometheus Exporter:** Monitor benchmark performance in real-time with the built-in Prometheus metrics exporter.

## Getting Started

### Prerequisites

- Go 1.20+
- Docker (for building the container image)
- `make`

## Local Usage

### Build

To build the benchmark tool and the driver plugins, run the following command:

```bash
make build
```

This will create the `oxia-benchmark` binary and the driver plugins in the `build/` directory.

### Run

To execute a benchmark, you need to provide a driver configuration file and a workload configuration file.

```bash
./build/oxia-benchmark --driver-config conf/driver-oxia.yaml --workloads conf/workload-mixed.yaml
```

To clean up the build artifacts, run:

```bash
make clean
```

## Kubernetes Deployment

The `charts` directory contains a Helm chart for deploying the benchmark to a Kubernetes cluster. The `benchmark-stack` chart can deploy the entire benchmark environment, including the system under test (Oxia, etcd, or Zookeeper).

### Chart Configuration

The chart is configured using values files. There are several pre-configured values files for different scenarios:

- `values-3-server.yaml`: Deploys a 3-server cluster of each system.
- `values-6-server.yaml`: Deploys a 6-server cluster of each system.
- `values-12-server.yaml`: Deploys a 12-server cluster of each system.
- `values-3-server-latency.yaml`: Deploys a 3-server cluster of each system and configures the benchmark for latency testing.

You can customize these values files to match your requirements. For example, to disable the deployment of a specific system, set the `enabled` flag to `false` in the corresponding section.

### Deploying the Benchmark

To deploy the benchmark, use the `helm` command.

Example for deploying the Oxia benchmark with a 3-server cluster:

```bash
helm install benchmark charts/benchmark-stack \
  -f charts/benchmark-stack/values-3-server.yaml \
  --set zookeeper.enabled=false \
  --set etcd.enabled=false
```

This command will deploy a 3-node Oxia cluster and the benchmark workers configured to connect to it. The Zookeeper and etcd deployments will be disabled.

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

### Workload Configuration

The workload configuration defines the benchmark scenarios. See `conf/workload-mixed.yaml` for an example.

Key workload parameters:

- `readRatio`: The proportion of read operations (0.0 for write-only, 1.0 for read-only).
- `keyspaceSize`: The number of keys to use in the benchmark.
- `keyDistribution`: The distribution of keys (e.g., `order`, `zipf`, `uniform`).
- `valueSize`: The size of the values in bytes.
- `targetRate`: The desired rate of operations per second.
- `duration`: The duration of the workload.
- `parallelism`: The number of concurrent clients.

## Monitoring

The benchmark tool exposes Prometheus metrics on port `9090` by default. You can scrape these metrics to monitor the performance of the benchmark in real-time. The metrics address can be changed with the `--metrics-addr` flag.

## Extending the Benchmark

To add support for a new key-value store, you can create a new driver.

1.  Create a new directory in the `drivers/` directory.
2.  Implement the `drivers.Driver` interface in your new driver.
3.  Add a build target to the `Makefile` to build your driver as a Go plugin.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.