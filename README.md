# Oxia Benchmark

A service-agnostic key-value store benchmark framework, designed to compare the performance of different distributed coordination systems like Oxia, etcd, and Zookeeper.

## Overview

The benchmark tool provides a flexible framework for generating various workloads against different key-value stores. It uses a plugin-based architecture for drivers, making it extensible to other systems.

- **Drivers**: The `drivers` directory contains the implementations for different systems. Each driver is built as a Go plugin (`.so` file).
- **Workloads**: The benchmark runs a series of workloads defined in a YAML file. These workloads can be configured to simulate different access patterns and load levels.
- **Metrics**: The benchmark exposes Prometheus metrics for monitoring performance.

## Getting Started

### Prerequisites

- Go 1.20+
- Docker (for building the Docker image)

### Build

To build the benchmark tool and the drivers, run:

```bash
make build
```

This will create the `oxia-benchmark` binary and the driver plugins in the `build` directory.

### Run

To run the benchmark, you need to provide a driver configuration file and a workload configuration file.

Example:

```bash
./build/oxia-benchmark --driver-config conf/driver-oxia.yaml --workloads conf/workload-mixed.yaml
```

#### Driver Configuration

The driver configuration specifies the target system to benchmark. Configuration files for the supported drivers are located in the `conf` directory:

- `conf/driver-oxia.yaml`: For Oxia
- `conf/driver-etcd.yaml`: For etcd
- `conf/driver-zookeeper.yaml`: For Zookeeper

You can customize these files to match your setup.

#### Workload Configuration

The workload configuration defines the benchmark scenarios. The `conf/workload-mixed.yaml` file provides an example of a mixed workload with different read/write ratios and key distributions.

Key workload parameters:

- `readRatio`: The proportion of read operations (0.0 for write-only, 1.0 for read-only).
- `keyspaceSize`: The number of keys to use in the benchmark.
- `keyDistribution`: The distribution of keys (e.g., `order`, `zipf`, `uniform`).
- `valueSize`: The size of the values in bytes.
- `targetRate`: The desired rate of operations per second.
- `duration`: The duration of the workload.
- `parallelism`: The number of concurrent clients.

### Docker

You can also build and run the benchmark using Docker:

```bash
make docker
```

This will build a Docker image named `oxia-benchmark:latest`. To run the benchmark in a Docker container, you will need to mount the configuration files into the container.

## Monitoring

The benchmark tool exposes Prometheus metrics on port `9090` by default. You can scrape these metrics to monitor the performance of the benchmark in real-time. The metrics address can be changed with the `--metrics-addr` flag.

## Extending the Benchmark

You can extend the benchmark to support other key-value stores by creating a new driver.

1.  Create a new directory in the `drivers` directory for your driver.
2.  Implement the `drivers.Driver` interface in your new driver.
3.  Add a new build target to the `Makefile` to build your driver as a Go plugin.

## Cleanup

To remove the build artifacts, run:

```bash
make clean
```