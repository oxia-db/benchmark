# Benchmark dashboards

Prometheus + Grafana stack for visualizing `oxia-benchmark` metrics, including
distributed runs where multiple load-generator instances each expose their own
`/metrics` endpoint.

## Quick start

1. Start one or more benchmark clients — using any [supported backend](../README.md#supported-backends) — each on a different `-m` port:

   ```
   bin/oxia-benchmark --driver-config conf/driver-oxia.yaml \
       --workloads conf/workload-mixed.yaml -m 0.0.0.0:8080
   ```

2. Point Prometheus at the clients by editing
   [`prometheus.yml`](prometheus.yml). The default config scrapes
   `host.docker.internal:8080`. Add one target per client for distributed runs,
   giving each a unique `instance_id` label.

3. Launch the monitoring stack:

   ```
   cd deploy && docker compose up -d
   ```

4. Open Grafana at <http://localhost:3000> (anonymous admin login is enabled).
   The **Oxia Benchmark** dashboard is auto-provisioned.

## Dashboard panels

- **Throughput** — `sum by (type, ok) (rate(kv_op_latency_seconds_count[...]))`
  aggregated across all instances.
- **Latency percentiles over time** — p50 / p95 / p99 / p99.9 using
  `histogram_quantile` on the classic buckets.
- **Latency distribution heatmap** — density of operations per latency bucket
  over time.
- **Percentile distribution (HdrHistogram-style)** — line chart with percentile
  on the X axis (log-scaled: 0.5 → 0.99999) and latency on the Y axis (log-
  scaled, seconds), one line per operation type. Same shape as the classic
  HdrHistogram percentile plot.
- **Scheduling delay percentiles** — intended vs. actual send time. Use this to
  distinguish load-generator stalls (GC, CPU contention) from real server
  latency.

## Distributed runs

Each benchmark instance exposes its own metrics endpoint. Prometheus scrapes
them all and the queries in the dashboard use `sum by (...)` to aggregate.
Update `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'oxia-benchmark'
    static_configs:
      - targets: ['host1:8080']
        labels:
          instance_id: 'client-1'
      - targets: ['host2:8080']
        labels:
          instance_id: 'client-2'
```

Reload with `curl -X POST http://localhost:9090/-/reload`.

## Native histograms

The benchmark emits both classic (fixed-bucket) and native (exponential-bucket)
Prometheus histograms. Classic buckets are used by the dashboard's
`histogram_quantile` queries. Native histograms are available if you prefer
finer-grained queries — enable them in Prometheus with
`--enable-feature=native-histograms` (already configured in
`docker-compose.yml`).
