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

package runner

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"oxia-benchmark/drivers"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bmizerany/perks/quantile"
	"github.com/oxia-db/oxia/common/metric"
	"golang.org/x/time/rate"
)

var (
	opReadLatency = metric.NewLatencyHistogram("kv.op.latency", "Read operation latency",
		map[string]any{"type": "read"})
	opWriteLatency = metric.NewLatencyHistogram("kv.op.latency", "Write operation latency",
		map[string]any{"type": "write"})
	outstandingRequestGauge = metric.NewUpDownCounter("kv.op.outstanding", "Count of outstanding operations", "count",
		map[string]any{})
)

type runner struct {
	workload *Workload
	driver   drivers.KVStoreDriver
	keys     []string
	limiter  *rate.Limiter

	writeLatencyCh  chan int64
	readLatencyCh   chan int64
	periodFailedOps atomic.Int64
	totalFailedOps  atomic.Int64

	periodStats stats
	totalStats  stats

	ctx    context.Context
	cancel context.CancelFunc
}

type stats struct {
	writeOpsCount int64
	readOpsCount  int64
	failedOps     atomic.Int64

	writeLatency *quantile.Stream
	readLatency  *quantile.Stream
}

func Run(wl *Workload, driver drivers.KVStoreDriver) error {
	slog.Info("Running workload", slog.Any("workload", *wl))

	r := &runner{
		workload:       wl,
		driver:         driver,
		limiter:        rate.NewLimiter(rate.Limit(wl.TargetRate), int(wl.TargetRate)),
		writeLatencyCh: make(chan int64, 1000),
		readLatencyCh:  make(chan int64, 1000),
		periodStats: stats{
			writeLatency: quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0),
			readLatency:  quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0),
		},
		totalStats: stats{
			writeLatency: quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0),
			readLatency:  quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0),
		},
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.keys = make([]string, wl.KeyspaceSize)
	for i := 0; i < wl.KeyspaceSize; i++ {
		r.keys[i] = fmt.Sprintf("key-%016d", i)
	}

	testStart := time.Now()
	statsTickerPeriod := 10 * time.Second
	stasTicker := time.Tick(statsTickerPeriod)
	endTimer := time.NewTimer(wl.Duration)

	wg := &sync.WaitGroup{}

	for i := 0; i < wl.Parallelism; i++ {
		wg.Go(func() {
			r.generateTraffic()
		})
	}

	for {
		select {
		case <-stasTicker:
			printStats(&r.periodStats, statsTickerPeriod)

		case wl := <-r.writeLatencyCh:
			r.periodStats.writeOpsCount++
			r.totalStats.writeOpsCount++
			latency := float64(wl) / 1000.0 // Convert to millis
			r.periodStats.writeLatency.Insert(latency)
			r.totalStats.writeLatency.Insert(latency)

		case rl := <-r.readLatencyCh:
			r.periodStats.readOpsCount++
			r.totalStats.readOpsCount++
			latency := float64(rl) / 1000.0 // Convert to millis
			r.periodStats.readLatency.Insert(latency)
			r.totalStats.readLatency.Insert(latency)

		case <-endTimer.C:
			// Stop all workers and wait for them
			r.cancel()
			time.Sleep(1 * time.Second)
			wg.Wait()

			slog.Info("-------------------------------------------------------")
			slog.Info("Cumulative write/read latencies")
			printStats(&r.totalStats, time.Now().Sub(testStart))

			return nil
		}
	}
}

type kv struct {
	key   string
	value []byte
	start time.Time
}

func (r *runner) generateTraffic() {
	value := make([]byte, r.workload.ValueSize)
	perWorkerRate := float64(r.workload.TargetRate) / float64(r.workload.Parallelism)
	limiter := rate.NewLimiter(rate.Limit(perWorkerRate), int(perWorkerRate))
	reqCh := make(chan *kv, 1000)
	go r.consumeTraffic(reqCh)
	for {
		if err := limiter.Wait(r.ctx); err != nil {
			return
		}
		key := r.keys[rand.Intn(r.workload.KeyspaceSize)] //nolint:gosec
		outstandingRequestGauge.Inc()
		start := time.Now()
		reqCh <- &kv{key, value, start}
	}
}

type result struct {
	kvResultCh <-chan error
	latencyCh  chan<- int64
	start      time.Time
}

func (r *runner) consumeTraffic(reqCh <-chan *kv) {
	resultCh := make(chan *result)
	go r.handleResult(resultCh)
	for {
		select {
		case req := <-reqCh:
			key := req.key
			value := req.value
			var ch <-chan error
			var latencyCh chan int64
			if rand.Float64() < r.workload.ReadRatio {
				ch = r.driver.Get(key)
				latencyCh = r.readLatencyCh
			} else {
				ch = r.driver.Put(key, value)
				latencyCh = r.writeLatencyCh
			}

			resultCh <- &result{
				kvResultCh: ch,
				latencyCh:  latencyCh,
				start:      req.start,
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *runner) handleResult(resultCh <-chan *result) {
	for {
		select {
		case result := <-resultCh:
			if err := <-result.kvResultCh; err != nil {
				slog.Error("Error", "error", err)
				r.periodStats.failedOps.Add(1)
				r.totalStats.failedOps.Add(1)
			} else {
				result.latencyCh <- time.Since(result.start).Microseconds()
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func printStats(s *stats, period time.Duration) {
	writeRate := float64(s.writeOpsCount) / period.Seconds()
	readRate := float64(s.readOpsCount) / period.Seconds()
	failedOpsRate := float64(s.failedOps.Swap(0)) / period.Seconds()

	slog.Info(fmt.Sprintf(`Stats - Total ops: %6.1f ops/s - Failed ops: %6.1f ops/s
			Write ops %6.1f w/s  Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f
			Read  ops %6.1f r/s  Latency ms: 50%% %5.1f - 95%% %5.1f - 99%% %5.1f - 99.9%% %5.1f - max %6.1f`,
		writeRate+readRate,
		failedOpsRate,
		writeRate,
		s.writeLatency.Query(0.5),
		s.writeLatency.Query(0.95),
		s.writeLatency.Query(0.99),
		s.writeLatency.Query(0.999),
		s.writeLatency.Query(1.0),
		readRate,
		s.readLatency.Query(0.5),
		s.readLatency.Query(0.95),
		s.readLatency.Query(0.99),
		s.readLatency.Query(0.999),
		s.readLatency.Query(1.0),
	))

	s.writeLatency.Reset()
	s.readLatency.Reset()
	s.writeOpsCount = 0
	s.readOpsCount = 0
}
