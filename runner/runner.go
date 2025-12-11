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
	"oxia-benchmark/runner/sequence"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bmizerany/perks/quantile"
	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/common/metric"
	"golang.org/x/time/rate"
)

type runner struct {
	workload          *Workload
	driver            drivers.KVStoreDriver
	sequenceGenerator sequence.Generator
	limiter           *rate.Limiter

	writeLatencyCh  chan int64
	readLatencyCh   chan int64
	periodFailedOps atomic.Int64
	totalFailedOps  atomic.Int64

	periodStats stats
	totalStats  stats

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	opReadFailedLatency   metric.LatencyHistogram
	opReadSuccessLatency  metric.LatencyHistogram
	opWriteFailedLatency  metric.LatencyHistogram
	opWriteSuccessLatency metric.LatencyHistogram

	outstandingRequestGauge metric.UpDownCounter
}

type stats struct {
	writeOpsCount int64
	readOpsCount  int64
	failedOps     atomic.Int64

	writeLatency *quantile.Stream
	readLatency  *quantile.Stream
}

func Run(metadata Metadata, wl *Workload, driver drivers.KVStoreDriver) error {
	slog.Info("Running workload", slog.Any("workload", *wl))

	sequenceGenerator := sequence.NewGenerator(wl.KeyDistribution, wl.KeyspaceSize)

	r := &runner{
		wg:                sync.WaitGroup{},
		workload:          wl,
		sequenceGenerator: sequenceGenerator,
		driver:            driver,
		limiter:           rate.NewLimiter(rate.Limit(wl.TargetRate), int(wl.TargetRate)),
		writeLatencyCh:    make(chan int64, 1000),
		readLatencyCh:     make(chan int64, 1000),
		periodStats: stats{
			writeLatency: quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0),
			readLatency:  quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0),
		},
		totalStats: stats{
			writeLatency: quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0),
			readLatency:  quantile.NewTargeted(0.50, 0.95, 0.99, 0.999, 1.0),
		},
		opReadFailedLatency: metric.NewLatencyHistogram("kv.op.latency", "Read operation latency",
			map[string]any{
				"driver":       driver.Name(),
				"type":         "read",
				"ok":           false,
				"valueSize":    wl.ValueSize,
				"distribution": wl.KeyDistribution,
				"readRatio":    wl.ReadRatio,
				"serverNum":    metadata.ServerNum,
			}),
		opReadSuccessLatency: metric.NewLatencyHistogram("kv.op.latency", "Read operation latency",
			map[string]any{
				"driver":       driver.Name(),
				"type":         "read",
				"ok":           true,
				"valueSize":    wl.ValueSize,
				"distribution": wl.KeyDistribution,
				"readRatio":    wl.ReadRatio,
				"serverNum":    metadata.ServerNum,
			}),
		opWriteSuccessLatency: metric.NewLatencyHistogram("kv.op.latency", "Write operation latency",
			map[string]any{
				"driver":       driver.Name(),
				"type":         "write",
				"ok":           true,
				"valueSize":    wl.ValueSize,
				"distribution": wl.KeyDistribution,
				"readRatio":    wl.ReadRatio,
				"serverNum":    metadata.ServerNum,
			}),
		opWriteFailedLatency: metric.NewLatencyHistogram("kv.op.latency", "Write operation latency",
			map[string]any{
				"driver":       driver.Name(),
				"type":         "write",
				"ok":           false,
				"valueSize":    wl.ValueSize,
				"distribution": wl.KeyDistribution,
				"readRatio":    wl.ReadRatio,
				"serverNum":    metadata.ServerNum,
			}),
		outstandingRequestGauge: metric.NewUpDownCounter("kv.op.outstanding", "Count of outstanding operations", "count",
			map[string]any{
				"driver":       driver.Name(),
				"valueSize":    wl.ValueSize,
				"distribution": wl.KeyDistribution,
				"readRatio":    wl.ReadRatio,
				"serverNum":    metadata.ServerNum,
			},
		),
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	testStart := time.Now()
	statsTickerPeriod := 10 * time.Second
	stasTicker := time.Tick(statsTickerPeriod)
	endTimer := time.NewTimer(wl.Duration)

	for i := 0; i < wl.Parallelism; i++ {
		r.wg.Go(func() {
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
			r.wg.Wait()

			slog.Info("-------------------------------------------------------")
			slog.Info("Cumulative write/read latencies")
			printStats(&r.totalStats, time.Now().Sub(testStart))

			return nil
		}
	}
}

type kvReq struct {
	key   string
	value []byte
}

type kvRes struct {
	kvResCh        <-chan *drivers.KVResult
	latencyCh      chan<- int64
	start          time.Time
	successLatency *metric.Timer
	failedLatency  *metric.Timer
}

func (r *runner) generateTraffic() {
	value := make([]byte, r.workload.ValueSize)
	perWorkerRate := r.workload.TargetRate / float64(r.workload.Parallelism)
	limiter := rate.NewLimiter(rate.Limit(perWorkerRate), int(perWorkerRate))
	reqCh := make(chan *kvReq, 1000)
	r.wg.Go(func() {
		r.consumeTraffic(reqCh)
	})
	for {
		if err := limiter.Wait(r.ctx); err != nil {
			return
		}
		key := fmt.Sprintf("k-%016d", r.sequenceGenerator.Next())
		r.outstandingRequestGauge.Inc()
		channel.PushNoBlock(reqCh, &kvReq{key, value})
	}
}

func (r *runner) consumeTraffic(reqCh <-chan *kvReq) {
	resCh := make(chan *kvRes)
	r.wg.Go(func() {
		r.handleResult(resCh)
	})
	for {
		select {
		case req := <-reqCh:
			key := req.key
			value := req.value
			var ch <-chan *drivers.KVResult
			var latencyCh chan int64
			start := time.Now()

			var successTimer metric.Timer
			var failedTimer metric.Timer
			if rand.Float64() < r.workload.ReadRatio {
				ch = r.driver.Get(key)
				latencyCh = r.readLatencyCh
				successTimer = r.opReadSuccessLatency.Timer()
				failedTimer = r.opReadFailedLatency.Timer()
			} else {
				ch = r.driver.Put(key, value)
				latencyCh = r.writeLatencyCh
				successTimer = r.opWriteSuccessLatency.Timer()
				failedTimer = r.opWriteFailedLatency.Timer()
			}
			channel.PushNoBlock(resCh, &kvRes{
				kvResCh:        ch,
				latencyCh:      latencyCh,
				start:          start,
				successLatency: &successTimer,
				failedLatency:  &failedTimer,
			})
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *runner) handleResult(resCh <-chan *kvRes) {
	for {
		select {
		case result := <-resCh:
			if res := <-result.kvResCh; res.Err != nil {
				slog.Error("Error", "error", res.Err)
				r.periodStats.failedOps.Add(1)
				r.totalStats.failedOps.Add(1)
				result.failedLatency.Done()
			} else {
				result.latencyCh <- time.Since(res.End).Microseconds()
				result.successLatency.Done()
			}
			r.outstandingRequestGauge.Dec()
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
