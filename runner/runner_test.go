package runner

import (
	"oxia-benchmark/drivers"
	"sync/atomic"
	"testing"
	"time"
)

type mockDriver struct {
	putCount atomic.Int64
	getCount atomic.Int64
}

func (m *mockDriver) Name() string { return "mock" }

func (m *mockDriver) Init(_ map[string]any) error { return nil }

func (m *mockDriver) Put(_ string, _ []byte) <-chan *drivers.KVResult {
	m.putCount.Add(1)
	ch := make(chan *drivers.KVResult, 1)
	ch <- &drivers.KVResult{Err: nil, End: time.Now()}
	return ch
}

func (m *mockDriver) Get(_ string) <-chan *drivers.KVResult {
	m.getCount.Add(1)
	ch := make(chan *drivers.KVResult, 1)
	ch <- &drivers.KVResult{Err: nil, End: time.Now()}
	return ch
}

func (m *mockDriver) Close() error { return nil }

func TestRun_WriteOnly(t *testing.T) {
	drv := &mockDriver{}
	wl := &Workload{
		ReadRatio:       0.0,
		KeyspaceSize:    100,
		KeyDistribution: "uniform",
		ValueSize:       64,
		TargetRate:      1000,
		Duration:        time.Second,
		Parallelism:     2,
	}
	metadata := Metadata{ServerNum: 1}

	err := Run(metadata, wl, drv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	puts := drv.putCount.Load()
	gets := drv.getCount.Load()
	if puts == 0 {
		t.Fatal("expected some put operations")
	}
	if gets != 0 {
		t.Fatalf("expected no get operations with readRatio=0, got %d", gets)
	}
}

func TestRun_ReadOnly(t *testing.T) {
	drv := &mockDriver{}
	wl := &Workload{
		ReadRatio:       1.0,
		KeyspaceSize:    100,
		KeyDistribution: "uniform",
		ValueSize:       64,
		TargetRate:      1000,
		Duration:        time.Second,
		Parallelism:     2,
	}
	metadata := Metadata{ServerNum: 1}

	err := Run(metadata, wl, drv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	puts := drv.putCount.Load()
	gets := drv.getCount.Load()
	if gets == 0 {
		t.Fatal("expected some get operations")
	}
	if puts != 0 {
		t.Fatalf("expected no put operations with readRatio=1, got %d", puts)
	}
}

func TestRun_MixedWorkload(t *testing.T) {
	drv := &mockDriver{}
	wl := &Workload{
		ReadRatio:       0.5,
		KeyspaceSize:    100,
		KeyDistribution: "uniform",
		ValueSize:       64,
		TargetRate:      2000,
		Duration:        time.Second,
		Parallelism:     2,
	}
	metadata := Metadata{ServerNum: 1}

	err := Run(metadata, wl, drv)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	puts := drv.putCount.Load()
	gets := drv.getCount.Load()
	if puts == 0 {
		t.Fatal("expected some put operations")
	}
	if gets == 0 {
		t.Fatal("expected some get operations")
	}
}

func TestRun_AllDistributions(t *testing.T) {
	for _, dist := range []string{"uniform", "zipf", "order"} {
		t.Run(dist, func(t *testing.T) {
			drv := &mockDriver{}
			wl := &Workload{
				ReadRatio:       0.0,
				KeyspaceSize:    100,
				KeyDistribution: dist,
				ValueSize:       32,
				TargetRate:      500,
				Duration:        500 * time.Millisecond,
				Parallelism:     1,
			}
			err := Run(Metadata{ServerNum: 1}, wl, drv)
			if err != nil {
				t.Fatalf("unexpected error with distribution %q: %v", dist, err)
			}
			if drv.putCount.Load() == 0 {
				t.Fatalf("expected operations with distribution %q", dist)
			}
		})
	}
}

func TestRun_RespectsDuration(t *testing.T) {
	drv := &mockDriver{}
	wl := &Workload{
		ReadRatio:       0.0,
		KeyspaceSize:    100,
		KeyDistribution: "uniform",
		ValueSize:       64,
		TargetRate:      1000,
		Duration:        500 * time.Millisecond,
		Parallelism:     1,
	}

	start := time.Now()
	err := Run(Metadata{ServerNum: 1}, wl, drv)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed < 400*time.Millisecond {
		t.Fatalf("finished too quickly: %v", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("took too long: %v", elapsed)
	}
}
