package runner

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkload_WithDefault(t *testing.T) {
	w := &Workload{}
	w.WithDefault()
	if w.KeyDistribution != "uniform" {
		t.Fatalf("expected default keyDistribution 'uniform', got %q", w.KeyDistribution)
	}

	w2 := &Workload{KeyDistribution: "zipf"}
	w2.WithDefault()
	if w2.KeyDistribution != "zipf" {
		t.Fatalf("expected keyDistribution 'zipf' to be preserved, got %q", w2.KeyDistribution)
	}
}

func TestWorkload_Validate_Valid(t *testing.T) {
	w := &Workload{
		ReadRatio:       0.5,
		KeyspaceSize:    1000,
		KeyDistribution: "uniform",
		ValueSize:       64,
		TargetRate:      1000,
		Duration:        10 * time.Second,
		Parallelism:     4,
	}
	if err := w.Validate(); err != nil {
		t.Fatalf("expected valid workload, got error: %v", err)
	}
}

func TestWorkload_Validate_Errors(t *testing.T) {
	base := Workload{
		ReadRatio:       0.5,
		KeyspaceSize:    1000,
		KeyDistribution: "uniform",
		ValueSize:       64,
		TargetRate:      1000,
		Duration:        10 * time.Second,
		Parallelism:     4,
	}

	tests := []struct {
		name   string
		modify func(*Workload)
		errMsg string
	}{
		{"zero keyspaceSize", func(w *Workload) { w.KeyspaceSize = 0 }, "keyspaceSize"},
		{"zero valueSize", func(w *Workload) { w.ValueSize = 0 }, "valueSize"},
		{"negative valueSize", func(w *Workload) { w.ValueSize = -1 }, "valueSize"},
		{"zero targetRate", func(w *Workload) { w.TargetRate = 0 }, "targetRate"},
		{"negative targetRate", func(w *Workload) { w.TargetRate = -1 }, "targetRate"},
		{"zero duration", func(w *Workload) { w.Duration = 0 }, "duration"},
		{"negative duration", func(w *Workload) { w.Duration = -1 }, "duration"},
		{"zero parallelism", func(w *Workload) { w.Parallelism = 0 }, "parallelism"},
		{"negative parallelism", func(w *Workload) { w.Parallelism = -1 }, "parallelism"},
		{"readRatio > 1", func(w *Workload) { w.ReadRatio = 1.5 }, "readRatio"},
		{"readRatio < 0", func(w *Workload) { w.ReadRatio = -0.1 }, "readRatio"},
		{"bad distribution", func(w *Workload) { w.KeyDistribution = "invalid" }, "keyDistribution"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := base
			tt.modify(&w)
			err := w.Validate()
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !contains(err.Error(), tt.errMsg) {
				t.Fatalf("expected error containing %q, got %q", tt.errMsg, err.Error())
			}
		})
	}
}

func TestWorkload_Validate_AllDistributions(t *testing.T) {
	for _, dist := range []string{"uniform", "zipf", "order"} {
		w := &Workload{
			ReadRatio:       0,
			KeyspaceSize:    100,
			KeyDistribution: dist,
			ValueSize:       64,
			TargetRate:      100,
			Duration:        time.Second,
			Parallelism:     1,
		}
		if err := w.Validate(); err != nil {
			t.Fatalf("distribution %q should be valid, got: %v", dist, err)
		}
	}
}

func TestWorkload_Validate_BoundaryReadRatio(t *testing.T) {
	base := Workload{
		KeyspaceSize:    100,
		KeyDistribution: "uniform",
		ValueSize:       64,
		TargetRate:      100,
		Duration:        time.Second,
		Parallelism:     1,
	}

	for _, ratio := range []float64{0.0, 1.0} {
		w := base
		w.ReadRatio = ratio
		if err := w.Validate(); err != nil {
			t.Fatalf("readRatio %v should be valid, got: %v", ratio, err)
		}
	}
}

func TestLoad_ValidFile(t *testing.T) {
	content := `
exitWhenFinish: true
metadata:
  serverNum: 3
items:
  - readRatio: 0.5
    keyspaceSize: 1000
    keyDistribution: uniform
    valueSize: 64
    targetRate: 1000
    duration: 10s
    parallelism: 4
`
	path := writeTempFile(t, content)
	wls, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !wls.ExitWhenFinish {
		t.Fatal("expected exitWhenFinish to be true")
	}
	if wls.Metadata.ServerNum != 3 {
		t.Fatalf("expected serverNum 3, got %d", wls.Metadata.ServerNum)
	}
	if len(wls.Items) != 1 {
		t.Fatalf("expected 1 workload item, got %d", len(wls.Items))
	}
	if wls.Items[0].ReadRatio != 0.5 {
		t.Fatalf("expected readRatio 0.5, got %f", wls.Items[0].ReadRatio)
	}
}

func TestLoad_DefaultDistribution(t *testing.T) {
	content := `
items:
  - keyspaceSize: 100
    valueSize: 64
    targetRate: 100
    duration: 1s
    parallelism: 1
`
	path := writeTempFile(t, content)
	wls, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wls.Items[0].KeyDistribution != "uniform" {
		t.Fatalf("expected default distribution 'uniform', got %q", wls.Items[0].KeyDistribution)
	}
}

func TestLoad_ValidationError(t *testing.T) {
	content := `
items:
  - readRatio: 0.5
    keyspaceSize: 0
    valueSize: 64
    targetRate: 1000
    duration: 10s
    parallelism: 4
`
	path := writeTempFile(t, content)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for keyspaceSize=0")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	path := writeTempFile(t, ":::invalid yaml")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/workload.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoad_MultipleWorkloads(t *testing.T) {
	content := `
items:
  - keyspaceSize: 100
    keyDistribution: order
    valueSize: 64
    targetRate: 100
    duration: 1s
    parallelism: 1
  - readRatio: 1.0
    keyspaceSize: 200
    keyDistribution: zipf
    valueSize: 128
    targetRate: 200
    duration: 5s
    parallelism: 2
`
	path := writeTempFile(t, content)
	wls, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(wls.Items) != 2 {
		t.Fatalf("expected 2 workload items, got %d", len(wls.Items))
	}
	if wls.Items[1].KeyspaceSize != 200 {
		t.Fatalf("expected second workload keyspaceSize 200, got %d", wls.Items[1].KeyspaceSize)
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
