package sequence

import (
	"sync"
	"testing"
)

func TestNewGenerator_Uniform(t *testing.T) {
	g := NewGenerator("uniform", 1000)
	if g == nil {
		t.Fatal("expected non-nil generator")
	}
	for i := 0; i < 100; i++ {
		v := g.Next()
		if v >= 1000 {
			t.Fatalf("uniform generator returned %d, expected < 1000", v)
		}
	}
}

func TestNewGenerator_Zipf(t *testing.T) {
	g := NewGenerator("zipf", 1000)
	if g == nil {
		t.Fatal("expected non-nil generator")
	}
	for i := 0; i < 100; i++ {
		v := g.Next()
		if v >= 1000 {
			t.Fatalf("zipf generator returned %d, expected < 1000", v)
		}
	}
}

func TestNewGenerator_Order(t *testing.T) {
	g := NewGenerator("order", 1000)
	if g == nil {
		t.Fatal("expected non-nil generator")
	}
	for i := 0; i < 100; i++ {
		v := g.Next()
		if v >= 1000 {
			t.Fatalf("order generator returned %d, expected < 1000", v)
		}
	}
}

func TestNewGenerator_UnknownPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for unknown generator")
		}
	}()
	NewGenerator("unknown", 1000)
}

func TestOrder_Sequential(t *testing.T) {
	g := NewGenerator("order", 5)
	expected := []uint64{1, 2, 3, 4, 0, 1, 2, 3, 4, 0}
	for i, want := range expected {
		got := g.Next()
		if got != want {
			t.Fatalf("iteration %d: got %d, want %d", i, got, want)
		}
	}
}

func TestOrder_ConcurrentSafety(t *testing.T) {
	g := NewGenerator("order", 100)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				v := g.Next()
				if v >= 100 {
					t.Errorf("order generator returned %d, expected < 100", v)
				}
			}
		}()
	}
	wg.Wait()
}

func TestZipf_ConcurrentSafety(t *testing.T) {
	g := NewGenerator("zipf", 1000)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				v := g.Next()
				if v >= 1000 {
					t.Errorf("zipf generator returned %d, expected < 1000", v)
				}
			}
		}()
	}
	wg.Wait()
}

func TestUniform_Distribution(t *testing.T) {
	g := NewGenerator("uniform", 10)
	seen := make(map[uint64]bool)
	for i := 0; i < 1000; i++ {
		seen[g.Next()] = true
	}
	// With 1000 samples over 10 values, we should see most of them
	if len(seen) < 8 {
		t.Fatalf("uniform distribution too skewed: only saw %d of 10 values", len(seen))
	}
}
