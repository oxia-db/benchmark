package drivers

import (
	"testing"
)

func TestBuild_UnknownDriver(t *testing.T) {
	cfg := &DriverConfig{
		Driver: "unknown",
		Config: map[string]any{},
	}
	_, err := Build(cfg)
	if err == nil {
		t.Fatal("expected error for unknown driver")
	}
	expected := "unknown driver: unknown"
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

func TestBuild_MissingPlugin(t *testing.T) {
	cfg := &DriverConfig{
		Driver: "oxia",
		Config: map[string]any{},
	}
	_, err := Build(cfg)
	if err == nil {
		t.Fatal("expected error when plugin file is missing")
	}
}
