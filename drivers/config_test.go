package drivers

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig_Valid(t *testing.T) {
	content := `
driver: oxia
config:
  serviceAddress: "localhost:6648"
  namespace: "default"
`
	path := writeTempFile(t, content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Driver != "oxia" {
		t.Fatalf("expected driver 'oxia', got %q", cfg.Driver)
	}
	if cfg.Config["serviceAddress"] != "localhost:6648" {
		t.Fatalf("expected serviceAddress 'localhost:6648', got %v", cfg.Config["serviceAddress"])
	}
	if cfg.Config["namespace"] != "default" {
		t.Fatalf("expected namespace 'default', got %v", cfg.Config["namespace"])
	}
}

func TestLoadConfig_EtcdEndpoints(t *testing.T) {
	content := `
driver: etcd
config:
  endpoints:
    - "localhost:2379"
    - "localhost:2380"
  dialTimeout: "5s"
`
	path := writeTempFile(t, content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Driver != "etcd" {
		t.Fatalf("expected driver 'etcd', got %q", cfg.Driver)
	}
	endpoints, ok := cfg.Config["endpoints"].([]any)
	if !ok {
		t.Fatalf("expected endpoints to be a list, got %T", cfg.Config["endpoints"])
	}
	if len(endpoints) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(endpoints))
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadConfig_InvalidYAML(t *testing.T) {
	path := writeTempFile(t, ":::invalid")
	_, err := LoadConfig(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadConfig_EmptyFile(t *testing.T) {
	path := writeTempFile(t, "")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Driver != "" {
		t.Fatalf("expected empty driver, got %q", cfg.Driver)
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
