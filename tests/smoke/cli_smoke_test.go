//go:build smoke

// Package smoke contains fast, dependency-free boot checks for the
// quota-service binary. They build the real binary and exercise its
// non-serving subcommands, which links every package (including the
// generated protobuf descriptors) and would catch an init-time or wiring
// regression before it ships in the container image.
package smoke

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

var (
	repoRoot string
	binPath  string
)

func TestMain(m *testing.M) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("smoke: cannot determine caller path")
	}
	repoRoot = filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))

	dir, err := os.MkdirTemp("", "quota-smoke")
	if err != nil {
		panic("smoke: mkdir temp: " + err.Error())
	}
	defer os.RemoveAll(dir)

	binPath = filepath.Join(dir, "quota-service")
	build := exec.Command("go", "build", "-o", binPath, "./cmd/quota-service")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		panic("smoke: build quota-service: " + err.Error() + "\n" + string(out))
	}

	os.Exit(m.Run())
}

func runService(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(binPath, args...)
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestVersionSubcommand(t *testing.T) {
	out, err := runService(t, "version")
	if err != nil {
		t.Fatalf("version: %v\n%s", err, out)
	}
	if !strings.Contains(out, "quota-service") {
		t.Fatalf("version output missing service name: %q", out)
	}
}

func TestPrintConfigSubcommand(t *testing.T) {
	out, err := runService(t, "print-config")
	if err != nil {
		t.Fatalf("print-config: %v\n%s", err, out)
	}
	if !strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Fatalf("print-config did not emit JSON: %q", out)
	}
}

func TestValidateLimitsExamples(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join(repoRoot, "examples", "limits", "*.yaml"))
	if err != nil {
		t.Fatalf("glob examples: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("no example limit files found")
	}
	for _, example := range matches {
		out, err := runService(t, "validate-limits", example)
		if err != nil {
			t.Fatalf("validate-limits %s: %v\n%s", example, err, out)
		}
		if !strings.Contains(out, "\"valid\": true") {
			t.Fatalf("validate-limits %s did not report valid: %q", example, out)
		}
	}
}
