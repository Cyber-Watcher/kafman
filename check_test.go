package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/config"
)

// runCheck against an address nothing listens on: the report must name the
// cluster, explain the failure and return a non-nil error for the exit code.
func TestRunCheckReportsFailure(t *testing.T) {
	cfg, err := config.LoadFrom([]byte(
		"clusters:\n  - name: dead\n    brokers: [127.0.0.1:1]\n    request-timeout: 2s\n"), t.TempDir())
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	cluster, err := cfg.Current()
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	start := time.Now()
	err = runCheck(&out, cfg, cluster, false)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error when the cluster is unreachable")
	}
	if !strings.Contains(err.Error(), "1 of 1 clusters unreachable") {
		t.Errorf("error = %v, want a count of failures", err)
	}

	report := out.String()
	for _, want := range []string{"✖ dead", "PLAINTEXT", "nothing is listening"} {
		if !strings.Contains(report, want) {
			t.Errorf("report should mention %q, got:\n%s", want, report)
		}
	}

	// request-timeout is 2s; anything close to the 15s default would mean the
	// per-cluster timeout is being ignored.
	if elapsed > 10*time.Second {
		t.Errorf("check took %v, the cluster's request-timeout should have bounded it", elapsed)
	}
}

func TestRunCheckAllVisitsEveryCluster(t *testing.T) {
	cfg, err := config.LoadFrom([]byte(
		"clusters:\n"+
			"  - name: one\n    brokers: [127.0.0.1:1]\n    request-timeout: 1s\n"+
			"  - name: two\n    brokers: [127.0.0.1:2]\n    request-timeout: 1s\n"), t.TempDir())
	if err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	cluster, err := cfg.Current()
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := runCheck(&out, cfg, cluster, true); err == nil {
		t.Fatal("expected an error")
	} else if !strings.Contains(err.Error(), "2 of 2") {
		t.Errorf("error = %v, want both clusters counted", err)
	}

	report := out.String()
	if !strings.Contains(report, "one") || !strings.Contains(report, "two") {
		t.Errorf("both clusters should appear in the report, got:\n%s", report)
	}
}
