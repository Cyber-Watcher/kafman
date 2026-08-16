package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

// runCheck tests one or all clusters and prints a report, without starting the
// interface. It exists so a broken connection can be diagnosed from a script or
// a terminal that cannot run a TUI, and returns an error when any check fails so
// the exit code is usable in CI.
func runCheck(out io.Writer, cfg *config.Config, cluster *config.Cluster, all bool) error {
	clusters := []*config.Cluster{cluster}
	if all {
		clusters = clusters[:0]
		for i := range cfg.Clusters {
			clusters = append(clusters, &cfg.Clusters[i])
		}
	}

	var failed int
	for _, cl := range clusters {
		ctx, cancel := context.WithTimeout(context.Background(), kafka.DefaultConnectTimeout)
		result := kafka.CheckConnectivity(ctx, cl)
		cancel()

		if result.OK() {
			printOK(out, cl, result)
			continue
		}
		failed++
		printFailure(out, cl, result)
	}

	if failed > 0 {
		return fmt.Errorf("%d of %d clusters unreachable", failed, len(clusters))
	}
	return nil
}

func printOK(out io.Writer, cl *config.Cluster, result kafka.ConnCheck) {
	info := result.Info
	printf(out, "✔ %s  %s  %v\n", cl.Name, cl.SecurityProfile(), result.Latency.Round(time.Millisecond))
	if info.ClusterID != "" {
		printf(out, "    cluster id  %s\n", info.ClusterID)
	}
	for _, b := range info.Brokers {
		marker := " "
		if b.IsController {
			marker = "◆"
		}
		rack := ""
		if b.Rack != "" {
			rack = "  " + b.Rack
		}
		printf(out, "    %s broker %-3d %s%s\n", marker, b.ID, b.Addr, rack)
	}
}

func printFailure(out io.Writer, cl *config.Cluster, result kafka.ConnCheck) {
	printf(out, "✖ %s  %s\n", cl.Name, cl.SecurityProfile())
	if result.Diagnosis != "" {
		printf(out, "    %s\n", indent(result.Diagnosis, "    "))
	}
	printf(out, "    %s\n", indent(result.Err.Error(), "    "))
}

// printf writes a report line. A write failure here means the output is gone
// (a closed pipe, typically), and there is nowhere left to report it.
func printf(out io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(out, format, args...)
}

func indent(s, prefix string) string {
	return strings.ReplaceAll(s, "\n", "\n"+prefix)
}
