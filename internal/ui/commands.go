package ui

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/msg"
)

// This file is the only place where the Kafka layer meets bubbletea.
//
// internal/kafka takes a context and returns values; the adapters here wrap a
// call into a tea.Cmd. Keeping the seam here is what lets the domain package be
// tested, and later reused headless, without dragging in the UI framework.

// connect opens a client for the cluster and describes it. The client is
// carried in the message and becomes the one the tabs use, so connecting and
// establishing the working connection are the same operation.
func connect(ctx context.Context, cluster *config.Cluster, instantiate kafka.Instantiator) tea.Cmd {
	return func() tea.Msg {
		started := time.Now()

		client, err := instantiate(cluster)
		if err != nil {
			return msg.ConnectFailed{Err: err, Diagnosis: kafka.Diagnose(err)}
		}

		info, err := client.DescribeCluster(ctx)
		if err != nil {
			_ = client.Close()
			return msg.ConnectFailed{Err: err, Diagnosis: kafka.Diagnose(err)}
		}

		return msg.Connected{Client: client, Info: info, Latency: time.Since(started)}
	}
}

// testCluster checks a cluster without disturbing the current connection: it
// opens a throwaway client and closes it again.
//
// The context is created inside the command so it lives exactly as long as the
// check does. Creating it outside would mean either leaking the cancel func or
// wrapping the command in tea.Sequence, which is harder to drive from a test.
func testCluster(name string, cluster *config.Cluster) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), kafka.DefaultConnectTimeout)
		defer cancel()
		return msg.ClusterTested{Name: name, Result: kafka.CheckConnectivity(ctx, cluster)}
	}
}

// closeClient shuts a client down off the UI goroutine, since it does network
// I/O. Failures are not surfaced: there is nothing the user could do about a
// connection that is being discarded anyway.
func closeClient(client kafka.Facade) tea.Cmd {
	if client == nil {
		return nil
	}
	return func() tea.Msg {
		_ = client.Close()
		return nil
	}
}
