// Command kafman is a terminal UI for monitoring and managing Apache Kafka clusters.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/logging"
	"github.com/Cyber-Watcher/kafman/internal/ui"
	"github.com/Cyber-Watcher/kafman/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", version.Name, err)
		os.Exit(1)
	}
}

type flags struct {
	configPath   string
	cluster      string
	logFile      string
	logLevel     string
	plainFonts   bool
	printVersion bool
	check        bool
	checkAll     bool
}

func parseFlags() *flags {
	f := &flags{}

	flag.StringVar(&f.configPath, "config", "", "path to the config file (default: $XDG_CONFIG_HOME/kafman/config.yaml)")
	flag.StringVar(&f.configPath, "c", "", "shorthand for -config")
	flag.StringVar(&f.cluster, "cluster", "", "cluster to connect to on start (default: current-cluster from the config)")
	flag.StringVar(&f.logFile, "log-file", "", "path to the log file (default: $XDG_STATE_HOME/kafman/kafman.log)")
	flag.StringVar(&f.logLevel, "log-level", "info", "log level: debug, info, warn, error")
	flag.BoolVar(&f.plainFonts, "plain-fonts", false, "use plain ASCII instead of Nerd Font glyphs")
	flag.BoolVar(&f.check, "check", false, "test the connection to the selected cluster and exit")
	flag.BoolVar(&f.checkAll, "check-all", false, "test the connection to every configured cluster and exit")
	flag.BoolVar(&f.printVersion, "version", false, "print version and exit")
	flag.BoolVar(&f.printVersion, "v", false, "shorthand for -version")

	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(),
			"%s — terminal UI for monitoring and managing Apache Kafka clusters.\n\nUsage:\n  %s [flags]\n\nFlags:\n",
			version.Name, version.Name)
		flag.PrintDefaults()
	}
	flag.Parse()

	return f
}

func run() error {
	f := parseFlags()

	if f.printVersion {
		fmt.Print(version.Full())
		return nil
	}

	level, err := parseLogLevel(f.logLevel)
	if err != nil {
		return err
	}

	logPath := f.logFile
	if logPath == "" {
		logPath = logging.DefaultPath()
	}
	closer, err := logging.Setup(logPath, level)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := closer.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "%s: close log file: %v\n", version.Name, cerr)
		}
	}()

	configPath := config.ResolvePath(f.configPath)
	cfg, err := config.Load(configPath)
	if err != nil {
		// Configuration errors are the one class of failure worth printing to
		// the terminal in full: the user has to edit the file to get past them.
		return err
	}

	// The --cluster flag overrides current-cluster for this run only; it is not
	// written back to the file.
	cluster, err := selectCluster(cfg, f.cluster)
	if err != nil {
		return err
	}

	if f.plainFonts {
		cfg.UI.PlainFonts = true
	}

	// --check runs without the interface, so its output can be piped or read in
	// a terminal that cannot host a TUI.
	if f.check || f.checkAll {
		return runCheck(os.Stdout, cfg, cluster, f.checkAll)
	}

	opts := ui.Options{
		Config:  cfg,
		Cluster: cluster,
		LogPath: logPath,
	}
	slog.Info("starting",
		"config", configPath,
		"cluster", cluster.Name,
		"security", cluster.SecurityProfile(),
		"brokers", len(cluster.Brokers))

	program := tea.NewProgram(ui.New(opts), tea.WithAltScreen())
	if _, err := program.Run(); err != nil {
		return fmt.Errorf("run interface: %w", err)
	}

	slog.Info("stopped")
	return nil
}

// selectCluster resolves which cluster to start on: the --cluster flag if given,
// otherwise current-cluster from the config.
func selectCluster(cfg *config.Config, name string) (*config.Cluster, error) {
	if name != "" {
		return cfg.Cluster(name)
	}
	return cfg.Current()
}

func parseLogLevel(s string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("invalid log level %q: want debug, info, warn or error", s)
	}
	return level, nil
}
