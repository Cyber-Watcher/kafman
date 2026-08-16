package config

import "time"

// Defaults applied to any field left unset in the file.
const (
	DefaultTheme            = ThemeDark
	DefaultTimestampFormat  = TimestampDateTime
	DefaultRefreshInterval  = 5 * time.Second
	DefaultRecordBufferSize = 5000
	DefaultRequestTimeout   = 10 * time.Second
	DefaultClientID         = "kafman"

	// MaxQueryHistory caps the saved query history so the config file cannot
	// grow without bound.
	MaxQueryHistory = 100
)

// Themes.
const (
	ThemeDark  = "dark"
	ThemeLight = "light"
)

// Timestamp display formats.
const (
	TimestampDateTime = "datetime"
	TimestampAgo      = "ago"
)

// New returns a usable configuration with one local cluster. It is what gets
// written on first run, so kafman is immediately useful against a stock broker.
func New() *Config {
	return &Config{
		CurrentCluster: "local",
		UI:             defaultUI(),
		Clusters: []Cluster{{
			Name:           "local",
			Color:          "#00D787",
			Brokers:        []string{"localhost:9092"},
			ClientID:       DefaultClientID,
			RequestTimeout: Duration(DefaultRequestTimeout),
		}},
	}
}

func defaultUI() UI {
	return UI{
		Theme:            DefaultTheme,
		PlainFonts:       false,
		TimestampFormat:  DefaultTimestampFormat,
		RefreshInterval:  Duration(DefaultRefreshInterval),
		RecordBufferSize: DefaultRecordBufferSize,
	}
}

// applyDefaults fills in everything the file left empty. Called after unmarshal
// and before validation, so validation only ever sees complete values.
func (c *Config) applyDefaults() {
	if c.UI.Theme == "" {
		c.UI.Theme = DefaultTheme
	}
	if c.UI.TimestampFormat == "" {
		c.UI.TimestampFormat = DefaultTimestampFormat
	}
	if c.UI.RefreshInterval <= 0 {
		c.UI.RefreshInterval = Duration(DefaultRefreshInterval)
	}
	if c.UI.RecordBufferSize <= 0 {
		c.UI.RecordBufferSize = DefaultRecordBufferSize
	}
	if len(c.UI.QueryHistory) > MaxQueryHistory {
		c.UI.QueryHistory = c.UI.QueryHistory[len(c.UI.QueryHistory)-MaxQueryHistory:]
	}

	for i := range c.Clusters {
		cl := &c.Clusters[i]
		if cl.ClientID == "" {
			cl.ClientID = DefaultClientID
		}
		if cl.RequestTimeout <= 0 {
			cl.RequestTimeout = Duration(DefaultRequestTimeout)
		}
		if cl.SASL.Enabled && cl.SASL.Mechanism == "" {
			// PLAIN is what a bare "sasl: {enabled: true, username, password}"
			// almost always means.
			cl.SASL.Mechanism = MechanismPlain
		}
		cl.SASL.Mechanism = normaliseMechanism(cl.SASL.Mechanism)
	}

	if c.CurrentCluster == "" && len(c.Clusters) > 0 {
		c.CurrentCluster = c.Clusters[0].Name
	}
}

// AddQuery records a query in the history, most recent last, without duplicates.
func (c *Config) AddQuery(query string) {
	if query == "" {
		return
	}
	for i, q := range c.UI.QueryHistory {
		if q == query {
			c.UI.QueryHistory = append(c.UI.QueryHistory[:i], c.UI.QueryHistory[i+1:]...)
			break
		}
	}
	c.UI.QueryHistory = append(c.UI.QueryHistory, query)
	if len(c.UI.QueryHistory) > MaxQueryHistory {
		c.UI.QueryHistory = c.UI.QueryHistory[len(c.UI.QueryHistory)-MaxQueryHistory:]
	}
}
