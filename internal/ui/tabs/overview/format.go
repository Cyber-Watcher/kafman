package overview

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/x/ansi"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
)

// humanBytes renders a byte count the way a person reads it.
func humanBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTP"[exp])
}

// humanCount groups digits with underscores, matching how kafman renders every
// other large number. Underscores rather than commas because they read as a
// separator in a table without looking like decimal points.
func humanCount(n int64) string {
	if n < 0 {
		return "-" + humanCount(-n)
	}
	digits := fmt.Sprint(n)
	if len(digits) <= 4 {
		return digits
	}

	var b strings.Builder
	lead := len(digits) % 3
	if lead > 0 {
		b.WriteString(digits[:lead])
	}
	for i := lead; i < len(digits); i += 3 {
		if b.Len() > 0 {
			b.WriteByte('_')
		}
		b.WriteString(digits[i : i+3])
	}
	return b.String()
}

// sortBySize orders topics by their on-disk size, largest first, falling back to
// the name so a refresh cannot shuffle equal rows.
func sortBySize(topics []kafka.Topic, sizes map[string]int64) {
	sort.SliceStable(topics, func(i, j int) bool {
		a, b := sizes[topics[i].Name], sizes[topics[j].Name]
		if a != b {
			return a > b
		}
		return topics[i].Name < topics[j].Name
	})
}

// padRight pads by display width, never by len(): the quorum column carries a
// multi-byte ★, and byte counting shifted every column after it.
func padRight(s string, width int) string {
	if w := ansi.StringWidth(s); w < width {
		return s + strings.Repeat(" ", width-w)
	}
	return s
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func truncate(s string, width int) string {
	return ansi.Truncate(s, width, "…")
}
