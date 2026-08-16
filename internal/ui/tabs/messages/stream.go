package messages

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// recordsMsg carries one batch and the session it came from, so the next wait
// can be scheduled without the model holding the channel itself.
type recordsMsg struct {
	batch   kafka.Batch
	session *kafka.ReadSession
}

// readEndedMsg reports that a read finished or failed.
type readEndedMsg struct {
	Err error
}

// awaitBatch blocks on the session's channel inside a command, so bubbletea's
// own goroutine does the waiting.
//
// Each delivered batch schedules the next wait, which is what turns a channel
// into a stream of messages. The batching itself happens in internal/kafka:
// without it a busy topic would deliver thousands of messages a second straight
// into the render loop.
func awaitBatch(session *kafka.ReadSession) tea.Cmd {
	if session == nil {
		return nil
	}
	return func() tea.Msg {
		batch, ok := <-session.Records
		if !ok {
			return readEndedMsg{Err: session.Err()}
		}
		return recordsMsg{batch: batch, session: session}
	}
}

// ktxTheme is an alias so the view code reads as though the theme were local.
type ktxTheme = theme.Theme

// humanCount groups digits with underscores, matching the rest of kafman.
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

// producedMsg reports the outcome of publishing a record.
type producedMsg struct {
	Result kafka.ProduceResult
	Err    error
}
