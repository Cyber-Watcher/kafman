package messages

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/Cyber-Watcher/kafman/internal/kafka"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/chrome"
	"github.com/Cyber-Watcher/kafman/internal/ui/components/form"
	"github.com/Cyber-Watcher/kafman/internal/ui/ktx"
)

// Button labels on the produce form.
const (
	buttonSend   = "Send"
	buttonCancel = "Cancel"
)

// produceForm publishes one record.
type produceForm struct {
	form  *form.Model
	topic string

	partitionMode *form.Radio
	partition     *form.Text
	key           *form.Text
	headers       *form.Text
	value         *form.Text

	sending bool
	err     error
}

// Partition modes.
const (
	partitionAuto   = "by key"
	partitionManual = "manual"
)

func newProduceForm(topic string) *produceForm {
	f := &produceForm{topic: topic}

	f.partitionMode = form.NewRadio("Partition", []string{partitionAuto, partitionManual}, 0)
	f.partition = form.NewText("Partition no.", "0").
		WithHint("used only in manual mode").
		WithValidator(func(s string) error {
			if s == "" {
				return nil
			}
			if _, err := strconv.Atoi(s); err != nil {
				return errors.New("must be a number")
			}
			return nil
		})

	f.key = form.NewText("Key", "").
		WithPlaceholder("ord-1234").
		WithHint("empty means no key, so the partition is random")

	f.headers = form.NewText("Headers", "").
		WithPlaceholder("trace-id=abc, source=kafman").
		WithValidator(func(s string) error {
			if s == "" {
				return nil
			}
			for _, pair := range strings.Split(s, ",") {
				if !strings.Contains(pair, "=") {
					return errors.New("use name=value, separated by commas")
				}
			}
			return nil
		})

	f.value = form.NewText("Value", "").
		WithPlaceholder(`{"id":"ord-1234","status":"NEW"}`).
		WithValidator(func(s string) error {
			// A payload that looks like JSON but is not would be published
			// happily and fail somewhere downstream instead.
			trimmed := strings.TrimSpace(s)
			if trimmed == "" {
				return nil
			}
			if trimmed[0] != '{' && trimmed[0] != '[' {
				return nil
			}
			if !json.Valid([]byte(trimmed)) {
				return errors.New("looks like JSON but does not parse")
			}
			return nil
		})

	f.form = form.New([]form.Field{
		f.partitionMode, f.partition, f.key, f.headers, f.value,
	}, buttonSend, buttonCancel)

	return f
}

// Name implements the overlay contract.
func (f *produceForm) Name() string { return "Produce" }

// Shortcuts implements ui.ShortcutProvider.
func (f *produceForm) Shortcuts() []chrome.Shortcut {
	return []chrome.Shortcut{
		{Key: "Tab", Label: "Next field"},
		{Key: "Enter", Label: "Send"},
		{Key: "Esc", Label: "Cancel"},
	}
}

// Update forwards to the form.
func (f *produceForm) Update(msg tea.Msg) tea.Cmd { return f.form.Update(msg) }

// Cancelled reports whether the user backed out.
func (f *produceForm) Cancelled() bool {
	return f.form.Cancelled() || f.form.Submitted() == buttonCancel
}

// Submitted reports whether Send was pressed, consuming the event.
func (f *produceForm) Submitted() bool {
	if f.form.Submitted() == buttonSend {
		f.form.Reset()
		return true
	}
	return false
}

func (f *produceForm) markSending() { f.sending = true }

func (f *produceForm) setError(err error) {
	f.sending = false
	f.err = err
}

// Request assembles the record to publish.
func (f *produceForm) Request() (kafka.ProduceRequest, error) {
	if !f.form.Valid() {
		return kafka.ProduceRequest{}, fmt.Errorf("fix the marked fields first")
	}

	req := kafka.ProduceRequest{
		Topic: f.topic,
		Value: []byte(f.value.Value()),
	}
	if key := f.key.Value(); key != "" {
		req.Key = []byte(key)
	}

	if f.partitionMode.Selected() == partitionManual {
		partition, err := strconv.Atoi(f.partition.Value())
		if err != nil {
			return kafka.ProduceRequest{}, fmt.Errorf("partition must be a number")
		}
		req.Manual, req.Partition = true, int32(partition)
	}

	for _, pair := range strings.Split(f.headers.Value(), ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		name, value, _ := strings.Cut(pair, "=")
		req.Headers = append(req.Headers, kafka.Header{
			Key:   strings.TrimSpace(name),
			Value: strings.TrimSpace(value),
		})
	}
	return req, nil
}

// View renders the form.
func (f *produceForm) View(k *ktx.Context) string {
	th := k.Theme
	width := min(k.WindowWidth-12, 90)

	lines := []string{
		th.Title.Render("Produce to " + f.topic),
		"",
		f.form.View(th, width),
	}

	switch {
	case f.sending:
		lines = append(lines, "", th.Warning.Render("sending…"))
	case f.err != nil:
		lines = append(lines, "", th.Critical.Render("✖ "+f.err.Error()))
	}

	return lipgloss.JoinVertical(lipgloss.Left, lines...)
}
