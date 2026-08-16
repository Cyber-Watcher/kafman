// Package ktx carries the program-wide context threaded through every View.
//
// The height budget is the important part: instead of every screen computing
// its own layout arithmetic, each rendered chunk subtracts the height it used
// and whatever draws last sizes itself from the remainder. The idea is borrowed
// from ktea's ProgramKtx.
package ktx

import (
	"github.com/Cyber-Watcher/kafman/internal/config"
	"github.com/Cyber-Watcher/kafman/internal/ui/theme"
)

// Minimum usable terminal size. Below this the app shows a notice instead of a
// broken layout.
const (
	MinWidth  = 60
	MinHeight = 15
)

// Context is passed to every View. It is recreated on each render pass, so the
// height budget starts fresh every frame.
type Context struct {
	Config *config.Config
	Theme  *theme.Theme

	// WindowWidth and WindowHeight are the full terminal dimensions.
	WindowWidth  int
	WindowHeight int

	// AvailableHeight is what is left for the current View after the chrome
	// around it has taken its share.
	AvailableHeight int
}

// New builds a context for one render pass.
func New(cfg *config.Config, th *theme.Theme, width, height int) *Context {
	return &Context{
		Config:          cfg,
		Theme:           th,
		WindowWidth:     width,
		WindowHeight:    height,
		AvailableHeight: height,
	}
}

// HeightUsed records that a rendered chunk consumed h rows.
func (c *Context) HeightUsed(h int) {
	c.AvailableHeight -= h
	if c.AvailableHeight < 0 {
		c.AvailableHeight = 0
	}
}

// TooSmall reports whether the terminal is below the usable minimum.
func (c *Context) TooSmall() bool {
	return c.WindowWidth < MinWidth || c.WindowHeight < MinHeight
}

// Narrow reports whether panels should stack into a single column instead of
// sitting side by side.
func (c *Context) Narrow() bool {
	return c.WindowWidth < 100
}

// Clone returns a copy, so a nested View can spend its own height budget
// without affecting its siblings.
func (c *Context) Clone() *Context {
	copied := *c
	return &copied
}

// WithHeight returns a copy whose available height is h.
func (c *Context) WithHeight(h int) *Context {
	copied := c.Clone()
	copied.AvailableHeight = h
	return copied
}
