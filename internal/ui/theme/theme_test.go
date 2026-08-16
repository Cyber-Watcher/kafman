package theme

import "testing"

// The cluster chip takes its background from the user's config, so the text
// colour has to be derived rather than fixed: white on yellow is unreadable.
func TestContrastingText(t *testing.T) {
	cases := []struct {
		background string
		want       string
		why        string
	}{
		{"#FFFF00", "#000000", "yellow is bright"},
		{"#00FF00", "#000000", "green is bright"},
		{"#FFFFFF", "#000000", "white is bright"},
		{"#00D787", "#000000", "the default cluster green is bright"},
		{"#000080", "#FFFFFF", "navy is dark"},
		{"#FF0000", "#FFFFFF", "red is dark to the eye despite full intensity"},
		{"#7571F9", "#FFFFFF", "the accent indigo is dark"},
		{"#000000", "#FFFFFF", "black is dark"},
		{"not-a-colour", "#FFFFFF", "an unparsable value falls back to white"},
		{"", "#FFFFFF", "an empty value falls back to white"},
	}

	for _, tc := range cases {
		t.Run(tc.background, func(t *testing.T) {
			if got := contrastingText(tc.background); got != tc.want {
				t.Errorf("contrastingText(%q) = %q, want %q — %s",
					tc.background, got, tc.want, tc.why)
			}
		})
	}
}

func TestParseHex(t *testing.T) {
	r, g, b, ok := parseHex("#7571F9")
	if !ok {
		t.Fatal("a valid hex colour should parse")
	}
	if r != 0x75 || g != 0x71 || b != 0xF9 {
		t.Errorf("parseHex = (%d, %d, %d), want (117, 113, 249)", r, g, b)
	}

	for _, invalid := range []string{"", "#FFF", "FFFFFF", "#GGGGGG", "#1234567"} {
		if _, _, _, ok := parseHex(invalid); ok {
			t.Errorf("parseHex(%q) should have failed", invalid)
		}
	}
}

func TestByNameFallsBackToDark(t *testing.T) {
	if got := ByName("light", false); got.Name != "light" {
		t.Errorf("ByName(light) = %q", got.Name)
	}
	if got := ByName("neon", false); got.Name != "dark" {
		t.Errorf("an unknown theme should fall back to dark, got %q", got.Name)
	}
}

func TestGlyphHonoursPlainFonts(t *testing.T) {
	nerd := New(Dark, false)
	plain := New(Dark, true)

	if got := nerd.Glyph("●", "*"); got != "●" {
		t.Errorf("with Nerd Fonts on, Glyph = %q", got)
	}
	if got := plain.Glyph("●", "*"); got != "*" {
		t.Errorf("with plain fonts on, Glyph = %q", got)
	}
}
