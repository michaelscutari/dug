package tui

import (
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
)

var (
	// Colors
	colorPrimary   = lipgloss.AdaptiveColor{Light: "#005B9A", Dark: "#4FA3FF"}
	colorText      = lipgloss.AdaptiveColor{Light: "#1F1F1F", Dark: "#E6E6E6"}
	colorSecondary = lipgloss.AdaptiveColor{Light: "#4A4A4A", Dark: "#9A9A9A"}
	colorHighlight = lipgloss.AdaptiveColor{Light: "#C2185B", Dark: "#FF6FB3"}
	colorSuccess   = lipgloss.AdaptiveColor{Light: "#0B7A5F", Dark: "#6EE7B7"}
	colorWarning   = lipgloss.AdaptiveColor{Light: "#B45309", Dark: "#F59E0B"}
	colorMuted     = lipgloss.AdaptiveColor{Light: "#666666", Dark: "#6F6F6F"}
	colorSelectBg  = lipgloss.AdaptiveColor{Light: "#DDEBFF", Dark: "#2B4C7E"}
	colorSelectFg  = lipgloss.AdaptiveColor{Light: "#000000", Dark: "#FFFFFF"}

	// Styles
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorPrimary).
			MarginBottom(1)

	pathStyle = lipgloss.NewStyle().
			Foreground(colorSecondary).
			MarginBottom(1)

	breadcrumbStyle = lipgloss.NewStyle().
			Foreground(colorSecondary)

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorMuted).
			BorderStyle(lipgloss.NormalBorder()).
			BorderBottom(true).
			BorderForeground(colorMuted)

	selectedStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorSelectFg).
			Background(colorSelectBg)

	dirStyle = lipgloss.NewStyle().
			Foreground(colorPrimary).
			Bold(true)

	fileStyle = lipgloss.NewStyle().
			Foreground(colorText)

	symlinkStyle = lipgloss.NewStyle().
			Foreground(colorHighlight)

	sizeStyle = lipgloss.NewStyle().
			Foreground(colorSuccess).
			Width(10).
			Align(lipgloss.Right)

	countStyle = lipgloss.NewStyle().
			Foreground(colorWarning).
			Width(8).
			Align(lipgloss.Right)

	helpStyle = lipgloss.NewStyle().
			Foreground(colorMuted).
			MarginTop(1)

	statusStyle = lipgloss.NewStyle().
			Foreground(colorSecondary)

	filterStyle = lipgloss.NewStyle().
			Foreground(colorWarning)

	statsStyle = lipgloss.NewStyle().
			Foreground(colorSecondary).
			MarginBottom(1)
)

// FormatSize formats a byte count for display.
func FormatSize(bytes int64) string {
	return humanize.Bytes(uint64(bytes))
}

// FormatCount formats a count for display.
func FormatCount(n int64) string {
	return humanize.Comma(n)
}
