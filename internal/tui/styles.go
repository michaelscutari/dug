package tui

import (
	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
)

var (
	// Colors
	colorPrimary   = lipgloss.Color("39")  // Blue
	colorSecondary = lipgloss.Color("245") // Gray
	colorHighlight = lipgloss.Color("212") // Pink
	colorSuccess   = lipgloss.Color("76")  // Green
	colorWarning   = lipgloss.Color("214") // Orange
	colorMuted     = lipgloss.Color("240") // Dark gray

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
			Foreground(lipgloss.Color("0")).
			Background(colorPrimary)

	dirStyle = lipgloss.NewStyle().
			Foreground(colorPrimary).
			Bold(true)

	fileStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255"))

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
