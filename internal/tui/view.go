package tui

import (
	"fmt"
	"strings"

	"github.com/michaelscutari/dug/internal/db"
	"github.com/michaelscutari/dug/internal/entry"
)

// View implements tea.Model.
func (m *Model) View() string {
	if m.err != nil {
		return fmt.Sprintf("Error: %v\n\nPress q to quit.", m.err)
	}

	if m.scanMeta == nil {
		return "Loading..."
	}

	var b strings.Builder
	headerLines := 0

	writeLine := func(line string) {
		b.WriteString(line)
		b.WriteString("\n")
		headerLines++
	}

	// Header
	writeLine(titleStyle.Render("dug - Disk Usage Browser"))

	// Scan info - show both sizes
	scanInfo := fmt.Sprintf("Scan: %s | Apparent: %s | Disk: %s | Files: %s",
		m.scanMeta.StartTime.Format("2006-01-02 15:04"),
		FormatSize(m.scanMeta.TotalSize),
		FormatSize(m.scanMeta.TotalBlocks),
		FormatCount(m.scanMeta.FileCount),
	)
	writeLine(statsStyle.Render(scanInfo))

	// Breadcrumbs / path
	pathLabel := fmt.Sprintf("Path: %s", truncateMiddle(m.currentPath, max(10, m.width-6)))
	writeLine(breadcrumbStyle.Render(pathLabel))

	// Current directory stats
	if m.rollup != nil {
		dirInfo := fmt.Sprintf("Apparent: %s | Disk: %s | %s files | %s subdirs",
			FormatSize(m.rollup.TotalSize),
			FormatSize(m.rollup.TotalBlocks),
			FormatCount(m.rollup.TotalFiles),
			FormatCount(m.rollup.TotalDirs),
		)
		writeLine(statsStyle.Render(dirInfo))
	}

	// Status line
	status := fmt.Sprintf("Items: %s", FormatCount(int64(len(m.entries))))
	if m.filter != "" {
		status += fmt.Sprintf(" | Filter: %q", m.filter)
	}
	if len(m.entries) > 0 && m.cursor < len(m.entries) {
		sel := m.entries[m.cursor]
		status += fmt.Sprintf(" | Sel: %s (%s/%s)",
			sel.Name, FormatSize(sel.TotalSize), FormatSize(sel.TotalBlocks))
	}
	writeLine(statusStyle.Render(status))

	// Filter input
	if m.filterActive {
		filterLine := fmt.Sprintf("Filter: %s_", m.filter)
		writeLine(filterStyle.Render(filterLine))
	} else if m.filter != "" {
		filterLine := fmt.Sprintf("Filter: %s", m.filter)
		writeLine(filterStyle.Render(filterLine))
	}

	// Column headers with sort indicator
	apparentLabel := headerLabel("APPARENT", m.sort == SortBySize, "v")
	diskLabel := headerLabel("DISK", m.sort == SortByDisk, "v")
	filesLabel := headerLabel("FILES", m.sort == SortByFiles, "v")
	nameLabel := headerLabel("NAME", m.sort == SortByName, "^")

	// Calculate visible rows
	footerLines := 2
	visibleRows := m.height - headerLines - footerLines
	if visibleRows < 5 {
		visibleRows = 5
	}

	// Determine scroll offset
	startIdx := 0
	if m.cursor >= visibleRows {
		startIdx = m.cursor - visibleRows + 1
	}
	endIdx := min(len(m.entries), startIdx+visibleRows)

	widths := calcColumnWidths(m.entries, startIdx, endIdx, apparentLabel, diskLabel, filesLabel, "DIRS")
	nameWidth := calcNameWidth(m.width, widths)

	nameLabel = truncateRight(nameLabel, nameWidth)
	header := fmt.Sprintf("%*s %*s %*s %*s  %s",
		widths.apparent, apparentLabel,
		widths.disk, diskLabel,
		widths.files, filesLabel,
		widths.dirs, "DIRS",
		nameLabel,
	)
	writeLine(headerStyle.Render(header))

	// Entries
	for i := startIdx; i < endIdx; i++ {
		e := m.entries[i]
		line := m.formatEntry(e, i == m.cursor, widths, nameWidth)
		b.WriteString(line)
		b.WriteString("\n")
	}

	// Pad if needed
	displayedRows := min(len(m.entries)-startIdx, visibleRows)
	for i := displayedRows; i < visibleRows; i++ {
		b.WriteString("\n")
	}

	// Footer
	b.WriteString("\n")
	help := m.helpLine()
	if len(m.entries) > 0 {
		help = fmt.Sprintf("%s [%d/%d]", help, m.cursor+1, len(m.entries))
	}
	b.WriteString(helpStyle.Render(help))

	return b.String()
}

type columnWidths struct {
	apparent int
	disk     int
	files    int
	dirs     int
}

func calcColumnWidths(entries []db.DisplayEntry, startIdx, endIdx int, apparentLabel, diskLabel, filesLabel, dirsLabel string) columnWidths {
	w := columnWidths{
		apparent: len(apparentLabel),
		disk:     len(diskLabel),
		files:    len(filesLabel),
		dirs:     len(dirsLabel),
	}

	for i := startIdx; i < endIdx; i++ {
		e := entries[i]
		apparent := len(FormatSize(e.TotalSize))
		disk := len(FormatSize(e.TotalBlocks))
		files := len(FormatCount(e.TotalFiles))
		dirs := len(FormatCount(e.TotalDirs))

		if apparent > w.apparent {
			w.apparent = apparent
		}
		if disk > w.disk {
			w.disk = disk
		}
		if files > w.files {
			w.files = files
		}
		if dirs > w.dirs {
			w.dirs = dirs
		}
	}

	return w
}

func calcNameWidth(totalWidth int, w columnWidths) int {
	// columns + spaces: a + d + f + r + 2 spaces between first 4 + two spaces before name
	used := w.apparent + w.disk + w.files + w.dirs + 5
	nameWidth := totalWidth - used
	if nameWidth < 10 {
		nameWidth = 10
	}
	return nameWidth
}

func truncateRight(s string, maxLen int) string {
	if maxLen <= 0 || len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func (m *Model) formatEntry(e db.DisplayEntry, selected bool, widths columnWidths, nameWidth int) string {
	// Format sizes
	apparent := FormatSize(e.TotalSize)
	disk := FormatSize(e.TotalBlocks)

	// Format counts
	files := FormatCount(e.TotalFiles)
	dirs := FormatCount(e.TotalDirs)

	// Format name with type indicator
	var name string
	switch e.Kind {
	case entry.KindDir:
		name = dirStyle.Render(e.Name + "/")
	case entry.KindSymlink:
		name = symlinkStyle.Render(e.Name + "@")
	default:
		name = fileStyle.Render(e.Name)
	}

	name = truncateRight(name, nameWidth)
	line := fmt.Sprintf("%*s %*s %*s %*s  %s",
		widths.apparent, apparent,
		widths.disk, disk,
		widths.files, files,
		widths.dirs, dirs,
		name,
	)

	if selected {
		return selectedStyle.Render(line)
	}
	return line
}

func headerLabel(label string, active bool, dir string) string {
	if active {
		return label + dir
	}
	return label
}

func truncateMiddle(s string, maxLen int) string {
	if maxLen <= 0 || len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	head := (maxLen - 3) / 2
	tail := maxLen - 3 - head
	return s[:head] + "..." + s[len(s)-tail:]
}
