package tui

import (
	"path/filepath"

	"github.com/michaelscutari/dug/internal/entry"

	tea "github.com/charmbracelet/bubbletea"
)

// Update implements tea.Model.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKey(msg)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case dataLoadedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.scanMeta = msg.scanMeta
		m.currentPath = msg.scanMeta.RootPath
		m.filter = ""
		m.filterActive = false
		m.setEntries(msg.entries)
		m.rollup = msg.rollup
		return m, nil

	case entriesLoadedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.filter = ""
		m.filterActive = false
		m.setEntries(msg.entries)
		m.rollup = msg.rollup
		return m, nil
	}

	return m, nil
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.filterActive {
		switch msg.String() {
		case "enter":
			m.filterActive = false
			return m, nil

		case "esc":
			m.filterActive = false
			m.filter = ""
			m.applyFilter()
			return m, nil

		case "backspace":
			if len(m.filter) > 0 {
				runes := []rune(m.filter)
				m.filter = string(runes[:len(runes)-1])
				m.applyFilter()
			}
			return m, nil

		case "q", "ctrl+c":
			return m, tea.Quit
		}

		if msg.Type == tea.KeyRunes {
			m.filter += msg.String()
			m.applyFilter()
			return m, nil
		}

		return m, nil
	}

	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit

	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
		return m, nil

	case "down", "j":
		if m.cursor < len(m.entries)-1 {
			m.cursor++
		}
		return m, nil

	case "enter", "l", "right":
		if len(m.entries) > 0 && m.cursor < len(m.entries) {
			selected := m.entries[m.cursor]
			if selected.Kind == entry.KindDir {
				m.currentPath = selected.Path
				m.filter = ""
				m.filterActive = false
				return m, m.loadEntries(selected.Path)
			}
		}
		return m, nil

	case "backspace", "h", "left":
		if m.scanMeta != nil && m.currentPath != m.scanMeta.RootPath {
			parent := filepath.Dir(m.currentPath)
			m.currentPath = parent
			m.filter = ""
			m.filterActive = false
			return m, m.loadEntries(parent)
		}
		return m, nil

	case "s":
		m.sort = SortBySize
		return m, m.loadEntries(m.currentPath)

	case "d":
		m.sort = SortByDisk
		return m, m.loadEntries(m.currentPath)

	case "n":
		m.sort = SortByName
		return m, m.loadEntries(m.currentPath)

	case "f":
		m.sort = SortByFiles
		return m, m.loadEntries(m.currentPath)

	case "/":
		m.filterActive = true
		return m, nil

	case "home", "g":
		m.cursor = 0
		return m, nil

	case "end", "G":
		if len(m.entries) > 0 {
			m.cursor = len(m.entries) - 1
		}
		return m, nil

	case "pgup":
		m.cursor -= 10
		if m.cursor < 0 {
			m.cursor = 0
		}
		return m, nil

	case "pgdown":
		m.cursor += 10
		if m.cursor >= len(m.entries) {
			m.cursor = len(m.entries) - 1
		}
		if m.cursor < 0 {
			m.cursor = 0
		}
		return m, nil
	}

	return m, nil
}
