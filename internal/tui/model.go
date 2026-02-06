package tui

import (
	"database/sql"
	"strings"

	"github.com/michaelscutari/dug/internal/db"
	"github.com/michaelscutari/dug/internal/entry"

	tea "github.com/charmbracelet/bubbletea"
)

// SortColumn represents the current sort field.
type SortColumn int

const (
	SortBySize SortColumn = iota
	SortByDisk
	SortByName
	SortByFiles
)

func (s SortColumn) String() string {
	switch s {
	case SortByDisk:
		return "disk"
	case SortByName:
		return "name"
	case SortByFiles:
		return "files"
	default:
		return "size"
	}
}

// Model holds the TUI state.
type Model struct {
	db           *sql.DB
	currentPath  string
	allEntries   []db.DisplayEntry
	entries      []db.DisplayEntry
	cursor       int
	sort         SortColumn
	width        int
	height       int
	scanMeta     *entry.ScanMeta
	rollup       *entry.Rollup
	filter       string
	filterActive bool
	err          error
}

// NewModel creates a new TUI model.
func NewModel(database *sql.DB) *Model {
	return &Model{
		db:   database,
		sort: SortBySize,
	}
}

// Init implements tea.Model.
func (m *Model) Init() tea.Cmd {
	return m.loadInitialData
}

type dataLoadedMsg struct {
	scanMeta *entry.ScanMeta
	entries  []db.DisplayEntry
	rollup   *entry.Rollup
	err      error
}

func (m *Model) loadInitialData() tea.Msg {
	meta, err := db.GetScanMeta(m.db)
	if err != nil {
		return dataLoadedMsg{err: err}
	}

	entries, err := db.LoadChildren(m.db, meta.RootPath, m.sort.String(), 1000)
	if err != nil {
		return dataLoadedMsg{err: err}
	}

	rollup, err := db.GetRollup(m.db, meta.RootPath)
	if err != nil {
		return dataLoadedMsg{err: err}
	}

	return dataLoadedMsg{
		scanMeta: meta,
		entries:  entries,
		rollup:   rollup,
	}
}

type entriesLoadedMsg struct {
	entries []db.DisplayEntry
	rollup  *entry.Rollup
	err     error
}

func (m *Model) loadEntries(path string) tea.Cmd {
	return func() tea.Msg {
		entries, err := db.LoadChildren(m.db, path, m.sort.String(), 1000)
		if err != nil {
			return entriesLoadedMsg{err: err}
		}

		rollup, _ := db.GetRollup(m.db, path)

		return entriesLoadedMsg{
			entries: entries,
			rollup:  rollup,
		}
	}
}

func (m *Model) helpLine() string {
	if m.filterActive {
		return "Type to filter | Enter: apply | Esc: clear | q: quit"
	}
	return "↑/↓ move | Enter: open | Backspace: close | s/d/n/f: sort | /: filter | q: quit"
}

func (m *Model) setEntries(entries []db.DisplayEntry) {
	m.allEntries = entries
	m.applyFilter()
}

func (m *Model) applyFilter() {
	if m.filter == "" {
		m.entries = m.allEntries
	} else {
		filtered := make([]db.DisplayEntry, 0, len(m.allEntries))
		needle := strings.ToLower(m.filter)
		for _, e := range m.allEntries {
			if strings.Contains(strings.ToLower(e.Name), needle) {
				filtered = append(filtered, e)
			}
		}
		m.entries = filtered
	}
	m.cursor = 0
}
