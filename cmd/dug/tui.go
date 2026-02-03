package main

import (
	"database/sql"
	"fmt"

	"github.com/michaelscutari/dug/internal/db"
	"github.com/michaelscutari/dug/internal/tui"
	"github.com/spf13/cobra"

	tea "github.com/charmbracelet/bubbletea"
	_ "modernc.org/sqlite"
)

var tuiCmd = &cobra.Command{
	Use:   "tui",
	Short: "Browse a scan database interactively",
	Long:  `Open an interactive TUI to browse the filesystem tree and view disk usage.`,
	RunE:  runTUI,
}

var tuiDB string

func init() {
	tuiCmd.Flags().StringVarP(&tuiDB, "db", "d", "./data/latest.db", "Path to database file")
}

func runTUI(cmd *cobra.Command, args []string) error {
	database, err := sql.Open("sqlite", tuiDB)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	if err := db.ApplyReadPragmas(database); err != nil {
		return fmt.Errorf("failed to apply pragmas: %w", err)
	}

	model := tui.NewModel(database)
	p := tea.NewProgram(model, tea.WithAltScreen())

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}

	return nil
}
