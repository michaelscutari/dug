package main

import (
	"database/sql"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/dustin/go-humanize"
	"github.com/michaelscutari/dug/internal/db"
	"github.com/spf13/cobra"

	_ "modernc.org/sqlite"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query the database non-interactively",
	Long:  `Query the scan database and output results for scripting.`,
	RunE:  runQuery,
}

var (
	queryDB    string
	queryPath  string
	querySort  string
	queryLimit int
)

func init() {
	queryCmd.Flags().StringVarP(&queryDB, "db", "d", "./data/latest.db", "Path to database file")
	queryCmd.Flags().StringVarP(&queryPath, "path", "p", "", "Directory path to query")
	queryCmd.Flags().StringVarP(&querySort, "sort", "s", "size", "Sort by: size, disk, name, files")
	queryCmd.Flags().IntVarP(&queryLimit, "limit", "n", 20, "Maximum number of results")
}

func runQuery(cmd *cobra.Command, args []string) error {
	database, err := sql.Open("sqlite", queryDB)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	// If no path specified, get root from scan_meta
	if queryPath == "" {
		err := database.QueryRow(`SELECT root_path FROM scan_meta WHERE id = 1`).Scan(&queryPath)
		if err != nil {
			return fmt.Errorf("failed to get root path: %w", err)
		}
	}

	entries, err := db.LoadChildren(database, queryPath, querySort, queryLimit)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "APPARENT\tDISK\tFILES\tDIRS\tNAME\n")
	for _, e := range entries {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			humanize.Bytes(uint64(e.TotalSize)),
			humanize.Bytes(uint64(e.TotalBlocks)),
			humanize.Comma(e.TotalFiles),
			humanize.Comma(e.TotalDirs),
			e.Name,
		)
	}
	w.Flush()

	return nil
}
