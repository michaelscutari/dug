package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	_ "modernc.org/sqlite"
)

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Display scan metadata",
	Long:  `Print metadata about a scan database including timestamps and statistics.`,
	RunE:  runInfo,
}

var infoDB string

func init() {
	infoCmd.Flags().StringVarP(&infoDB, "db", "d", "./data/latest.db", "Path to database file")
}

func runInfo(cmd *cobra.Command, args []string) error {
	database, err := sql.Open("sqlite", infoDB)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	var rootPath string
	var startTime, endTime int64
	var totalSize, totalBlocks, fileCount, dirCount, errorCount int64

	err = database.QueryRow(`
		SELECT root_path, start_time, COALESCE(end_time, 0), total_size, total_blocks, file_count, dir_count, error_count
		FROM scan_meta WHERE id = 1
	`).Scan(&rootPath, &startTime, &endTime, &totalSize, &totalBlocks, &fileCount, &dirCount, &errorCount)

	if err != nil {
		return fmt.Errorf("failed to read scan metadata: %w", err)
	}

	start := time.Unix(startTime, 0)
	end := time.Unix(endTime, 0)
	duration := end.Sub(start)

	fmt.Printf("Scan Information\n")
	fmt.Printf("================\n\n")
	fmt.Printf("Root Path:    %s\n", rootPath)
	fmt.Printf("Start Time:   %s\n", start.Format(time.RFC3339))
	if endTime > 0 {
		fmt.Printf("End Time:     %s\n", end.Format(time.RFC3339))
		fmt.Printf("Duration:     %s\n", duration.Round(time.Millisecond))
	}
	fmt.Printf("\nStatistics\n")
	fmt.Printf("----------\n")
	fmt.Printf("Files:         %s\n", humanize.Comma(fileCount))
	fmt.Printf("Directories:   %s\n", humanize.Comma(dirCount))
	fmt.Printf("Apparent Size: %s\n", humanize.Bytes(uint64(totalSize)))
	fmt.Printf("Disk Usage:    %s\n", humanize.Bytes(uint64(totalBlocks)))
	if errorCount > 0 {
		fmt.Printf("Errors:        %s\n", humanize.Comma(errorCount))
	}

	return nil
}
