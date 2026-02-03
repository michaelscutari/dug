package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "0.1.0"

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "dug",
	Short: "A filesystem profiler for large-scale NFS environments",
	Long: `dug is a filesystem profiler that scans directories and stores
metadata in SQLite for analysis. It provides a TUI browser for
exploring disk usage.`,
}

func init() {
	rootCmd.Version = version
	rootCmd.AddCommand(scanCmd)
	rootCmd.AddCommand(tuiCmd)
	rootCmd.AddCommand(infoCmd)
	rootCmd.AddCommand(queryCmd)
}
