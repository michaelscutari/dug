package pathutil

import "path/filepath"

// Normalize returns a canonical filesystem path string.
// It removes trailing slashes, collapses "." and "..", and
// preserves relative paths when provided.
func Normalize(path string) string {
	if path == "" {
		return path
	}
	return filepath.Clean(path)
}
