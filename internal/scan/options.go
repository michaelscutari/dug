package scan

import "regexp"

// ScanOptions configures the scanning behavior.
type ScanOptions struct {
	// Workers is the number of concurrent directory processors.
	Workers int

	// Xdev prevents crossing filesystem boundaries.
	Xdev bool

	// MaxErrors is the maximum number of errors before aborting.
	// Zero means unlimited.
	MaxErrors int

	// ExcludePatterns are regular expressions for paths to skip.
	ExcludePatterns []*regexp.Regexp

	// BatchSize is the number of entries to batch before flushing to DB.
	BatchSize int

	// FlushInterval is the maximum time between flushes in milliseconds.
	FlushIntervalMs int
}

// DefaultOptions returns sensible defaults for scanning.
func DefaultOptions() *ScanOptions {
	opts := &ScanOptions{
		Workers:         8,
		Xdev:            true,
		MaxErrors:       0,
		ExcludePatterns: nil,
		BatchSize:       10000,
		FlushIntervalMs: 1000,
	}
	// Exclude NFS snapshot directories by default
	opts.AddExcludePattern(`/\.snapshot(/|$)`)
	return opts
}

// WithWorkers sets the number of workers.
func (o *ScanOptions) WithWorkers(n int) *ScanOptions {
	o.Workers = n
	return o
}

// WithXdev sets cross-device behavior.
func (o *ScanOptions) WithXdev(xdev bool) *ScanOptions {
	o.Xdev = xdev
	return o
}

// WithMaxErrors sets the maximum error count.
func (o *ScanOptions) WithMaxErrors(n int) *ScanOptions {
	o.MaxErrors = n
	return o
}

// AddExcludePattern adds a pattern to exclude.
func (o *ScanOptions) AddExcludePattern(pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	o.ExcludePatterns = append(o.ExcludePatterns, re)
	return nil
}

// ShouldExclude checks if a path matches any exclude pattern.
func (o *ScanOptions) ShouldExclude(path string) bool {
	for _, re := range o.ExcludePatterns {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}
