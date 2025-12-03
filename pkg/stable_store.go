package pkg

// StableStore provides an interface for storage engines to implement so
// that they can be used as a raft node's persistent storage
type StableStore interface {
	SetBytes(key, value []byte) error
	// Return nil if key does not exist
	GetBytes(key []byte) []byte

	SetUint64(key []byte, value uint64) error
	// Return 0 if key does not exist
	GetUint64(key []byte) uint64

	// Log storage
	StoreLog(log *LogEntry) error
	GetLog(index uint64) *LogEntry
	LastLogIndex() uint64
	TruncateLog(index uint64) error

	// For testing
	AllLogs() []*LogEntry
	// For testing or else db file will be indefinitely locked
	Close()
	// For testing purposes
	Remove()
	// For testing purposes
	Path() string
}
