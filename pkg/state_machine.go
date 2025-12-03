package pkg

// StateMachine is a general interface defining the methods a Raft state machine
// should implement. For this project, we use a HashMachine as our state machine.
type StateMachine interface {
	// Useful for testing once you use type assertions to convert the state
	GetState() (state interface{})

	// Apply log is invoked once a log entry is committed.
	ApplyCommand(command uint64, data []byte) ([]byte, error)

	//
	// Snapshot is used to support log compaction. Returns an array of bytes
	// to store in some snapshot store
	// Snapshot() ([]byte, error)

	// Restore is used to restore an FSM from a snapshot
	// Restore([]byte)
}
