package pkg

import (
	"errors"
)

// ////////////////////////////////////////////////////////////////////////////////
// // High level API for StableStore                                        	 //
// ////////////////////////////////////////////////////////////////////////////////

// initStore stores zero-index log
func (n *Node) InitStableStore() {
	n.StoreLog(&LogEntry{
		Index:  0,
		TermId: 0,
		Type:   CommandType_NOOP,
		Data:   []byte{},
	})
}

// setCurrentTerm sets the current node's term and writes log to disk
func (n *Node) SetCurrentTerm(newTerm uint64) {
	currentTerm := n.GetCurrentTerm()
	if newTerm != currentTerm {
		n.Out("Setting current term from %v -> %v", currentTerm, newTerm)
	}
	err := n.StableStore.SetUint64([]byte("current_term"), newTerm)
	if err != nil {
		n.Error("Unable to flush new term to disk: %v", err)
		panic(err)
	}
}

// GetCurrentTerm returns the current node's term
func (n *Node) GetCurrentTerm() uint64 {
	return n.StableStore.GetUint64([]byte("current_term"))
}

// setVotedFor sets the candidateId for which the current node voted for, and writes log to disk
func (n *Node) setVotedFor(candidateID string) {
	err := n.StableStore.SetBytes([]byte("voted_for"), []byte(candidateID))
	if err != nil {
		n.Error("Unable to flush new votedFor to disk: %v", err)
		panic(err)
	}
}

// GetVotedFor returns the Id of the candidate that the current node voted for
func (n *Node) GetVotedFor() string {
	return string(n.StableStore.GetBytes([]byte("voted_for")))
}

// CacheClientReply caches the given client response with the provided cache ID.
func (n *Node) CacheClientReply(cacheID string, reply ClientReply) error {
	key := []byte("cacheID:" + cacheID)
	if value := n.StableStore.GetBytes(key); value != nil {
		return errors.New("request with the same clientId and seqNum already exists")
	}

	buf, err := encodeMsgPack(reply)
	if err != nil {
		return err
	}
	err = n.StableStore.SetBytes(key, buf.Bytes())
	if err != nil {
		n.Error("Unable to flush new client request to disk: %v", err)
		panic(err)
	}
	return nil
}

// GetCachedReply checks if the given client request has a cached response.
// It returns the cached response (or nil) and a boolean indicating whether or not
// a cached response existed.
func (n *Node) GetCachedReply(cacheID string) (*ClientReply, bool) {
	key := []byte("cacheID:" + cacheID)

	if value := n.StableStore.GetBytes(key); value != nil {
		var reply ClientReply
		if err := decodeMsgPack(value, &reply); err != nil {
			panic(err)
		}
		return &reply, true
	}
	return nil, false
}

// LastLogIndex returns index of last log. If no log exists, it returns 0.
func (n *Node) LastLogIndex() uint64 {
	return n.StableStore.LastLogIndex()
}

// StoreLog appends log to log entry. Should always succeed
func (n *Node) StoreLog(log *LogEntry) {
	err := n.StableStore.StoreLog(log)
	if err != nil {
		panic(err)
	}
}


// GetLog gets a log at a specific index. If log does not exist, GetLog returns nil
func (n *Node) GetLog(index uint64) *LogEntry {
	return n.StableStore.GetLog(index)
}

// TruncateLog deletes logs from index to end of logs. Should always succeed
func (n *Node) TruncateLog(index uint64) {
	err := n.StableStore.TruncateLog(index)
	if err != nil {
		panic(err)
	}
}

// RemoveLogs removes data from stableStore
func (n *Node) RemoveLogs() {
	n.StableStore.Remove()
}
