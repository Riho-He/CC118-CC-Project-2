package pkg

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to LeaderState")
	n.setState(LeaderState)

	// Initialize leader state
	for _, peer := range n.Peers {
		n.nextIndex[peer.Id] = n.LastLogIndex() + 1
		n.matchIndex[peer.Id] = 0
	}

	// Send initial heartbeat
	fallback := n.sendHeartbeats()
	if fallback {
		return n.doFollower
	}

	ticker := time.NewTicker(n.Config.HeartbeatTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-n.gracefulExit:
			return nil

		case <-ticker.C:
			fallback := n.sendHeartbeats()
			if fallback {
				return n.doFollower
			}

		case msg := <-n.clientRequest:
			// Leader Logic:
			// 1. Create LogEntry
			// 2. Append to local log
			// 3. Cache the response channel so we can reply after commit

			entry := &LogEntry{
				Index:   n.LastLogIndex() + 1,
				TermId:  n.GetCurrentTerm(),
				Type:    CommandType_STATE_MACHINE_COMMAND, // Default to StateMachine command
				Command: msg.request.StateMachineCmd,
				Data:    msg.request.Data,
				CacheId: CreateCacheID(msg.request.ClientId, msg.request.SequenceNum),
			}

			// Handle registration specifically if needed, though usually handled by CommandType
			if msg.request.StateMachineCmd == 0 && len(msg.request.Data) > 0 && string(msg.request.Data) == "register" {
				// Simple heuristic if registration uses specific data/cmd, 
				// but Proto definition has CommandType_CLIENT_REGISTRATION.
				// However, client_request struct does not expose CommandType directly.
				// Based on test cases, Init is StateMachineCmd.
				// We'll stick to STATE_MACHINE_COMMAND unless specific logic requires otherwise.
				// Note: Test cases use HashChainInit which is a command, not registration type.
				// The proto defines CLIENT_REGISTRATION but the client uses RegisterClientRPC which is separate?
				// Actually RegisterClientRPC sends a ClientRequest with ID=0.
			}
			
			// If it's a registration request (ClientId=0), we might treat it differently
			// based on hints, but here we just log it.
			if msg.request.ClientId == 0 {
				entry.Type = CommandType_CLIENT_REGISTRATION
			}

			n.StoreLog(entry)

			// Store the reply channel
			n.requestsMutex.Lock()
			if _, ok := n.requestsByCacheID[entry.CacheId]; !ok {
				n.requestsByCacheID[entry.CacheId] = make([]chan ClientReply, 0)
			}
			n.requestsByCacheID[entry.CacheId] = append(n.requestsByCacheID[entry.CacheId], msg.reply)
			n.requestsMutex.Unlock()
			
			// Try to replicate immediately (optional, but good for latency)
			// n.sendHeartbeats() 

		case msg := <-n.appendEntries:
			// If another leader exists with higher term
			_, fallback := n.handleAppendEntries(msg)
			if fallback {
				return n.doFollower
			}

		case msg := <-n.requestVote:
			fallback := n.handleRequestVote(msg)
			if fallback {
				return n.doFollower
			}
		}
	}
}

// sendHeartbeats is used by the leader to send out heartbeats to each of
// the other nodes. It returns true if the leader should fall back to the
// follower state. (This happens if we discover that we are in an old term.)
func (n *Node) sendHeartbeats() (fallback bool) {
	peers := n.getPeers()
	currentTerm := n.GetCurrentTerm()

	var wg sync.WaitGroup
	// We need to track matchIndexes to update commitIndex later
	// Using the node state map directly since we are single threaded here wrt state updates
	// except for the goroutines updating values.

	for _, peer := range peers {
		if peer.Id == n.Self.Id {
			continue
		}

		wg.Add(1)
		go func(peer *RemoteNode) {
			defer wg.Done()

			// Determine prevLogIndex and prevLogTerm
			nextIdx := n.nextIndex[peer.Id]
			prevLogIndex := nextIdx - 1
			prevLogTerm := uint64(0)
			if prevLogIndex > 0 {
				if log := n.GetLog(prevLogIndex); log != nil {
					prevLogTerm = log.TermId
				}
			}

			// Fetch entries to send
			var entries []*LogEntry
			lastLogIdx := n.LastLogIndex()
			if lastLogIdx >= nextIdx {
				for i := nextIdx; i <= lastLogIdx; i++ {
					entries = append(entries, n.GetLog(i))
				}
			}

			request := &AppendEntriesRequest{
				Term:         currentTerm,
				Leader:       n.Self,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.CommitIndex.Load(),
			}

			reply, err := peer.AppendEntriesRPC(n, request)

			if err != nil {
				return
			}

			if reply.Term > currentTerm {
				n.SetCurrentTerm(reply.Term)
				n.setVotedFor("")
				fallback = true
				return
			}

			if reply.Success {
				// Update nextIndex and matchIndex
				newNext := prevLogIndex + uint64(len(entries)) + 1
				newMatch := prevLogIndex + uint64(len(entries))
				
				// Ensure we don't regress indices due to out-of-order replies
				if newNext > n.nextIndex[peer.Id] {
					n.nextIndex[peer.Id] = newNext
				}
				if newMatch > n.matchIndex[peer.Id] {
					n.matchIndex[peer.Id] = newMatch
				}
			} else {
				// Decrement nextIndex and retry (handled next heartbeat)
				// Simple optimization: decrement by 1, or skip back more
				if n.nextIndex[peer.Id] > 1 {
					n.nextIndex[peer.Id]--
				}
			}
		}(peer)
	}

	wg.Wait()

	if fallback {
		return true
	}

	// Update Commit Index
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
	// and log[N].term == currentTerm: set commitIndex = N

	// Collect all matchIndexes (including self)
	matchIndices := make([]uint64, 0)
	matchIndices = append(matchIndices, n.LastLogIndex()) // Leader has all logs
	for _, peer := range peers {
		if peer.Id == n.Self.Id {
			continue
		}
		matchIndices = append(matchIndices, n.matchIndex[peer.Id])
	}
	sort.Slice(matchIndices, func(i, j int) bool {
		return matchIndices[i] < matchIndices[j]
	})

	// The value at the index corresponding to majority is the commit index
	// e.g. 5 nodes, majority is 3. Sorted: [1, 2, 5, 5, 5]. Index 2 (length - majority) = 5.
	majorityIdx := len(peers) - (len(peers)/2 + 1) // index in sorted list
	// Wait, standard logic:
	// If sorted asc: [1, 2, 2]. Majority needs 2. Index 1.
	// 5 nodes: [1, 1, 2, 3, 4]. Majority 3. Index 2 (val 2).
	
	// Better logic: iterate from LastLogIndex down to CommitIndex+1
	for N := n.LastLogIndex(); N > n.CommitIndex.Load(); N-- {
		count := 0
		for _, matchIdx := range matchIndices {
			if matchIdx >= N {
				count++
			}
		}

		if count > len(peers)/2 {
			// Check term requirement
			if log := n.GetLog(N); log != nil && log.TermId == currentTerm {
				n.CommitIndex.Store(N)
				break
			}
		}
	}
	
	// Apply committed entries
	for n.LastApplied.Load() < n.CommitIndex.Load() {
		n.processLogEntry(n.LastApplied.Inc())
	}

	return false
}

// processLogEntry applies a single log entry to the finite state machine. It is
// called once a log entry has been replicated to a majority and committed by
// the leader. Once the entry has been applied, the leader responds to the client
// with the result, and also caches the response.
func (n *Node) processLogEntry(logIndex uint64) (fallback bool) {
	fallback = false
	entry := n.GetLog(logIndex)
	n.Out("Processing log index: %v, entry: %v", logIndex, entry)

	status := ClientStatus_OK
	var response []byte
	var err error
	var clientId uint64

	switch entry.Type {
	case CommandType_NOOP:
		return
	case CommandType_CLIENT_REGISTRATION:
		clientId = logIndex
	case CommandType_STATE_MACHINE_COMMAND:
		if clientId, err = strconv.ParseUint(strings.Split(entry.GetCacheId(), "-")[0], 10, 64); err != nil {
			panic(err)
		}
		if resp, ok := n.GetCachedReply(entry.GetCacheId()); ok {
			status = resp.GetStatus()
			response = resp.GetResponse()
		} else {
			response, err = n.StateMachine.ApplyCommand(entry.Command, entry.Data)
			if err != nil {
				status = ClientStatus_REQ_FAILED
				response = []byte(err.Error())
			}
		}
	}

	// Construct reply
	reply := ClientReply{
		Status:     status,
		ClientId:   clientId,
		Response:   response,
		LeaderHint: &RemoteNode{Addr: n.Self.Addr, Id: n.Self.Id},
	}

	// Send reply to client
	n.requestsMutex.Lock()
	defer n.requestsMutex.Unlock()
	// Add reply to cache
	if entry.CacheId != "" {
		if err = n.CacheClientReply(entry.CacheId, reply); err != nil {
			panic(err)
		}
	}
	if replies, exists := n.requestsByCacheID[entry.CacheId]; exists {
		for _, ch := range replies {
			ch <- reply
		}
		delete(n.requestsByCacheID, entry.CacheId)
	}

	return
}