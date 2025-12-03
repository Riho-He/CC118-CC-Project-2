package raft

import (
	"time"
)

// doLeader - main leader loop
func (n *Node) doLeader() stateFunction {
	n.Out("Transitioning to Leader state")
	n.State = LEADER_STATE

	// store no-op (raft paper 5.4.2)
	term, _ := n.GetCurrentTerm()
	entry := &LogEntry{
		Index:   n.LastLogIndex() + 1,
		Term:    term,
		Type:    CommandType_NOOP,
		Data:    []byte{},
		CacheId: "",
	}
	n.StoreLog(entry)

	// setup peer tracking
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	lastIdx := n.LastLogIndex()
	
	for _, p := range n.Peers {
		n.nextIndex[p.Id] = lastIdx + 1
		n.matchIndex[p.Id] = 0
	}

	ticker := time.NewTicker(n.config.HeartbeatTimeout)
	defer ticker.Stop()

	go n.sendHeartbeats()

	for {
		select {
		case <-ticker.C:
			go n.sendHeartbeats()

		case ae := <-n.appendEntries:
			term, _ := n.GetCurrentTerm()
			
			if ae.request.Term > term {
				n.setCurrentTerm(ae.request.Term)
				n.setVotedFor("")
				ae.reply <- AppendEntriesReply{
					Term:    ae.request.Term,
					Success: false,
				}
				return n.doFollower
			}
			
			ae.reply <- AppendEntriesReply{
				Term:    term,
				Success: false,
			}

		case rv := <-n.requestVote:
			term, _ := n.GetCurrentTerm()
			
			if rv.request.Term > term {
				n.setCurrentTerm(rv.request.Term)
				n.setVotedFor("")
				rv.reply <- RequestVoteReply{
					Term:        rv.request.Term,
					VoteGranted: false,
				}
				return n.doFollower
			}
			
			rv.reply <- RequestVoteReply{
				Term:        term,
				VoteGranted: false,
			}

		case req := <-n.clientRequest:
			n.handleClientRequest(req)

		case reg := <-n.registerClient:
			n.handleRegisterClient(reg)

		case <-n.gracefulExit:
			return nil
		}
	}
}

// sendHeartbeats - send to all peers
func (n *Node) sendHeartbeats() {
	term, _ := n.GetCurrentTerm()
	
	for _, p := range n.Peers {
		go func(peer *RemoteNode) {
			nextIdx := n.nextIndex[peer.Id]
			prevIdx := nextIdx - 1
			
			var prevTerm uint64 = 0
			if prevIdx > 0 {
				prevLog, err := n.GetLog(prevIdx)
				if err == nil {
					prevTerm = prevLog.Term
				}
			}
			
			// get entries to send
			var entries []*LogEntry
			lastIdx := n.LastLogIndex()
			
			if nextIdx <= lastIdx {
				for i := nextIdx; i <= lastIdx; i++ {
					e, err := n.GetLog(i)
					if err == nil {
						entries = append(entries, e)
					}
				}
			}
			
			req := &AppendEntriesRequest{
				Term:         term,
				LeaderId:     n.Self.Id,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: n.commitIndex,
			}
			
			replyChan := make(chan AppendEntriesReply, 1)
			go func() {
				reply, err := peer.AppendEntriesRPC(n, req)
				if err == nil {
					replyChan <- *reply
				}
			}()
			
			select {
			case reply := <-replyChan:
				currTerm, _ := n.GetCurrentTerm()
				
				if reply.Term > currTerm {
					n.setCurrentTerm(reply.Term)
					n.setVotedFor("")
					return
				}
				
				if reply.Success {
					if len(entries) > 0 {
						n.matchIndex[peer.Id] = entries[len(entries)-1].Index
						n.nextIndex[peer.Id] = n.matchIndex[peer.Id] + 1
					}
					n.updateCommitIndex()
				} else {
					// retry with earlier index
					if n.nextIndex[peer.Id] > 1 {
						n.nextIndex[peer.Id]--
					}
				}
				
			case <-time.After(n.config.HeartbeatTimeout):
				n.Debug("Heartbeat to %s timed out", peer.Id)
			}
		}(p)
	}
}

// updateCommitIndex - check if we can commit more
func (n *Node) updateCommitIndex() {
	term, _ := n.GetCurrentTerm()
	lastIdx := n.LastLogIndex()
	
	// try higher indices
	for N := lastIdx; N > n.commitIndex; N-- {
		count := 1 // self
		
		for _, p := range n.Peers {
			if n.matchIndex[p.Id] >= N {
				count++
			}
		}
		
		majority := (len(n.Peers) + 1) / 2 + 1
		if count >= majority {
			entry, err := n.GetLog(N)
			if err == nil && entry.Term == term {
				// commit everything up to N
				for i := n.commitIndex + 1; i <= N; i++ {
					e, err := n.GetLog(i)
					if err == nil {
						n.processLogEntry(*e)
					}
				}
				n.commitIndex = N
				break
			}
		}
	}
}

func (n *Node) handleRegisterClient(msg RegisterClientMsg) {
	term, _ := n.GetCurrentTerm()
	
	entry := &LogEntry{
		Index:   n.LastLogIndex() + 1,
		Term:    term,
		Type:    CommandType_CLIENT_REGISTRATION,
		Data:    []byte{},
		CacheId: createCacheID(0, 0),
	}
	
	n.StoreLog(entry)
	
	if n.requestsByCacheID == nil {
		n.requestsByCacheID = make(map[string][]chan ClientReply)
	}
	
	// convert reply type
	replyChan := make(chan ClientReply, 1)
	go func() {
		cr := <-replyChan
		msg.reply <- RegisterClientReply{
			Status:     cr.Status,
			Response:   cr.Response,
			LeaderHint: n.Self,
			ClientId:   entry.Index,
		}
	}()
	
	n.requestsByCacheID[entry.CacheId] = append(n.requestsByCacheID[entry.CacheId], replyChan)
}

func (n *Node) handleClientRequest(msg ClientRequestMsg) {
	term, _ := n.GetCurrentTerm()
	
	cacheID := createCacheID(msg.request.ClientId, msg.request.SequenceNum)
	
	// check cache
	cached, exists := n.GetCachedReply(msg.request.ClientId, msg.request.SequenceNum)
	if exists {
		msg.reply <- *cached
		return
	}
	
	if n.requestsByCacheID == nil {
		n.requestsByCacheID = make(map[string][]chan ClientReply)
	}
	
	// already pending?
	if _, pending := n.requestsByCacheID[cacheID]; pending {
		n.requestsByCacheID[cacheID] = append(n.requestsByCacheID[cacheID], msg.reply)
		return
	}
	
	entry := &LogEntry{
		Index:   n.LastLogIndex() + 1,
		Term:    term,
		Type:    CommandType_STATE_MACHINE_COMMAND,
		Data:    msg.request.Data,
		CacheId: cacheID,
	}
	
	n.StoreLog(entry)
	n.requestsByCacheID[cacheID] = []chan ClientReply{msg.reply}
}
