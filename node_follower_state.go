package raft

import (
	"time"
)

// doFollower - follower main loop
func (n *Node) doFollower() stateFunction {
	n.Out("Transitioning to Follower state")
	n.State = FOLLOWER_STATE
	
	timeout := n.getRandomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	
	for {
		select {
		case <-timer.C:
			n.Out("Election timeout, transitioning to Candidate")
			return n.doCandidate
			
		case ae := <-n.appendEntries:
			timer.Stop()
			timer = time.NewTimer(n.getRandomElectionTimeout())
			
			reply := n.handleAppendEntries(ae.request)
			ae.reply <- reply
			
		case rv := <-n.requestVote:
			reply := n.handleRequestVote(rv.request)
			
			if reply.VoteGranted {
				timer.Stop()
				timer = time.NewTimer(n.getRandomElectionTimeout())
			}
			
			rv.reply <- reply
			
		case req := <-n.clientRequest:
			term, _ := n.GetCurrentTerm()
			req.reply <- ClientReply{
				Status:     ClientStatus_NOT_LEADER,
				Response:   "",
				LeaderHint: n.Leader,
			}
			n.Debug("Redirecting client to leader (term %d)", term)
			
		case reg := <-n.registerClient:
			reg.reply <- RegisterClientReply{
				Status:     ClientStatus_NOT_LEADER,
				Response:   "",
				LeaderHint: n.Leader,
				ClientId:   0,
			}
			
		case <-n.gracefulExit:
			return nil
		}
	}
}

// handleAppendEntries - process append entries
func (n *Node) handleAppendEntries(req *AppendEntriesRequest) AppendEntriesReply {
	term, _ := n.GetCurrentTerm()
	
	if req.Term < term {
		return AppendEntriesReply{
			Term:    term,
			Success: false,
		}
	}
	
	// update term
	if req.Term > term {
		n.setCurrentTerm(req.Term)
		n.setVotedFor("")
		term = req.Term
	}
	
	// set leader
	for _, p := range n.Peers {
		if p.Id == req.LeaderId {
			n.Leader = p
			break
		}
	}
	
	// check prev log
	if req.PrevLogIndex > 0 {
		prevLog, err := n.GetLog(req.PrevLogIndex)
		if err != nil {
			return AppendEntriesReply{
				Term:    term,
				Success: false,
			}
		}
		
		if prevLog.Term != req.PrevLogTerm {
			return AppendEntriesReply{
				Term:    term,
				Success: false,
			}
		}
	}
	
	// handle entries
	if len(req.Entries) > 0 {
		for _, entry := range req.Entries {
			existing, err := n.GetLog(entry.Index)
			
			if err != nil {
				n.StoreLog(entry)
			} else if existing.Term != entry.Term {
				n.TruncateLog(entry.Index)
				n.StoreLog(entry)
			}
		}
	}
	
	// update commit
	if req.LeaderCommit > n.commitIndex {
		lastIdx := n.LastLogIndex()
		newCommit := req.LeaderCommit
		if lastIdx < newCommit {
			newCommit = lastIdx
		}
		
		for i := n.commitIndex + 1; i <= newCommit; i++ {
			e, err := n.GetLog(i)
			if err == nil {
				n.processLogEntry(*e)
			}
		}
		n.commitIndex = newCommit
	}
	
	return AppendEntriesReply{
		Term:    term,
		Success: true,
	}
}

// handleRequestVote - handle vote request
func (n *Node) handleRequestVote(req *RequestVoteRequest) RequestVoteReply {
	term, _ := n.GetCurrentTerm()
	votedFor, _ := n.GetVotedFor()
	
	if req.Term < term {
		return RequestVoteReply{
			Term:        term,
			VoteGranted: false,
		}
	}
	
	if req.Term > term {
		n.setCurrentTerm(req.Term)
		n.setVotedFor("")
		term = req.Term
		votedFor = ""
	}
	
	voteGranted := false
	
	if (votedFor == "" || votedFor == req.CandidateId) {
		lastIdx := n.LastLogIndex()
		var lastTerm uint64 = 0
		
		if lastIdx > 0 {
			lastLog, err := n.GetLog(lastIdx)
			if err == nil {
				lastTerm = lastLog.Term
			}
		}
		
		// check if candidate log is up to date
		upToDate := false
		if req.LastLogTerm > lastTerm {
			upToDate = true
		} else if req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx {
			upToDate = true
		}
		
		if upToDate {
			voteGranted = true
			n.setVotedFor(req.CandidateId)
		}
	}
	
	return RequestVoteReply{
		Term:        term,
		VoteGranted: voteGranted,
	}
}

func (n *Node) getRandomElectionTimeout() time.Duration {
	base := n.config.ElectionTimeout.Milliseconds()
	randomMs := base + (base * int64(n.Self.Id[0]%100) / 100)
	return time.Duration(randomMs) * time.Millisecond
}
