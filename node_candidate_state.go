package raft

import (
	"time"
)

// doCandidate - candidate main loop
func (n *Node) doCandidate() stateFunction {
	n.Out("Transitioning to Candidate state")
	n.State = CANDIDATE_STATE
	
	// start election
	term, _ := n.GetCurrentTerm()
	term++
	n.setCurrentTerm(term)
	n.setVotedFor(n.Self.Id)
	
	timeout := n.getRandomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	
	voteChan := make(chan bool, len(n.Peers))
	go n.requestVotes(voteChan)
	
	votes := 1
	majority := (len(n.Peers) + 1) / 2 + 1
	
	for {
		select {
		case <-timer.C:
			n.Out("Election timeout, restarting election")
			return n.doCandidate
			
		case granted := <-voteChan:
			if granted {
				votes++
				n.Debug("Received vote, total: %d/%d", votes, majority)
				
				if votes >= majority {
					n.Out("Won election with %d votes", votes)
					return n.doLeader
				}
			}
			
		case ae := <-n.appendEntries:
			term, _ := n.GetCurrentTerm()
			
			if ae.request.Term >= term {
				n.Out("Received AppendEntries from leader, stepping down")
				
				reply := n.handleAppendEntries(ae.request)
				ae.reply <- reply
				
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
				
				reply := n.handleRequestVote(rv.request)
				rv.reply <- reply
				
				return n.doFollower
			}
			
			rv.reply <- RequestVoteReply{
				Term:        term,
				VoteGranted: false,
			}
			
		case req := <-n.clientRequest:
			req.reply <- ClientReply{
				Status:     ClientStatus_NOT_LEADER,
				Response:   "",
				LeaderHint: n.Self,
			}
			
		case reg := <-n.registerClient:
			reg.reply <- RegisterClientReply{
				Status:     ClientStatus_NOT_LEADER,
				Response:   "",
				LeaderHint: n.Self,
				ClientId:   0,
			}
			
		case <-n.gracefulExit:
			return nil
		}
	}
}

// requestVotes - send vote requests to all peers
func (n *Node) requestVotes(voteChan chan bool) {
	term, _ := n.GetCurrentTerm()
	lastIdx := n.LastLogIndex()
	
	var lastTerm uint64 = 0
	if lastIdx > 0 {
		lastLog, err := n.GetLog(lastIdx)
		if err == nil {
			lastTerm = lastLog.Term
		}
	}
	
	for _, p := range n.Peers {
		go func(peer *RemoteNode) {
			req := &RequestVoteRequest{
				Term:         term,
				CandidateId:  n.Self.Id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			
			replyChan := make(chan RequestVoteReply, 1)
			go func() {
				reply, err := peer.RequestVoteRPC(n, req)
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
					voteChan <- false
					return
				}
				
				voteChan <- reply.VoteGranted
				
			case <-time.After(n.config.ElectionTimeout):
				n.Debug("RequestVote to %s timed out", peer.Id)
				voteChan <- false
			}
		}(p)
	}
}
