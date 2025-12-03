package pkg

import (
	"sync"
)

// doCandidate implements the logic for a Raft node in the candidate state.
func (n *Node) doCandidate() stateFunction {
	n.Out("Transitioning to CANDIDATE_STATE")
	n.setState(CandidateState)

	// Increment current term
	n.SetCurrentTerm(n.GetCurrentTerm() + 1)
	// Vote for self
	n.setVotedFor(n.Self.Id)

	// Reset election timer
	timeout := randomTimeout(n.Config.ElectionTimeout)

	// Channels for election results (using buffers to prevent blocking)
	electionWin := make(chan bool, 1)
	fallbackCh := make(chan bool, 1)

	// Trigger asynchronous vote requests
	go func() {
		fb, win := n.requestVotes(n.GetCurrentTerm())
		if fb {
			fallbackCh <- true
		} else if win {
			electionWin <- true
		}
	}()

	for {
		select {
		case <-n.gracefulExit:
			return nil

		case <-fallbackCh:
			// Discovered higher term during election
			return n.doFollower

		case <-electionWin:
			// Won election
			return n.doLeader

		case <-timeout:
			// Election timeout elapses: start new election
			return n.doCandidate

		case msg := <-n.clientRequest:
			msg.reply <- ClientReply{Status: ClientStatus_ELECTION_IN_PROGRESS}

		case msg := <-n.appendEntries:
			// Check if the leader is valid (term >= currentTerm)
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

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {
	n.Out("Starting election for term %v", currTerm)
	votes := 1 // Vote for self
	var votesMutex sync.Mutex
	var wg sync.WaitGroup

	peers := n.getPeers()
	for _, peer := range peers {
		if peer.Id == n.Self.Id {
			continue
		}

		wg.Add(1)
		go func(peer *RemoteNode) {
			defer wg.Done()

			// Construct RequestVote RPC
			request := &RequestVoteRequest{
				Term:         currTerm,
				Candidate:    n.Self,
				LastLogIndex: n.LastLogIndex(),
			}

			// Get LastLogTerm
			lastLog := n.GetLog(n.LastLogIndex())
			if lastLog != nil {
				request.LastLogTerm = lastLog.TermId
			} else {
				request.LastLogTerm = 0
			}

			// Send RPC
			reply, err := peer.RequestVoteRPC(n, request)

			if err != nil {
				n.Debug("Failed to request vote from %v: %v", peer.Id, err)
				return
			}

			if reply.Term > currTerm {
				n.Out("Discovered higher term %v from %v", reply.Term, peer.Id)
				n.SetCurrentTerm(reply.Term)
				n.setVotedFor("")
				fallback = true
				return
			}

			if reply.VoteGranted {
				votesMutex.Lock()
				votes++
				votesMutex.Unlock()
			}
		}(peer)
	}

	// Wait for all RPCs to return (or timeout via client timeout)
	wg.Wait()

	if fallback {
		return true, false
	}

	// Check if we won majority
	if votes > len(peers)/2 {
		n.Out("Won election with %v votes", votes)
		return false, true
	}

	n.Out("Lost election with %v votes", votes)
	return false, false
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	request := msg.request
	reply := msg.reply

	currentTerm := n.GetCurrentTerm()

	// 1. Reply false if term < currentTerm
	if request.Term < currentTerm {
		n.Out("Rejecting vote for %v: lower term", request.Candidate.Id)
		reply <- RequestVoteReply{
			Term:        currentTerm,
			VoteGranted: false,
		}
		return false
	}

	// If term is higher, update term and become follower (fallback)
	if request.Term > currentTerm {
		n.Out("Updating term %v -> %v due to RequestVote", currentTerm, request.Term)
		n.SetCurrentTerm(request.Term)
		n.setVotedFor("")
		fallback = true
		// Update local currentTerm variable for subsequent checks
		currentTerm = request.Term
	}

	// 2. If votedFor is null or candidateId...
	votedFor := n.GetVotedFor()
	canVote := (votedFor == "" || votedFor == request.Candidate.Id)

	// ...and candidate's log is at least as up-to-date as receiver's log, grant vote
	isUpToDate := false
	myLastLogIndex := n.LastLogIndex()
	myLastLogTerm := uint64(0)
	if log := n.GetLog(myLastLogIndex); log != nil {
		myLastLogTerm = log.TermId
	}

	if request.LastLogTerm > myLastLogTerm {
		isUpToDate = true
	} else if request.LastLogTerm == myLastLogTerm && request.LastLogIndex >= myLastLogIndex {
		isUpToDate = true
	}

	if canVote && isUpToDate {
		n.Out("Granting vote to %v", request.Candidate.Id)
		n.setVotedFor(request.Candidate.Id)
		reply <- RequestVoteReply{
			Term:        currentTerm,
			VoteGranted: true,
		}
		// If we are candidate/leader and we grant a vote (due to higher term usually, handled by fallback), 
		// or if we are follower, this is valid.
		return fallback
	}

	n.Out("Rejecting vote for %v: votedFor=%v, upToDate=%v", request.Candidate.Id, votedFor, isUpToDate)
	reply <- RequestVoteReply{
		Term:        currentTerm,
		VoteGranted: false,
	}

	return fallback
}