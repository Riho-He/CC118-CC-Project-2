package raft

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// createCacheID - make unique id from client id and seq
func createCacheID(clientId uint64, seq uint64) string {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], clientId)
	binary.BigEndian.PutUint64(data[8:16], seq)
	
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash[:16])
}

func (n *Node) setCurrentTerm(term uint64) error {
	return n.StableStore.SetCurrentTerm(term)
}

func (n *Node) setVotedFor(candidateId string) error {
	return n.StableStore.SetVotedFor(candidateId)
}

type RemoteNode struct {
	Id   string
	Addr string
}

// AppendEntriesRPC - send append entries
func (r *RemoteNode) AppendEntriesRPC(caller *Node, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	target := caller.getNodeById(r.Id)
	if target == nil {
		return nil, fmt.Errorf("node %s not found", r.Id)
	}
	
	replyChan := make(chan AppendEntriesReply, 1)
	
	// check network policy
	if caller.NetworkPolicy != nil {
		if !caller.NetworkPolicy.IsPolicyPermitted(caller.Self.Id, r.Id) {
			return nil, fmt.Errorf("network policy blocked")
		}
	}
	
	select {
	case target.appendEntries <- AppendEntriesMsg{
		request: req,
		reply:   replyChan,
	}:
	default:
		return nil, fmt.Errorf("channel full")
	}
	
	reply := <-replyChan
	return &reply, nil
}

// RequestVoteRPC - send vote request
func (r *RemoteNode) RequestVoteRPC(caller *Node, req *RequestVoteRequest) (*RequestVoteReply, error) {
	target := caller.getNodeById(r.Id)
	if target == nil {
		return nil, fmt.Errorf("node %s not found", r.Id)
	}
	
	replyChan := make(chan RequestVoteReply, 1)
	
	if caller.NetworkPolicy != nil {
		if !caller.NetworkPolicy.IsPolicyPermitted(caller.Self.Id, r.Id) {
			return nil, fmt.Errorf("network policy blocked")
		}
	}
	
	select {
	case target.requestVote <- RequestVoteMsg{
		request: req,
		reply:   replyChan,
	}:
	default:
		return nil, fmt.Errorf("channel full")
	}
	
	reply := <-replyChan
	return &reply, nil
}

func (n *Node) getNodeById(id string) *Node {
	if n.clusterNodes == nil {
		return nil
	}
	
	for _, node := range n.clusterNodes {
		if node.Self.Id == id {
			return node
		}
	}
	return nil
}

// message types
type AppendEntriesMsg struct {
	request *AppendEntriesRequest
	reply   chan AppendEntriesReply
}

type RequestVoteMsg struct {
	request *RequestVoteRequest
	reply   chan RequestVoteReply
}

type ClientRequestMsg struct {
	request *ClientRequest
	reply   chan ClientReply
}

type RegisterClientMsg struct {
	request *RegisterClientRequest
	reply   chan RegisterClientReply
}

// rpc types
type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateId  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type ClientRequest struct {
	ClientId    uint64
	SequenceNum uint64
	Data        []byte
}

type ClientReply struct {
	Status     ClientStatus
	Response   string
	LeaderHint *RemoteNode
}

type RegisterClientRequest struct {
}

type RegisterClientReply struct {
	Status     ClientStatus
	Response   string
	LeaderHint *RemoteNode
	ClientId   uint64
}

type ClientStatus int

const (
	ClientStatus_OK ClientStatus = iota
	ClientStatus_NOT_LEADER
	ClientStatus_ELECTION_IN_PROGRESS
	ClientStatus_ERROR
)

type CommandType int

const (
	CommandType_NOOP CommandType = iota
	CommandType_CLIENT_REGISTRATION
	CommandType_STATE_MACHINE_COMMAND
)

type LogEntry struct {
	Index   uint64
	Term    uint64
	Type    CommandType
	Data    []byte
	CacheId string
}

const (
	FOLLOWER_STATE  = "FOLLOWER"
	CANDIDATE_STATE = "CANDIDATE"
	LEADER_STATE    = "LEADER"
)

// network policy for testing
type NetworkPolicy struct {
	policies map[string]NetworkPolicyType
}

type NetworkPolicyType int

const (
	NetworkPolicy_NONE NetworkPolicyType = iota
	NetworkPolicy_BLACKHOLE
	NetworkPolicy_DELAY
)

func NewNetworkPolicy() *NetworkPolicy {
	return &NetworkPolicy{
		policies: make(map[string]NetworkPolicyType),
	}
}

func (np *NetworkPolicy) RegisterPolicy(nodeId string, policy NetworkPolicyType) {
	np.policies[nodeId] = policy
}

func (np *NetworkPolicy) IsPolicyPermitted(from, to string) bool {
	if np.policies[from] == NetworkPolicy_BLACKHOLE {
		return false
	}
	if np.policies[to] == NetworkPolicy_BLACKHOLE {
		return false
	}
	return true
}
