package pkg

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"

	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

// NodeState represents one of five possible states a Raft node can be in.
type NodeState int32

// Enum for potential node states
const (
	FollowerState NodeState = iota
	CandidateState
	LeaderState
	JoinState
	ExitState
)

// Node defines an individual Raft node.
type Node struct {
	UnimplementedRaftRPCServer
	// Immutable state on all servers
	Self     *RemoteNode
	Port     int
	nodeCond *sync.Cond

	// gRPC related fields
	ctx           context.Context
	cancel        context.CancelFunc
	Server        *grpc.Server
	NetworkPolicy *NetworkPolicy

	// Stable store (written to disk, use helper methods)
	StableStore StableStore

	// Replicated state machine (e.g. hash machine, kv-store etc.)
	StateMachine StateMachine

	// volatile state on all servers
	Leader              *RemoteNode
	NodeMutex           sync.Mutex
	lastHeardFromLeader *atomic.Int64

	Peers      []*RemoteNode
	peersMutex sync.RWMutex

	Config Config

	CommitIndex *atomic.Uint64
	LastApplied *atomic.Uint64
	state       *atomic.Int32

	// Leader specific volatile state
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	LeaderMutex sync.Mutex

	// Channels to send / receive various RPC messages (used in state functions)
	appendEntries chan AppendEntriesMsg
	requestVote   chan RequestVoteMsg
	clientRequest chan ClientRequestMsg
	gracefulExit  chan bool

	// Client request map (used to store channels to respond through once a
	// request has been processed)
	requestsByCacheID map[string][]chan ClientReply
	requestsMutex     sync.Mutex
}

// CreateNode is used to construct a new Raft node. It takes a configuration,
// as well as implementations of various interfaces that are required.
// If we have any old state, such as snapshots, logs, Peers, etc, all those will be restored when creating the Raft node.
// Use port=0 for auto selection
func CreateNode(listener net.Listener, config Config, stateMachine StateMachine, stableStore StableStore) (*Node, error) {
	n := new(Node)

	// Set remote self based on listener address
	n.Self = &RemoteNode{
		Id:   AddrToID(listener.Addr().String(), config.NodeIDSize),
		Addr: listener.Addr().String(),
	}
	n.Config = config
	n.Peers = []*RemoteNode{n.Self}
	n.nodeCond = sync.NewCond(&n.peersMutex)

	// Obtain port from listener
	_, realPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return nil, err
	}
	n.Port, err = strconv.Atoi(realPort)
	if err != nil {
		return nil, err
	}

	n.state = atomic.NewInt32(int32(JoinState))
	n.lastHeardFromLeader = atomic.NewInt64(0)

	// Initialize network policy
	n.NetworkPolicy = NewNetworkPolicy()

	// Initialize leader specific state
	n.CommitIndex = atomic.NewUint64(0)
	n.LastApplied = atomic.NewUint64(0)
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)

	// Initialize RPC channels
	n.appendEntries = make(chan AppendEntriesMsg)
	n.requestVote = make(chan RequestVoteMsg)
	n.clientRequest = make(chan ClientRequestMsg)
	n.gracefulExit = make(chan bool)

	// Initialize state machine (in Puddlestore, you'll switch this with your
	// own state machine)
	n.StateMachine = stateMachine

	// Initialize stable store with Bolt store
	n.StableStore = stableStore
	n.InitStableStore()

	// Initialize client request cache
	n.requestsByCacheID = make(map[string][]chan ClientReply)

	// Start RPC server
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	n.ctx = ctx
	n.cancel = cancel
	n.Server = grpc.NewServer()
	RegisterRaftRPCServer(n.Server, n)
	go n.Server.Serve(listener)
	n.Out("Created node")

	return n, nil
}

func (n *Node) Connect(remote *RemoteNode) error {
	if remote != nil {
		reply, err := remote.JoinRPC(n)
		if err != nil {
			return err
		}
		n.setPeers(reply.GetPeers())
		go n.run()
	} else {
		go n.startCluster()
	}
	return nil
}

// stateFunction is a function defined on a Raft node, that while executing,
// handles the logic of the current state. When the time comes to transition to
// another state, the function returns the next function to execute.
type stateFunction func() stateFunction

func (n *Node) run() {
	n.Out("Node running...")
	var curr stateFunction = n.doFollower
	for curr != nil {
		curr = curr()
	}
}

// startCluster puts the current Raft node on hold until the required number of
// Peers join the cluster. Once they do, it starts the Peers via a StartNodeRPC
// call, and then starts the current node in the follower state.
func (n *Node) startCluster() {
	n.Out("Waiting to start cluster until all have joined")
	// Wait for all nodes to join cluster...
	n.nodeCond.L.Lock()
	defer n.nodeCond.L.Unlock()

	for len(n.Peers) < n.Config.ClusterSize {
		n.nodeCond.Wait()
	}

	// Start the current Raft node, initially in follower state
	go n.run()
}

func (n *Node) GetState() NodeState {
	return NodeState(n.state.Load())
}

func (n *Node) setState(state NodeState) {
	n.state.Store(int32(state))
}

func (n *Node) getLeader() *RemoteNode {
	n.NodeMutex.Lock()
	defer n.NodeMutex.Unlock()
	return n.Leader
}

func (n *Node) setLeader(leader *RemoteNode) {
	n.NodeMutex.Lock()
	defer n.NodeMutex.Unlock()
	n.Leader = leader
}

func (n *Node) getPeers() []*RemoteNode {
	n.peersMutex.RLock()
	defer n.peersMutex.RUnlock()
	var peers []*RemoteNode
	for _, peer := range n.Peers {
		peers = append(peers, &RemoteNode{
			Addr: peer.Addr,
			Id:   peer.Id,
		})
	}
	return peers
}

func (n *Node) setPeers(peers []*RemoteNode) {
	n.peersMutex.Lock()
	defer n.peersMutex.Unlock()
	lenOldPeers := len(n.Peers)
	n.Peers = peers
	n.Config.ClusterSize = len(peers)
	n.Out("num peers changing from: %v --> %v", lenOldPeers, len(peers))
}

// Join adds the fromNode to the current Raft cluster.
func (n *Node) Join(fromNode *RemoteNode) ([]*RemoteNode, error) {
	n.nodeCond.L.Lock()
	defer n.nodeCond.L.Unlock()

	exists := func() bool {
		for _, peer := range n.Peers {
			if peer.Id == fromNode.Id {
				return true
			}
		}
		return false
	}()

	// Cluster is already at capacity
	if len(n.Peers) == n.Config.ClusterSize {
		// Node rejoining
		if exists {
			return n.Peers, nil
		}
		// Extra node joining
		return nil, fmt.Errorf("cluster is already full")
	}

	// Cluster is not at capacity
	if !exists {
		n.Peers = append(n.Peers, fromNode)
	}

	// Cluster is now at capacity. Wake up all threads waiting on condvar
	if len(n.Peers) == n.Config.ClusterSize {
		n.nodeCond.Broadcast()
		return n.Peers, nil
	}

	// Wait until cluster is at capacity
	for len(n.Peers) < n.Config.ClusterSize {
		n.nodeCond.Wait()
	}

	return n.Peers, nil
}

var ErrShutdown = errors.New("node has shutdown")

// AppendEntriesMsg is used for notifying candidates of a new leader and transferring logs
type AppendEntriesMsg struct {
	request *AppendEntriesRequest
	reply   chan AppendEntriesReply
}

// AppendEntries is invoked on us by a remote node, and sends the request and a
// reply channel to the stateFunction.
func (n *Node) AppendEntries(req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	n.Debug("AppendEntries request received")
	// Ensures that goroutine is not blocking on sending msg to reply channel
	reply := make(chan AppendEntriesReply, 1)
	n.appendEntries <- AppendEntriesMsg{req, reply}
	select {
	case <-n.ctx.Done():
		return nil, ErrShutdown
	case msg := <-reply:
		return &msg, nil
	}
}

// RequestVoteMsg is used for raft elections
type RequestVoteMsg struct {
	request *RequestVoteRequest
	reply   chan RequestVoteReply
}

// RequestVote is invoked on us by a remote node, and sends the request and a
// reply channel to the stateFunction.
func (n *Node) RequestVote(req *RequestVoteRequest) (*RequestVoteReply, error) {
	n.Debug("RequestVote request received")
	reply := make(chan RequestVoteReply, 1)
	n.requestVote <- RequestVoteMsg{req, reply}
	select {
	case <-n.ctx.Done():
		return nil, ErrShutdown
	case msg := <-reply:
		return &msg, nil
	}
}

// ClientRequestMsg is sent from a client to raft leader to make changes to the state machine
type ClientRequestMsg struct {
	request *ClientRequest
	reply   chan ClientReply
}

// RegisterClient is invoked on us by a client, and sends the request and a
// reply channel to the stateFunction. If the cluster hasn't started yet, it
// returns the corresponding RegisterClientReply.
func (n *Node) RegisterClient(req *ClientRequest) (*ClientReply, error) {
	n.Debug("RegisterClientRequest received")
	reply := make(chan ClientReply, 1)

	// If cluster hasn't started yet, return
	if n.GetState() == JoinState {
		return &ClientReply{
			Status:     ClientStatus_CLUSTER_NOT_STARTED,
			ClientId:   0,
			Response:   nil,
			LeaderHint: nil,
		}, nil
	}

	// Send request down channel to be processed by current stateFunction
	n.clientRequest <- ClientRequestMsg{req, reply}
	select {
	case <-n.ctx.Done():
		return nil, ErrShutdown
	case msg := <-reply:
		return &msg, nil
	}
}

// ClientRequest is invoked on us by a client, and sends the request and a
// reply channel to the stateFunction. If the cluster hasn't started yet, it
// returns the corresponding ClientReply.
func (n *Node) ClientRequest(req *ClientRequest) (*ClientReply, error) {
	n.Debug("ClientRequest request received")

	// If cluster hasn't started yet, return
	if n.GetState() == JoinState {
		return &ClientReply{
			Status:     ClientStatus_CLUSTER_NOT_STARTED,
			ClientId:   req.ClientId,
			Response:   nil,
			LeaderHint: nil,
		}, nil
	}

	reply := make(chan ClientReply, 1)
	cacheID := CreateCacheID(req.ClientId, req.SequenceNum)
	cr, exists := n.GetCachedReply(cacheID)

	if exists {
		// If the request has been cached, reply with existing response
		return cr, nil
	}

	// Else, send request down channel to be processed by current stateFunction
	n.clientRequest <- ClientRequestMsg{req, reply}
	select {
	case <-n.ctx.Done():
		return nil, ErrShutdown
	case msg := <-reply:
		return &msg, nil
	}
}

// Exit abruptly shuts down the current node's process, including the GRPC serven.
func (n *Node) Exit() {
	n.Out("Abruptly shutting down node!")
	os.Exit(0)
}

// GracefulExit sends a signal down the gracefulExit channel, in order to enable
// a safe exit from the cluster, handled by the current stateFunction.
func (n *Node) GracefulExit() {
	n.NetworkPolicy.PauseWorld(true)
	n.Out("Gracefully shutting down node!")
	if state := n.GetState(); !(state == ExitState || state == JoinState) {
		n.setState(ExitState)
		n.gracefulExit <- true
	}
	n.cancel()
	n.Server.GracefulStop()
	n.StableStore.Close()
}
