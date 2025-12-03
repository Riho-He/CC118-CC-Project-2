// Tufts University, CS 118, Fall 2025
//
// Purpose: Provides wrappers around the client interface of GRPC to invoke
// functions on remote Raft nodes.

package pkg

import (
	"google.golang.org/grpc/status"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// RPCTimeout is default timeout for rpc client calls
const RPCTimeout = 2 * time.Second

var DialOptions []grpc.DialOption

func init() {
	DialOptions = []grpc.DialOption{grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithTimeout(2 * time.Second), grpc.WithUnaryInterceptor(clientUnaryInterceptor)}
}

// clientUnaryInterceptor is a client unary interceptor that injects a default timeout
func clientUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	ctx, cancel := context.WithTimeout(ctx, RPCTimeout)
	defer cancel()

	return invoker(ctx, method, req, reply, cc, opts...)
}

// RPC Invocation functions

var ConnMap = make(map[string]*grpc.ClientConn)
var ConnMapLock = &sync.RWMutex{}

// makeClientConn creates a new client connection to the given remote node
func makeClientConn(remote *RemoteNode) (*grpc.ClientConn, error) {
	return grpc.Dial(remote.Addr, DialOptions...)
}

// ClientConn creates or returns a cached RPC client for the given remote node
func (remote *RemoteNode) ClientConn() (RaftRPCClient, error) {
	ConnMapLock.RLock()
	if cc, ok := ConnMap[remote.Addr]; ok {
		ConnMapLock.RUnlock()
		return NewRaftRPCClient(cc), nil
	}
	ConnMapLock.RUnlock()

	cc, err := makeClientConn(remote)
	if err != nil {
		return nil, err
	}
	ConnMapLock.Lock()
	ConnMap[remote.Addr] = cc
	ConnMapLock.Unlock()

	return NewRaftRPCClient(cc), err
}

// RemoveClientConn removes the client connection to the given node, if present
func (remote *RemoteNode) RemoveClientConn() {
	ConnMapLock.Lock()
	defer ConnMapLock.Unlock()
	if cc, ok := ConnMap[remote.Addr]; ok {
		cc.Close()
		delete(ConnMap, remote.Addr)
	}
}

// connCheck checks the given error and removes the client connection if it's not nil
func (remote *RemoteNode) connCheck(err error) error {
	if err != nil {
		st := status.Convert(err)
		if st.Message() == ErrorNetworkPolicyDenied.Error() {
			time.Sleep(RPCTimeout)
		} else {
			remote.RemoveClientConn()
		}
	}
	return err
}

// JoinRPC tells the given remote node that we (a new Raft node) want to join the cluster
func (remote *RemoteNode) JoinRPC(local *Node) (*JoinReply, error) {
	// Get a client connection for the remote node
	if local.NetworkPolicy.IsDenied(local.Self, remote) {
		return nil, ErrorNetworkPolicyDenied
	}

	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.JoinCaller(context.Background(), local.Self)
	return reply, remote.connCheck(err)
}

// AppendEntriesRPC is called by a leader in the cluster attempting to append
// entries to one of its followers. Be sure to pass in a pointer to the Node
// making the request.
func (remote *RemoteNode) AppendEntriesRPC(local *Node, request *AppendEntriesRequest) (*AppendEntriesReply, error) {
	if local.NetworkPolicy.IsDenied(local.Self, remote) {
		return nil, ErrorNetworkPolicyDenied
	}

	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.AppendEntriesCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}

// RequestVoteRPC asks the given remote node for a vote, using the provided
// RequestVoteRequest struct as the request. Note that calling nodes should
// pass in a pointer to their own Node struct.
func (remote *RemoteNode) RequestVoteRPC(local *Node, request *RequestVoteRequest) (*RequestVoteReply, error) {
	if local.NetworkPolicy.IsDenied(local.Self, remote) {
		return nil, ErrorNetworkPolicyDenied
	}

	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.RequestVoteCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}

// RegisterClientRPC is called by a new client trying to register itself with
// the given Raft node in the cluster.
func (remote *RemoteNode) RegisterClientRPC() (*ClientReply, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	request := &ClientRequest{
		ClientId:        0,
		SequenceNum:     0,
		StateMachineCmd: 0,
		Data:            nil,
	}

	reply, err := cc.ClientRequestCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}

// ClientRequestRPC is executed by a client trying to make a request to the
// given Raft node in the cluster.
func (remote *RemoteNode) ClientRequestRPC(request *ClientRequest) (*ClientReply, error) {
	cc, err := remote.ClientConn()
	if err != nil {
		return nil, err
	}

	reply, err := cc.ClientRequestCaller(context.Background(), request)
	return reply, remote.connCheck(err)
}
