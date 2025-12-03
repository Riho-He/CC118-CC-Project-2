// Tufts University, CS 118, Fall 2025
//
// Purpose: Implements functions that are invoked by clients and other nodes
// over RPC.

package pkg

import (
	"golang.org/x/net/context"
)

// JoinCaller is called through GRPC to execute a join request.
func (n *Node) JoinCaller(ctx context.Context, r *RemoteNode) (*JoinReply, error) {
	// Check if the network policy prevents incoming requests from the requesting node
	if n.NetworkPolicy.IsDenied(r, n.Self) {
		return nil, ErrorNetworkPolicyDenied
	}

	// Defer to the local Join implementation, and marshall the results to
	// respond to the GRPC request.
	peers, err := n.Join(r)

	return &JoinReply{Peers: peers}, err
}

// AppendEntriesCaller is called through GRPC to respond to an append entries request.
func (n *Node) AppendEntriesCaller(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesReply, error) {
	if n.NetworkPolicy.IsDenied(req.Leader, n.Self) {
		return nil, ErrorNetworkPolicyDenied
	}

	return n.AppendEntries(req)
}

// RequestVoteCaller is called through GRPC to respond to a vote request.
func (n *Node) RequestVoteCaller(ctx context.Context, req *RequestVoteRequest) (*RequestVoteReply, error) {
	if n.NetworkPolicy.IsDenied(req.Candidate, n.Self) {
		return nil, ErrorNetworkPolicyDenied
	}

	return n.RequestVote(req)
}

// ClientRequestCaller is called through GRPC to respond to a client request.
func (n *Node) ClientRequestCaller(ctx context.Context, req *ClientRequest) (*ClientReply, error) {
	return n.ClientRequest(req)
}
