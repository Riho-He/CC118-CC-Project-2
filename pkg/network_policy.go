package pkg

import (
	"errors"
	"fmt"
	"sync"
)

// NetworkPolicy provides a way to explicitly disallow communication from a node
// to other nodes in the cluster. Each Node has its own network policy that
// can block rpc calls to specific nodes, and/or block communication to all
// other nodes. We encourage you to use this particularly in your test cases.
type NetworkPolicy struct {
	pauseWorld bool
	rpcPolicy  map[string]bool
	mutex 	   sync.Mutex
}

// NewNetworkPolicy creates a new network policy and initializes the rpcPolicy map
func NewNetworkPolicy() *NetworkPolicy {
	return &NetworkPolicy{
		pauseWorld: false,
		rpcPolicy:  make(map[string]bool, 0),
	}
}

// ErrorNetworkPolicyDenied is returned when a request is barred due to the node
var ErrorNetworkPolicyDenied = errors.New("the network policy has forbid this communication")

func getCommID(a, b *RemoteNode) string {
	return fmt.Sprintf("%v_%v", a.Id, b.Id)
}

// IsDenied checks our network policy to see if we are allowed to send or
// receive messages with the given remote node.
func (tp *NetworkPolicy) IsDenied(a, b *RemoteNode) bool {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	if tp.pauseWorld {
		return true
	}
	commStr := getCommID(a, b)
	allowed, exists := tp.rpcPolicy[commStr]
	return exists && !allowed
}

// RegisterPolicy registers whether or not communication is allowed from a to b.
func (tp *NetworkPolicy) RegisterPolicy(a, b *RemoteNode, allowed bool) {
	commStr := getCommID(a, b)
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	tp.rpcPolicy[commStr] = allowed
}

// PauseWorld temporarily enables / disables all network communication from the current node.
func (tp *NetworkPolicy) PauseWorld(on bool) {
	tp.mutex.Lock()
	defer tp.mutex.Unlock()
	tp.pauseWorld = on
}
