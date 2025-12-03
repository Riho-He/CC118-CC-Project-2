package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"time"
)

// consts for testing
const (
	WaitPeriod = 6
)

// ShowLogs toggles showing logs
var ShowLogs bool

func init() {
	flag.BoolVar(&ShowLogs, "raft.showlogs", false, "show student output")
}

func suppressLoggers() {
	raft.Out.SetOutput(ioutil.Discard)
	raft.Error.SetOutput(ioutil.Discard)
	raft.Debug.SetOutput(ioutil.Discard)
	grpclog.SetLogger(raft.Out)
}

// Creates a cluster of nodes at specific ports, with a
// more lenient election timeout for testing.
func createTestCluster(ports []int) ([]*raft.Node, error) {
	raft.SetDebug(false)
	config := raft.DefaultConfig()
	config.ClusterSize = len(ports)
	config.ElectionTimeout = time.Millisecond * 400

	return raft.CreateDefinedLocalCluster(config, ports)
}

// Returns the leader in a raft cluster, and an error otherwise.
func findLeader(nodes []*raft.Node) (*raft.Node, error) {
	leaders := make([]*raft.Node, 0)
	for _, node := range nodes {
		if node.GetState() == raft.LeaderState {
			leaders = append(leaders, node)
		}
	}

	if len(leaders) == 0 {
		return nil, fmt.Errorf("No leader found in slice of nodes")
	} else if len(leaders) == 1 {
		return leaders[0], nil
	} else {
		return nil, fmt.Errorf("Found too many leaders in slice of nodes: %v", len(leaders))
	}
}

// Returns whether all logs in a cluster match the leader's.
func logsMatch(leader *raft.Node, nodes []*raft.Node) bool {
	for _, node := range nodes {
		if node.GetState() != raft.LeaderState {
			if bytes.Compare(node.StateMachine.GetState().([]byte), leader.StateMachine.GetState().([]byte)) != 0 {
				return false
			}
		}
	}
	return true
}

// Given a slice of RaftNodes representing a cluster,
// exits each node and removes its logs.
func cleanupCluster(nodes []*raft.Node) {
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		node.Server.Stop()
		go func(node *raft.Node) {
			node.GracefulExit()
			node.RemoveLogs()
		}(node)
	}
	time.Sleep(5 * time.Second)
}
