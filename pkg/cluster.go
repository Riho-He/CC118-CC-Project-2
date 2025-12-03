package pkg

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"raft/pkg/hashmachine"
)

// CreateLocalCluster creates a new Raft cluster with the given config in the
// current process.
func CreateLocalCluster(config Config) (nodes []*Node, err error) {
	if err = CheckConfig(config); err != nil {
		return
	}
	nodes = make([]*Node, config.ClusterSize)

	var stableStore StableStore
	if config.InMemory {
		stableStore = NewMemoryStore()
	} else {
		stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
	}
	nodes[0], err = CreateNode(OpenPort(0), config, new(hashmachine.HashMachine), stableStore)
	if err != nil {
		Error.Printf("Error creating first node: %v", err)
		return
	}
	if err = nodes[0].Connect(nil); err != nil {
		Error.Printf("Error starting first node: %v", err)
		return
	}

	errChan := make(chan error)

	for i := 1; i < config.ClusterSize; i++ {
		var stableStore StableStore
		if config.InMemory {
			stableStore = NewMemoryStore()
		} else {
			stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", rand.Int())))
		}
		nodes[i], err = CreateNode(OpenPort(0), config, new(hashmachine.HashMachine), stableStore)
		if err != nil {
			return
		}
		go func(node *Node) {
			errChan <- node.Connect(nodes[0].Self)
		}(nodes[i])
	}

	for i := 1; i < config.ClusterSize; i++ {
		if err = <-errChan; err != nil {
			return
		}
	}
	close(errChan)

	return nodes, nil
}

// CreateDefinedLocalCluster creates a new Raft cluster with nodes listening at
// the given ports in the current process.
func CreateDefinedLocalCluster(config Config, ports []int) (nodes []*Node, err error) {
	if err = CheckConfig(config); err != nil {
		return
	}
	nodes = make([]*Node, config.ClusterSize)

	var stableStore StableStore
	if config.InMemory {
		stableStore = NewMemoryStore()
	} else {
		stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", ports[0])))
	}
	nodes[0], err = CreateNode(OpenPort(ports[0]), config, new(hashmachine.HashMachine), stableStore)
	if err != nil {
		Error.Printf("Error creating first node: %v", err)
		return
	}
	if err = nodes[0].Connect(nil); err != nil {
		Error.Printf("Error starting first node: %v", err)
		return
	}

	errChan := make(chan error)

	for i := 1; i < config.ClusterSize; i++ {
		var stableStore StableStore
		if config.InMemory {
			stableStore = NewMemoryStore()
		} else {
			stableStore = NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", ports[i])))
		}
		nodes[i], err = CreateNode(OpenPort(ports[i]), config, new(hashmachine.HashMachine), stableStore)
		if err != nil {
			Error.Printf("Error creating %v-th node: %v", i, err)
			return
		}
		go func(node *Node) {
			errChan <- node.Connect(nodes[0].Self)
		}(nodes[i])
	}

	for i := 1; i < config.ClusterSize; i++ {
		if err = <-errChan; err != nil {
			return
		}
	}
	close(errChan)

	return
}
