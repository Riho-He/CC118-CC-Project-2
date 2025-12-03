package main

import (
	"flag"
	"fmt"
	"net"
	"path/filepath"
	"strconv"

	"github.com/abiosoft/ishell"
	"raft/pkg/hashmachine"
	raft "raft/pkg"
)

// RaftNode is wrapper for raft.Node
type RaftNode struct {
	*raft.Node
}

func (node RaftNode) findNode(id string) *raft.RemoteNode {
	nodeList := node.Peers

	for _, remoteNode := range nodeList {
		if (remoteNode.Id == id) ||
			(remoteNode.Addr == id) {
			return remoteNode
		}
	}

	return nil
}

var (
	port   int
	addr   string
	memory bool
	debug  bool
)

func init() {
	portHelpString := "The server port to bind to. Defaults to a random port."
	flag.IntVar(&port, "port", 0, portHelpString)
	flag.IntVar(&port, "p", 0, portHelpString)

	connectHelpString := "An existing node to connect to. If left blank, does not attempt to connect to another node."
	flag.StringVar(&addr, "connect", "", connectHelpString)
	flag.StringVar(&addr, "c", "", connectHelpString)

	memoryHelpString := "Turn on in-memory storage. Default is true"
	flag.BoolVar(&memory, "memory", true, memoryHelpString)
	flag.BoolVar(&memory, "m", true, memoryHelpString)

	debugHelpString := "Turn on debug message printing."
	flag.BoolVar(&debug, "debug", false, debugHelpString)
	flag.BoolVar(&debug, "d", false, debugHelpString)
}

func main() {
	flag.Parse()

	raft.SetDebug(debug)

	// Initialize Raft with default config
	config := raft.DefaultConfig()
	config.InMemory = memory

	// Create listener
	listener := raft.OpenPort(port)
	_, realPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		fmt.Printf("Error starting Raft node: %v\n", err)
		return
	}
	port, err = strconv.Atoi(realPort)
	if err != nil {
		fmt.Printf("Error starting Raft node: %v\n", err)
		return
	}

	// Create stable store
	var stableStore raft.StableStore
	if config.InMemory {
		stableStore = raft.NewMemoryStore()
	} else {
		stableStore = raft.NewBoltStore(filepath.Join(config.LogPath, fmt.Sprintf("raft%d", port)))
	}

	// Create Raft node
	fmt.Println("Starting a Raft node...")
	raftNode, err := raft.CreateNode(listener, config, new(hashmachine.HashMachine), stableStore)
	node := RaftNode{raftNode}

	if err != nil {
		fmt.Printf("Error starting Raft node: %v\n", err)
		return
	}

	fmt.Printf("Successfully created Raft node: %v\n", node)

	// Kick off shell
	shell := ishell.New()

	debugCommand := ishell.Cmd{
		Name: "debug",
		Help: "turn debug messages on or off, on by default",
		Func: func(c *ishell.Context) {
			shell.Println("Usage: debug <on|off>")
		},
	}

	debugCommand.AddCmd(&ishell.Cmd{
		Name: "on",
		Help: "turn debug messages on",
		Func: func(c *ishell.Context) {
			raft.SetDebug(true)
		},
	})

	debugCommand.AddCmd(&ishell.Cmd{
		Name: "off",
		Help: "turn debug messages off",
		Func: func(c *ishell.Context) {
			raft.SetDebug(false)
		},
	})

	shell.AddCmd(&debugCommand)

	shell.AddCmd(&ishell.Cmd{
		Name: "state",
		Help: "print out the current local and cluster state",
		Func: func(c *ishell.Context) {
			shell.Println(node.FormatState())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "log",
		Help: "print out the local log cache",
		Func: func(c *ishell.Context) {
			shell.Println(node.FormatLogCache())
		},
	})

	enableCommand := ishell.Cmd{
		Name: "enable",
		Help: "enable communications with one or all nodes in the cluster",
		Func: func(c *ishell.Context) {
			shell.Println("Usage: enable all | enable <send|recv> <addr>")
		},
	}

	enableCommand.AddCmd(&ishell.Cmd{
		Name: "all",
		Help: "enable all communications with the cluster",
		Func: func(c *ishell.Context) {
			node.NetworkPolicy.PauseWorld(false)
		},
	})

	enableCommand.AddCmd(&ishell.Cmd{
		Name: "send",
		Help: "enable sending requests to a specific remote node",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Printf("Usage: enable send <addr>")
				return
			}

			remoteNode := node.findNode(c.Args[0])
			if remoteNode == nil {
				shell.Printf("Error: could not find node with given ID or address: %v\n", c.Args[0])
			}

			node.NetworkPolicy.RegisterPolicy(node.Self, remoteNode, true)
		},
	})

	enableCommand.AddCmd(&ishell.Cmd{
		Name: "recv",
		Help: "enable receiving requests from a specific remote node",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Printf("Usage: enable recv <addr>")
				return
			}

			remoteNode := node.findNode(c.Args[0])
			if remoteNode == nil {
				shell.Printf("Error: could not find node with given ID or address: %v\n", c.Args[0])
			}

			node.NetworkPolicy.RegisterPolicy(remoteNode, node.Self, true)
		},
	})

	shell.AddCmd(&enableCommand)

	disableCommand := ishell.Cmd{
		Name: "disable",
		Help: "disable communications with one or all nodes in the cluster",
		Func: func(c *ishell.Context) {
			shell.Println("Usage: disable all | disable <send|recv> <addr>")
		},
	}

	disableCommand.AddCmd(&ishell.Cmd{
		Name: "all",
		Help: "disable all communications with the cluster",
		Func: func(c *ishell.Context) {
			node.NetworkPolicy.PauseWorld(true)
		},
	})

	disableCommand.AddCmd(&ishell.Cmd{
		Name: "send",
		Help: "disable sending requests to a specific remote node",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Printf("Usage: disable send <addr>")
				return
			}

			remoteNode := node.findNode(c.Args[0])
			if remoteNode == nil {
				shell.Printf("Error: could not find node with given ID or address: %v\n", c.Args[0])
			}

			node.NetworkPolicy.RegisterPolicy(node.Self, remoteNode, false)
		},
	})

	disableCommand.AddCmd(&ishell.Cmd{
		Name: "recv",
		Help: "disable receiving requests from a specific remote node",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Printf("Usage: disable recv <addr>")
				return
			}

			remoteNode := node.findNode(c.Args[0])
			if remoteNode == nil {
				shell.Printf("Error: could not find node with given ID or address: %v\n", c.Args[0])
			}

			node.NetworkPolicy.RegisterPolicy(remoteNode, node.Self, false)
		},
	})

	shell.AddCmd(&disableCommand)

	shell.AddCmd(&ishell.Cmd{
		Name: "leave",
		Help: "gracefully leave the cluster",
		Func: func(c *ishell.Context) {
			raft.Out.Println("Gracefully exiting local raft node...")
			node.GracefulExit()
			raft.Out.Println("Bye!")
		},
	})

	shell.Println(shell.HelpText())
	shell.Run()
}
