package main

import (
	"flag"
	"log"

	"github.com/abiosoft/ishell"
	raft "raft/pkg"
)

var numNodes int
var inMemory bool
var debug bool

func init() {
	flag.IntVar(&numNodes, "n", 3, "number of nodes in the raft cluster")
	flag.BoolVar(&inMemory, "m", true, "toggle raft storage is persistent or in-memory")
	flag.BoolVar(&debug, "d", false, "toggle debug messages on or off")
}

// StartCluster starts all the nodes in the cluster
func StartCluster(config *raft.Config) []*raft.Node {
	nodes, err := raft.CreateLocalCluster(*config)
	if err != nil {
		panic(err)
	}
	return nodes
}

func main() {
	flag.Parse()

	config := raft.DefaultConfig()
	config.InMemory = inMemory
	config.ClusterSize = numNodes
	raft.SetDebug(debug)

	nodes := StartCluster(&config)
	log.Println("Cluster starting")

	shell := ishell.New()

	shell.AddCmd(&ishell.Cmd{
		Name: "nodes",
		Help: "nodes",
		Func: func(c *ishell.Context) {
			for _, node := range nodes {
				c.Println(node.Self)
			}
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "exit",
		Help: "exit shell",
		Func: func(c *ishell.Context) {
			c.Println("Cluster exiting")
			shell.Stop()
		},
	})

	debugCommand := ishell.Cmd{
		Name: "debug",
		Help: "toggle debug messages on/off, off by default",
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

	shell.Run()
}
