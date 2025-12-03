package main

import (
	"flag"
	"fmt"
	"raft/pkg/hashmachine"

	"github.com/abiosoft/ishell"
	raft "raft/pkg"
)

func main() {
	var addr string

	addrHelpString := "Address of an online node of the Raft cluster to connect to."
	flag.StringVar(&addr, "connect", "", addrHelpString)
	flag.StringVar(&addr, "c", "", addrHelpString)

	flag.Parse()

	// Validate address of Raft node
	if addr == "" {
		fmt.Println("Usage: raft-client -c <addr>\nYou must specify an address for the client to connect to!")
		return
	}

	// Connect to Raft node
	client, err := raft.Connect(addr)

	if err != nil {
		fmt.Printf("Error starting client: %v\n", err)
		return
	}

	// Kick off shell
	shell := ishell.New()

	shell.AddCmd(&ishell.Cmd{
		Name: "init",
		Help: "initialize the hash machine",
		Func: func(c *ishell.Context) {
			if len(c.Args) != 1 {
				shell.Println("Usage: init <value>")
				return
			}

			resp, err := client.SendRequest(hashmachine.HashChainInit, []byte(c.Args[0]))
			if err != nil {
				shell.Println(err.Error())
			}
			shell.Println(resp)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "hash",
		Help: "perform another round of hashing",
		Func: func(c *ishell.Context) {
			resp, err := client.SendRequest(hashmachine.HashChainAdd, []byte{})
			if err != nil {
				shell.Println(err.Error())
			}
			shell.Println(resp)
		},
	})

	shell.Println(shell.HelpText())
	shell.Run()
}
