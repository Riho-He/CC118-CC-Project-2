package pkg

import (
	"errors"
	"fmt"
	"time"
)

// Client represents a client that connects to a known node in the Raft cluster
// to issue commands. It can be used by a CLI, web app, or other application to
// interface with Raft.
type Client struct {
	ID          uint64           // Client ID, determined by the Raft node
	Leader      *RemoteNode // Raft node we're connected to (also last known leader)
	SequenceNum uint64           // Sequence number of the latest request sent by the client
}

// MaxRetries is the maximum times the Client will retry a request after
// receiving a REQ_FAILED reply from the Raft cluster
const MaxRetries = 3

// Connect creates a new Client and registers with the Raft node at the given address.
func Connect(addr string) (cp *Client, err error) {
	cp = new(Client)

	// Note: we don't yet know the ID of the remoteNode, so just set it to an
	// empty string.
	remoteNode := &RemoteNode{Id: "", Addr: addr}

	var reply *ClientReply
	retries := 0
LOOP:
	for retries < MaxRetries {
		reply, err = remoteNode.RegisterClientRPC()
		if err != nil {
			return
		}

		switch reply.Status {
		case ClientStatus_OK:
			fmt.Printf("%v is the leader. Client successfully created.\n", remoteNode)
			break LOOP
		case ClientStatus_REQ_FAILED:
			fmt.Printf("Request failed, retrying...\n")
			retries++
		case ClientStatus_NOT_LEADER:
			// The person we've contacted isn't the leader. Use their hint to find
			// the leader.
			fmt.Printf("%v is not the leader, but thinks that %v is\n", remoteNode, reply.LeaderHint)
			remoteNode = reply.LeaderHint
		case ClientStatus_ELECTION_IN_PROGRESS:
			// An election is in progress. Accept the hint and wait an appropriate
			// amount of time, so the election can finish.
			fmt.Printf("%v is not the leader, but thinks that %v is\n", remoteNode, reply.LeaderHint)
			remoteNode = reply.LeaderHint
			time.Sleep(time.Millisecond * 200)
		case ClientStatus_CLUSTER_NOT_STARTED:
			return nil, errors.New("cluster hasn't started")
		default:
		}
	}

	// We've registered with the leader!
	cp.ID = reply.ClientId
	cp.Leader = remoteNode

	return
}

// SendRequest sends a command the associated data to the last known leader of
// the Raft cluster, and handles responses.
func (c *Client) SendRequest(command uint64, data []byte) ([]byte, error) {
	request := ClientRequest{
		ClientId:        c.ID,
		SequenceNum:     c.SequenceNum,
		StateMachineCmd: command,
		Data:            data,
	}

	c.SequenceNum++

	var reply *ClientReply
	var err error
	retries := 0
LOOP:
	for retries < MaxRetries {
		reply, err = c.Leader.ClientRequestRPC(&request)
		if err != nil {
			return nil, err
		}

		switch reply.Status {
		case ClientStatus_OK:
			fmt.Printf("%v is the leader\n", c.Leader)
			fmt.Printf("Request returned \"%v\"\n", reply.Response)
			break LOOP
		case ClientStatus_REQ_FAILED:
			fmt.Printf("Request failed: %v\n", reply.Response)
			fmt.Println("Retrying...")
			retries++
		case ClientStatus_NOT_LEADER:
			// The person we've contacted isn't the leader. Use their hint to find
			// the leader.
			c.Leader = reply.LeaderHint
		case ClientStatus_ELECTION_IN_PROGRESS:
			// An election is in progress. Accept the hint and wait an appropriate
			// amount of time, so the election can finish.
			c.Leader = reply.LeaderHint
			time.Sleep(time.Millisecond * 200)
		case ClientStatus_CLUSTER_NOT_STARTED:
			return nil, errors.New("cluster hasn't started")
		}
	}

	return reply.GetResponse(), nil
}
