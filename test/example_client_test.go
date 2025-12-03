package test

import (
    "golang.org/x/net/context"
    raft "raft/pkg"
    "raft/pkg/hashmachine"
    "testing"
    "time"
)

// Example test making sure leaders can register the client and process the request from clients properly
func TestStudentClientInteraction_Leader(t *testing.T) {
    suppressLoggers()
    config := raft.DefaultConfig()
    cluster, _ := raft.CreateLocalCluster(config)
    defer cleanupCluster(cluster)

    time.Sleep(2 * time.Second)

    leader, err := findLeader(cluster)
    if err != nil {
        t.Fatal(err)
    }

    // This clientId value registers a client for the first time.
    clientid := 0

    // Hash initialization request
    initReq := raft.ClientRequest{
        ClientId:        uint64(clientid),
        SequenceNum:     1,
        StateMachineCmd: hashmachine.HashChainInit,
        Data:            []byte("hello"),
    }
    clientResult, _ := leader.ClientRequestCaller(context.Background(), &initReq)
    if clientResult.Status != raft.ClientStatus_OK {
        t.Fatal("Leader failed to commit a client request")
    }

    // Make sure further request is correct processed
    ClientReq := raft.ClientRequest{
        ClientId:        uint64(clientid),
        SequenceNum:     2,
        StateMachineCmd: hashmachine.HashChainAdd,
        Data:            []byte{},
    }
    clientResult, _ = leader.ClientRequestCaller(context.Background(), &ClientReq)
    if clientResult.Status != raft.ClientStatus_OK {
        t.Fatal("Leader failed to commit a client request")
    }
}

// Example test making sure the follower would reject the registration and requests from clients with correct messages
// The test on candidates can be similar with these sample tests
func TestStudentClientInteraction_Follower(t *testing.T) {
    suppressLoggers()
    config := raft.DefaultConfig()
    // set the ElectionTimeout long enough to keep nodes in the state of follower
    config.ElectionTimeout = 60 * time.Second
    config.ClusterSize = 3
    config.HeartbeatTimeout = 500 * time.Millisecond
    cluster, err := raft.CreateLocalCluster(config)
    defer cleanupCluster(cluster)

    time.Sleep(2 * time.Second)

    if err != nil {
        t.Fatal(err)
    }

    // make sure the client get the correct response while sending a request to a follower
    req := raft.ClientRequest{
        ClientId:        0,
        SequenceNum:     1,
        StateMachineCmd: hashmachine.HashChainInit,
        Data:            []byte("hello"),
    }
    clientResult, _ := cluster[0].ClientRequestCaller(context.Background(), &req)
    if clientResult.Status != raft.ClientStatus_NOT_LEADER && clientResult.Status != raft.ClientStatus_ELECTION_IN_PROGRESS {
        t.Fatal("Wrong response when sending a client request to a follower")
    }
}
