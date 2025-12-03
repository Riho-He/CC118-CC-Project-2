package pkg

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

// WinEADDRINUSE to support windows machines
const WinEADDRINUSE = syscall.Errno(10048)

// OpenPort creates a listener on the specified port.
func OpenPort(port int) net.Listener {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	listener, err := net.Listen("tcp4", fmt.Sprintf("%v:%v", hostname, port))
	if err != nil {
		panic(err)
	}
	return listener
}

// AddrToID converts a network address to a Raft node ID of specified length.
func AddrToID(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	v := h.Sum(nil)
	keyInt := big.Int{}
	keyInt.SetBytes(v[:length])
	return keyInt.String()
}

// randomTimeout uses time.After to create a timeout between minTimeout and 2x that.
func randomTimeout(minTimeout time.Duration) <-chan time.Time {
	// Generate a random duration between 0 and minTimeout
	// Note: We assume rand is seeded elsewhere (e.g. in cluster creation or init)
	extra := time.Duration(rand.Int63n(int64(minTimeout)))
	return time.After(minTimeout + extra)
}

// createCacheID creates a unique ID to store a client request and corresponding
// reply in cache.
func CreateCacheID(clientID, sequenceNum uint64) string {
	return fmt.Sprintf("%v-%v", clientID, sequenceNum)
}

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, u)
	return buf
}