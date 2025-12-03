package hashmachine

import (
	"crypto/md5"
	"errors"
	"fmt"
	"sync"
)

// Enum for types of HashMachine commands
const (
	HashChainInit uint64 = iota
	HashChainAdd
)

// HashMachine implements the raft.StateMachine interface, and represents a
// finite state machine storing a hash value. It stores a single hash value,
// which is updated by the successive application of the MD5 hash function.
type HashMachine struct {
	hash []byte
	sync.Mutex
}

func (h *HashMachine) init(data []byte) (hash string, err error) {
	h.Lock()
	defer h.Unlock()

	if len(h.hash) != 0 {
		return "", errors.New("the hash chain should only be initialized once")
	}

	h.hash = data

	return fmt.Sprintf("%v", h.hash), nil
}

func (h *HashMachine) add() (hash string, err error) {
	h.Lock()
	defer h.Unlock()

	if len(h.hash) == 0 {
		return "", errors.New("the hash chain hasn't been initialized yet")
	}

	sum := md5.Sum(h.hash)
	fmt.Printf("Hash is changing from %v to %v\n", h.hash, sum)
	h.hash = sum[:]

	return fmt.Sprintf("%v", h.hash), nil
}

// GetState returns the state of the state machine as an interface{}, which can
// be converted to the expected type using type assertions.
func (h *HashMachine) GetState() (state interface{}) {
	h.Lock()
	defer h.Unlock()

	return h.hash
}

// ApplyCommand applies the given state machine command to the HashMachine, and
// returns a message and an error if there is one.
func (h *HashMachine) ApplyCommand(command uint64, data []byte) ([]byte, error) {
	hash, err := func() (string, error) {
		switch command {
		case HashChainInit:
			return h.init(data)
		case HashChainAdd:
			return h.add()
		default:
			return "", errors.New("unknown command type")
		}
	}()
	return []byte(hash), err
}

func (h *HashMachine) Snapshot() ([]byte, error) {
	h.Lock()
	defer h.Unlock()

	return h.hash, nil
}

func (h *HashMachine) Restore(snapshot []byte) {
	h.Lock()
	defer h.Unlock()

	h.hash = snapshot
}

func (h *HashMachine) String() string {
	return fmt.Sprintf("HashMachine{%v}", h.hash)
}
