package hashmachine

import (
	"fmt"
	"reflect"
	"testing"
)

func TestInitialize(t *testing.T) {
	h := new(HashMachine)

	// Test initializing hash machine
	initialValue := []byte("3")
	hash, err := h.init(initialValue)

	if err != nil {
		t.Error(err)
	}

	if hash != "[51]" {
		t.Errorf("Expected initial value to be [51], was %v\n", hash)
	}

	// Test reinitializing hash machine
	_, err = h.init([]byte("5"))

	if err == nil {
		t.Error("Expected error when reinitializing hash machine, did not get any")
	}
}

func TestAdd(t *testing.T) {
	h1 := new(HashMachine)

	// Initialize hash machine
	initialValue := []byte("3")
	h1.init(initialValue)

	// Add to hash machine
	hash, err := h1.add()
	expectedValue := "[236 203 200 126 75 92 226 254 40 48 143 217 242 167 186 243]"

	if err != nil {
		t.Error(err)
	}

	if hash != expectedValue {
		t.Errorf("Expected %v, got %v\n", expectedValue, hash)
	}

	// Create second hash machine
	h2 := new(HashMachine)

	// Add to uninitialized machine
	_, err = h2.add()

	if err == nil {
		t.Error("Expected error when hashing uninitialized hash machine, did not get any")
	}
}

func TestGetState(t *testing.T) {
	h := new(HashMachine)

	// Test empty state is returned correctly
	state, ok := h.GetState().([]byte)

	if !ok {
		t.Error("State is either nil or not of type []byte")
	}

	if len(state) != 0 {
		t.Errorf("Expected state to be empty, was %v\n", state)
	}

	// Test []byte state is returned correctly
	initialValue := []byte("3")
	h.init(initialValue)
	state, ok = h.GetState().([]byte)

	if !ok {
		t.Error("State is either nil or not of type []byte")
	}

	if !reflect.DeepEqual(initialValue, state) {
		t.Errorf("Expected state to be %v, got %v\n", initialValue, state)
	}
}

func TestApplyCommand(t *testing.T) {
	h := new(HashMachine)

	// Test initializing hash machine
	initialValue := []byte("3")
	resp, err := h.ApplyCommand(HashChainInit, initialValue)
	if err != nil {
		t.Error(err)
	}
	if string(resp) != "[51]" {
		t.Errorf("Expected initial value to be [51], was %v\n", string(resp))
	}

	// Test adding to hash machine
	resp, err = h.ApplyCommand(HashChainAdd, nil)
	expectedValue := "[236 203 200 126 75 92 226 254 40 48 143 217 242 167 186 243]"
	if err != nil {
		t.Error(err)
	}
	if string(resp) != expectedValue {
		t.Errorf("Expected %v, got %v\n", expectedValue, resp)
	}

	// Test applying unknown command
	_, err = h.ApplyCommand(2, nil)
	if err == nil {
		t.Error("Expected error when applying unknown command, did not get any")
	}
}
func TestString(t *testing.T) {
	h := new(HashMachine)

	if str := fmt.Sprintf("%v", h); str != "HashMachine{[]}" {
		t.Errorf("Expected HashMachine{[]}, got %v\n", str)
	}

	h.init([]byte("3"))

	if str := fmt.Sprintf("%v", h); str != "HashMachine{[51]}" {
		t.Errorf("Expected HashMachine{[51]}, got %v\n", str)
	}
}
