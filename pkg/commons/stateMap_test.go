package commons

import (
	"fmt"
	// "sync"
	"testing"
	"runtime"
	"time"

)

func TestBasic(t *testing.T) {
	// TC1: Check Initialization
	list := NewStateList()
	if list.GetState() != int32(Undefined) {
		t.Errorf("Expected state to be Undefined, got %d", list.GetState())
	}

	// TC2: Check State Update
	newNode := &stateNode{val: &stateValue{}}
	newNode.val.setState(Received)
	if !list.UpdateState(list.head, newNode){
		t.Errorf("Failed to update state to Received")
	}
	if list.GetState() != newNode.getState() {
		t.Errorf("Expected state to be %d, got %d", newNode.getState(), list.GetState())
	}

	// TC3: Check series of updates
	ls := []State{NotReceived, Received, IgnoreBroker, Undefined}
	
	for _, state := range ls {
		newNode := &stateNode{val: &stateValue{}}
		newNode.val.setState(state)
		if !list.UpdateState(list.head, newNode) {
			t.Errorf("Failed to update state to %d", state)
		}
	}
}

func TestRaceUpdates(t *testing.T) {
	runtime.GOMAXPROCS(40)
	// TC1: Check Initialization
	list := NewStateList()
	if list.GetState() != int32(Undefined) {
			t.Errorf("Expected state to be Undefined, got %d", list.GetState())
	}

	threadsData := [][]State{
			{NotReceived, Received, IgnoreBroker, Undefined, NotReceived},
			{Received, IgnoreBroker, Undefined, NotReceived},
			{IgnoreBroker, Undefined, NotReceived, Received},
			{Undefined, NotReceived, Received, IgnoreBroker},
	}
	expectedList := []State{}
	ch := make(chan State) // Creates a channel that carries integers
	t.Run("TestMain", func (t *testing.T)   {
		t.Parallel()
		for {
			select {
			case state:= <-ch:
				expectedList = append(expectedList, state)
			case <-time.After(5 * time.Second):
				t.Log("Not reading channel")
				temp := list.head
				ind := len(expectedList) - 1
				for temp != nil {
					t.Logf("Thread %d: State %d", ind, temp.getState())
					if temp.getState() != int32(expectedList[ind]) {
						t.Fatalf("Expected state to be %d, got %d", expectedList[ind], temp.getState())
					}
					temp = temp.next
					ind--
				}
				if ind != -1 {
					t.Fatalf("Expected state to be %d, got %d", expectedList[ind], temp.getState())
				}
				if len(expectedList) == 0 {
					t.Fatal("Timeout: No states received")
				}
				return
			}
		}
	})
	for outerID, thread := range threadsData {
		thread := thread
		outerID := outerID
		for id, state := range thread {
			id := id
			state := state
			t.Run(fmt.Sprintf("TestNew#%d#%d",outerID,id), func(t *testing.T) {
				t.Parallel()
				newNode := &stateNode{
					val: &stateValue{},
				}
				newNode.setState(state)
				if done:=list.UpdateState(list.head,newNode); done{
					t.Logf("Thread %d: Updated state to %d", outerID, state)
					ch <- state
					} else {
					t.Logf("Thread %d: Failed to update state to %d", id, state)
				}			
			})
		}
	}
	
}
