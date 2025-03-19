package commons

import (
	"sync"
	"testing"
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
	// TC1: Check Initialization
	list := NewStateList()
	if list.GetState() != int32(Null) {
			t.Errorf("Expected state to be Null, got %d", list.GetState())
	}

	threadsData := [][]State{
			{NotReceived, Received, IgnoreBroker, Undefined, NotReceived},
			{Received, IgnoreBroker, Undefined, NotReceived},
			{IgnoreBroker, Undefined, NotReceived, Received},
			{Undefined, NotReceived, Received, IgnoreBroker},
	}
	expectedList := []State{}
	ch := make(chan State) // Creates a channel that carries integers

	var finalWg sync.WaitGroup
	finalWg.Add(1)
	go func()   {
		defer finalWg.Done()
		for {
				state,ok := <-ch
				if !ok {
					break
				}
				expectedList = append(expectedList, state)
		}
	}()
	var wg sync.WaitGroup
	for _, thread := range threadsData {
		thread := thread
		for id, state := range thread {
			id := id
			state := state
			wg.Add(1)
			go func() {
				defer wg.Done()
				newNode := &stateNode{
					val: &stateValue{},
				}
				newNode.setState(state)

				done := false
				for !done {
					done=list.UpdateState(list.head,newNode)
					if !done {
						t.Logf("Thread %d: Failed to update state to %d", id, state)
					}
				}	
				ch <- state		
			}()
		}
	}
	wg.Wait()
	close(ch)
	finalWg.Wait()
	temp := list.head
	ind := len(expectedList) - 1
	for temp.getState() != int32(Null) {
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
}
