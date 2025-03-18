package commons

import (
	"sync/atomic"
	"unsafe"
)

type stateValue struct {
	state atomic.Int32
	pkg *Package 
}

func (s *stateValue) setState(state State) {
	s.state.Store(int32(state))
}

func (s * stateValue) getState() int32 {
	return s.state.Load()
}

type stateNode struct {
	val *stateValue
	next *stateNode
}

func (n *stateNode) setState(state State) {
	n.val.setState(state)
}

func (n *stateNode) getState() int32 {
	return n.val.getState()
}

type stateList struct {
	head *stateNode
}

func (l *stateList) GetState() int32 {
	// Return the state of the head node atomically.
	if l.head == nil {
		return int32(Undefined)
	}
	return l.head.getState()
}

func (l *stateList) UpdateState(currHead,newState *stateNode) bool {
	// set the next pointer of the new state to the current head
	// in case of a successful swap. Otherswise, it does not matter
	// since the newState is not part of the linked list.
	newState.next = currHead

	// return the output of the compare and swap operation.
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), unsafe.Pointer(currHead), unsafe.Pointer(newState))
}


func NewStateList() *stateList {
	ptr :=&stateList{
		// Initialize the head node with a nil state and pkg value.
		head: nil,
	}
	return ptr
}
