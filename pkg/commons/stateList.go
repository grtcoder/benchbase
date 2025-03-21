package commons

import (
	"sync/atomic"
	"unsafe"
)

type StateValue struct {
	State int32
	Pkg *Package 
}

func (s *StateValue)  setState(state int32){
	s.State=state
}
func (s * StateValue) getState() int32 {
	return s.State
}
func (s *StateValue) getPackage() *Package {
	return s.Pkg
}

type StateNode struct {
	val *StateValue
	next *StateNode
}

func NewStateNode(value *StateValue) *StateNode {
	return &StateNode{
		val: value,
	}
}

func (n *StateNode) setState(state int32) {
	n.val.setState(state)
}

func (n *StateNode) GetState() int32 {
	return n.val.getState()
}

func (n *StateNode) GetPackage() *Package {
	return n.val.getPackage()
}

type StateList struct {
	head *StateNode
}

func (l *StateList) GetState() int32 {
	// Return the state of the head node atomically.
	if l.head == nil {
		return int32(Null)
	}
	return l.head.GetState()
}
func (l *StateList) GetHead() *StateNode {
	return l.head
}

func (l *StateList) UpdateState(currHead,newState *StateNode) bool {
	// set the next pointer of the new state to the current head
	// in case of a successful swap. Otherswise, it does not matter
	// since the newState is not part of the linked list.
	newState.next = currHead

	// return the output of the compare and swap operation.
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), unsafe.Pointer(currHead), unsafe.Pointer(newState))
}


func NewStateList() *StateList {
	ptr :=&StateList{
		// Initialize the head node with a nil state and pkg value.
		head: &StateNode{
			val: &StateValue{},
		},
	}
	ptr.head.setState(Null)
	return ptr
}
