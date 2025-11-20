package commons

import (
	"sync/atomic"
	"unsafe"
)

type StateValue struct {
	State int
	Pkg   *Package
}

func (s *StateValue) setState(state int) {
	s.State = state
}
func (s *StateValue) getState() int {
	return s.State
}
func (s *StateValue) getPackage() *Package {
	return s.Pkg
}

type StateNode struct {
	val  *StateValue
	next *StateNode
}

func NewStateNode(value *StateValue) *StateNode {
	return &StateNode{
		val: value,
	}
}

func (n *StateNode) setState(state int) {
	n.val.setState(state)
}

func (n *StateNode) GetState() int {
	return n.val.getState()
}

func (n *StateNode) GetPackage() *Package {
	return n.val.getPackage()
}

type StateList struct {
	head *StateNode
}

func (l *StateList) GetState() int {
	return l.head.GetState()
}

//	func (l *StateList) GetHead() *StateNode {
//		return l.head
//	}
func (l *StateList) GetHead() *StateNode {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)))
	return (*StateNode)(ptr)
}

func (l *StateList) UpdateState(currHead, newHead *StateNode) bool {
	// set the next pointer of the new state to the current head
	// in case of a successful swap. Otherswise, it does not matter
	// since the newState is not part of the linked list.
	newHead.next = currHead

	// return the output of the compare and swap operation.
	return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), unsafe.Pointer(currHead), unsafe.Pointer(newHead))
}

func (l *StateList) ReplaceState(newVal *StateValue) {
	for {
		old := l.GetHead()
		newHead := &StateNode{val: newVal, next: nil} // no link to old
		if atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&l.head)),
			unsafe.Pointer(old),
			unsafe.Pointer(newHead),
		) {
			return
		}
		// retry on contention
	}
}

func NewStateList() *StateList {
	ptr := &StateList{
		// Initialize the head node with a nil state and pkg value.
		head: &StateNode{
			val: &StateValue{
				State: Undefined,
				Pkg:   nil,
			},
		},
	}
	return ptr
}
