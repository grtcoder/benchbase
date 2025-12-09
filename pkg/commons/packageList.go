package commons

import (
	"sync/atomic"
	"unsafe"
)

type PackageNode struct {
	pkg  *Package
	next *PackageNode
}

func NewPackageNode(pkg *Package) *PackageNode {
	return &PackageNode{
		pkg: pkg,
	}
}

func (n *PackageNode) GetPackage() *Package {
	return n.pkg
}

func (n *PackageNode) GetNext() *PackageNode {
	return n.next
}

type PackageList struct {
	head *PackageNode
	len  int64
}

func NewPackageList() *PackageList {
	return &PackageList{
		head: nil,
		len:  0,
	}
}

// GetHead atomically loads the current head
func (l *PackageList) GetHead() *PackageNode {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)))
	return (*PackageNode)(ptr)
}

// Push atomically adds a new package to the head of the list.
func (l *PackageList) Push(pkg *Package) {
	newNode := NewPackageNode(pkg)
	for {
		currHead := l.GetHead()
		newNode.next = currHead
		if atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&l.head)),
			unsafe.Pointer(currHead),
			unsafe.Pointer(newNode),
		) {
			atomic.AddInt64(&l.len, 1)
			return
		}
	}
}

func (l *PackageList) PeekHead() *PackageNode {
	return l.GetHead()
}

// TryRemoveNode attempts to remove a node from the head position.
// Returns true if the node was successfully removed.
// Returns false if the node is no longer at the head (writers pushed new nodes).
func (l *PackageList) TryRemoveNode(node *PackageNode) bool {
	if node == nil {
		return false
	}
	if atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&l.head)),
		unsafe.Pointer(node),
		unsafe.Pointer(node.next),
	) {
		atomic.AddInt64(&l.len, -1)
		return true
	}
	return false
}

// RemoveNode removes a specific node from the list, even if it's not at the head.
// Safe for single reader only.
// Returns true if the node was found and removed, false if not found.
func (l *PackageList) RemoveNode(target *PackageNode) bool {
	if target == nil {
		return false
	}

	// If node at head try to remove it.
	if l.TryRemoveNode(target) {
		return true
	}

	// node is not at head, so we find it and remove it from the list.
	// Maybe: A doubly linked list would be better here.
	for {
		head := l.GetHead()
		if head == nil {
			return false
		}

		// Check if target is the head (race with TryRemoveNode)
		if head == target {
			if l.TryRemoveNode(target) {
				return true
			}
			continue
		}

		// Traverse to find the node before target
		prev := head
		curr := prev.next
		for curr != nil && curr != target {
			prev = curr
			curr = curr.next
		}

		if curr == nil {
			return false
		}

		// Found node so we can delete it.
		prev.next = target.next
		atomic.AddInt64(&l.len, -1)
		return true
	}
}

func (l *PackageList) Len() int64 {
	return atomic.LoadInt64(&l.len)
}

func (l *PackageList) IsEmpty() bool {
	return l.GetHead() == nil
}
