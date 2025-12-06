package commons

import (
	"sync/atomic"
	"unsafe"
)

type PackageNode struct {
	prev *PackageNode
	next *PackageNode
	Pkg  *Package
}

type PackageList struct {
	head   *PackageNode
	tail   *PackageNode
	length int64
}

func NewPackageNode(pkg *Package) *PackageNode {
	return &PackageNode{
		Pkg: pkg,
	}
}

// Put -> prepends new package node to the list
func (l *PackageList) Put(pkg *Package) {
	newNode := NewPackageNode(pkg)

	for {
		oldHeadPtr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)))
		oldHead := (*PackageNode)(oldHeadPtr)

		newNode.next = oldHead

		if oldHead != nil {
			oldHead.prev = newNode
		} else {
			// List was empty, set tail to new node
			l.tail = newNode
		}

		if atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&l.head)),
			oldHeadPtr,
			unsafe.Pointer(newNode),
		) {
			atomic.AddInt64(&l.length, 1)
			return
		}
	}
}

// GetHead() returns the head package node
func (l *PackageList) GetHead() *PackageNode {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)))
	return (*PackageNode)(ptr)
}

// GetTail() returns the tail package node
func (l *PackageList) GetTail() *PackageNode {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail)))
	return (*PackageNode)(ptr)
}

// Length returns the length of the package list
func (l *PackageList) Length() int64 {
	return atomic.LoadInt64(&l.length)
}

func NewPackageList() *PackageList {
	return &PackageList{}
}

// Return the node stored at a certain index
func (l *PackageList) GetAtIndex(index int64) *PackageNode {
	if index < 0 || index >= l.Length() {
		return nil
	}
	current := l.GetHead()
	var i int64 = 0
	for current != nil && i < index {
		current = current.next
		i++
	}
	return current
}

// Delete a node from the list
func (l *PackageList) Delete(node *PackageNode) {
	if node == nil {
		return
	}

	// Update the previous node's next pointer
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		// Node is head
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.head)), unsafe.Pointer(node.next))
	}

	// Update the next node's prev pointer
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		// Node is tail
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.tail)), unsafe.Pointer(node.prev))
	}

	atomic.AddInt64(&l.length, -1)
}

// find first node with given package ID
func (l *PackageList) FindByPackageID(packageID int) *PackageNode {
	current := l.GetHead()
	for current != nil {
		if current.Pkg != nil && current.Pkg.PackageID == packageID {
			return current
		}
		current = current.next
	}
	return nil
}
