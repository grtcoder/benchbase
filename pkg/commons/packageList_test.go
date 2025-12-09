package commons

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestPackageListPush(t *testing.T) {
	list := NewPackageList()

	// Test empty list
	if node := list.PeekHead(); node != nil {
		t.Error("PeekHead on empty list should return nil")
	}

	// Push some packages
	pkg1 := &Package{PackageID: 1, BrokerID: 1}
	pkg2 := &Package{PackageID: 2, BrokerID: 2}
	pkg3 := &Package{PackageID: 3, BrokerID: 3}

	list.Push(pkg1)
	list.Push(pkg2)
	list.Push(pkg3)

	if list.Len() != 3 {
		t.Errorf("Expected length 3, got %d", list.Len())
	}

	// Head should be pkg3 (most recent)
	head := list.PeekHead()
	if head == nil || head.GetPackage().PackageID != 3 {
		t.Error("Head should be pkg3")
	}
}

func TestPackageListTraversal(t *testing.T) {
	list := NewPackageList()

	// Push packages 1, 2, 3, 4, 5
	for i := 1; i <= 5; i++ {
		list.Push(&Package{PackageID: i, BrokerID: i})
	}

	// Traverse and verify order (LIFO: 5, 4, 3, 2, 1)
	node := list.PeekHead()
	expected := 5
	count := 0
	for node != nil {
		if node.GetPackage().PackageID != expected {
			t.Errorf("Expected PackageID %d, got %d", expected, node.GetPackage().PackageID)
		}
		node = node.GetNext()
		expected--
		count++
	}

	if count != 5 {
		t.Errorf("Expected to traverse 5 nodes, got %d", count)
	}
}

func TestPackageListConcurrentPush(t *testing.T) {
	list := NewPackageList()
	numWriters := 10
	packagesPerWriter := 100

	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Multiple writers pushing concurrently
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < packagesPerWriter; i++ {
				list.Push(&Package{
					PackageID: writerID*1000 + i,
					BrokerID:  writerID,
				})
			}
		}(w)
	}

	wg.Wait()

	expectedTotal := int64(numWriters * packagesPerWriter)
	if list.Len() != expectedTotal {
		t.Errorf("Expected length %d, got %d", expectedTotal, list.Len())
	}

	// Verify we can traverse and remove all elements
	count := 0
	for !list.IsEmpty() {
		node := list.PeekHead()
		if node != nil {
			list.RemoveNode(node)
			count++
		}
	}

	if count != int(expectedTotal) {
		t.Errorf("Expected to remove %d packages, got %d", expectedTotal, count)
	}
}

func TestPackageListPeekAndRemove(t *testing.T) {
	list := NewPackageList()

	// Test peek on empty list
	if node := list.PeekHead(); node != nil {
		t.Error("PeekHead on empty list should return nil")
	}

	// Push packages
	pkg1 := &Package{PackageID: 1, BrokerID: 1}
	pkg2 := &Package{PackageID: 2, BrokerID: 2}
	list.Push(pkg1)
	list.Push(pkg2)

	// Peek should return head without removing
	node := list.PeekHead()
	if node == nil || node.GetPackage().PackageID != 2 {
		t.Error("PeekHead should return the head (pkg2)")
	}

	// List should still have both items
	if list.Len() != 2 {
		t.Errorf("Expected length 2 after peek, got %d", list.Len())
	}

	// Process the package (simulated)
	_ = node.GetPackage()

	// Now remove the processed node
	if !list.TryRemoveNode(node) {
		t.Error("TryRemoveNode should succeed when node is at head")
	}

	if list.Len() != 1 {
		t.Errorf("Expected length 1 after remove, got %d", list.Len())
	}

	// Verify remaining package
	remaining := list.PeekHead()
	if remaining == nil || remaining.GetPackage().PackageID != 1 {
		t.Error("Remaining should be pkg1")
	}
}

func TestPackageListRemoveNodeAfterNewPush(t *testing.T) {
	list := NewPackageList()

	// Push initial package
	pkg1 := &Package{PackageID: 1, BrokerID: 1}
	list.Push(pkg1)

	// Peek at head
	node := list.PeekHead()
	if node == nil {
		t.Fatal("PeekHead should return node")
	}

	// Simulate writer pushing new packages while we process
	pkg2 := &Package{PackageID: 2, BrokerID: 2}
	pkg3 := &Package{PackageID: 3, BrokerID: 3}
	list.Push(pkg2)
	list.Push(pkg3)

	// List is now: pkg3 -> pkg2 -> pkg1
	// Our node (pkg1) is no longer at head

	// TryRemoveNode should fail since node is not at head
	if list.TryRemoveNode(node) {
		t.Error("TryRemoveNode should fail when node is not at head")
	}

	// RemoveNode should succeed (traverses to find the node)
	if !list.RemoveNode(node) {
		t.Error("RemoveNode should succeed even when node is not at head")
	}

	if list.Len() != 2 {
		t.Errorf("Expected length 2 after remove, got %d", list.Len())
	}

	// Verify remaining packages by traversal (should be pkg3 -> pkg2)
	head := list.PeekHead()
	if head == nil || head.GetPackage().PackageID != 3 {
		t.Errorf("Expected head PackageID 3, got %v", head)
	}
	next := head.GetNext()
	if next == nil || next.GetPackage().PackageID != 2 {
		t.Errorf("Expected next PackageID 2, got %v", next)
	}
}

func TestPackageListRemoveMiddleNode(t *testing.T) {
	list := NewPackageList()

	// Push packages: list becomes 5 -> 4 -> 3 -> 2 -> 1
	for i := 1; i <= 5; i++ {
		list.Push(&Package{PackageID: i, BrokerID: i})
	}

	// Find the middle node (pkg3)
	node := list.PeekHead()
	for node != nil && node.GetPackage().PackageID != 3 {
		node = node.GetNext()
	}

	if node == nil {
		t.Fatal("Should find node with PackageID 3")
	}

	// Remove middle node
	if !list.RemoveNode(node) {
		t.Error("RemoveNode should succeed for middle node")
	}

	if list.Len() != 4 {
		t.Errorf("Expected length 4 after remove, got %d", list.Len())
	}

	// Verify list is now: 5 -> 4 -> 2 -> 1
	curr := list.PeekHead()
	expected := []int{5, 4, 2, 1}
	for i, exp := range expected {
		if curr == nil {
			t.Fatalf("List ended early at position %d", i)
		}
		if curr.GetPackage().PackageID != exp {
			t.Errorf("Position %d: expected %d, got %d", i, exp, curr.GetPackage().PackageID)
		}
		curr = curr.GetNext()
	}
}

func TestPackageListTraverseAndDeleteMultiple(t *testing.T) {
	list := NewPackageList()

	// Push packages 1-10
	for i := 1; i <= 10; i++ {
		list.Push(&Package{PackageID: i, BrokerID: i})
	}

	// Traverse and delete even-numbered packages
	node := list.PeekHead()
	for node != nil {
		next := node.GetNext() // Save next before potential delete
		if node.GetPackage().PackageID%2 == 0 {
			list.RemoveNode(node)
		}
		node = next
	}

	// Should have 5 packages left (odd numbers: 9, 7, 5, 3, 1)
	if list.Len() != 5 {
		t.Errorf("Expected length 5, got %d", list.Len())
	}

	// Verify only odd numbers remain
	curr := list.PeekHead()
	for curr != nil {
		if curr.GetPackage().PackageID%2 == 0 {
			t.Errorf("Found even PackageID %d, should have been deleted", curr.GetPackage().PackageID)
		}
		curr = curr.GetNext()
	}
}

func TestPackageListConcurrentPushWhileDeletingHead(t *testing.T) {
	list := NewPackageList()

	// Initial package at head
	pkg1 := &Package{PackageID: 1, BrokerID: 1}
	list.Push(pkg1)

	// Get reference to current head (pkg1)
	headNode := list.PeekHead()
	if headNode == nil || headNode.GetPackage().PackageID != 1 {
		t.Fatal("Head should be pkg1")
	}

	// Simulate concurrent scenario:
	// 1. Writer pushes new package to head
	// 2. Reader deletes the old head

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine: push new package
	go func() {
		defer wg.Done()
		pkg2 := &Package{PackageID: 2, BrokerID: 2}
		list.Push(pkg2)
	}()

	// Reader goroutine: delete the old head (pkg1)
	go func() {
		defer wg.Done()
		// This should succeed even if writer pushed a new head
		list.RemoveNode(headNode)
	}()

	wg.Wait()

	// After both operations:
	// - pkg2 should be in the list (pushed by writer)
	// - pkg1 should NOT be in the list (removed by reader)

	// Verify pkg1 is not in list
	node := list.PeekHead()
	for node != nil {
		if node.GetPackage().PackageID == 1 {
			t.Error("pkg1 should have been deleted")
		}
		node = node.GetNext()
	}

	// Verify pkg2 is in list
	found := false
	node = list.PeekHead()
	for node != nil {
		if node.GetPackage().PackageID == 2 {
			found = true
			break
		}
		node = node.GetNext()
	}
	if !found {
		t.Error("pkg2 should be in the list")
	}
}

func TestPackageListConcurrentPushWhileDeletingHeadStress(t *testing.T) {
	// Stress test: many concurrent pushes while reader continuously deletes head
	list := NewPackageList()
	numWriters := 5
	pushesPerWriter := 100

	var writerWg sync.WaitGroup
	writerWg.Add(numWriters)

	deletedCount := int64(0)
	stopReader := make(chan struct{})
	readerDone := make(chan struct{})

	// Reader goroutine: continuously delete head
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stopReader:
				// Drain remaining items
				for !list.IsEmpty() {
					node := list.PeekHead()
					if node != nil && list.RemoveNode(node) {
						atomic.AddInt64(&deletedCount, 1)
					}
				}
				return
			default:
				node := list.PeekHead()
				if node != nil {
					if list.RemoveNode(node) {
						atomic.AddInt64(&deletedCount, 1)
					}
				}
			}
		}
	}()

	// Writers: push concurrently
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer writerWg.Done()
			for i := 0; i < pushesPerWriter; i++ {
				list.Push(&Package{
					PackageID: writerID*1000 + i,
					BrokerID:  writerID,
				})
			}
		}(w)
	}

	// Wait for writers to finish
	writerWg.Wait()

	// Stop reader and wait for it to drain
	close(stopReader)
	<-readerDone

	expectedTotal := int64(numWriters * pushesPerWriter)
	if deletedCount != expectedTotal {
		t.Errorf("Expected to delete %d packages, got %d", expectedTotal, deletedCount)
	}

	if !list.IsEmpty() {
		t.Error("List should be empty after all deletions")
	}
}

func TestPackageListConcurrentPushWithTraverseDelete(t *testing.T) {
	list := NewPackageList()
	numWriters := 10
	packagesPerWriter := 100

	var writerWg sync.WaitGroup
	writerWg.Add(numWriters)

	// Multiple writers pushing concurrently
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer writerWg.Done()
			for i := 0; i < packagesPerWriter; i++ {
				list.Push(&Package{
					PackageID: writerID*1000 + i,
					BrokerID:  writerID,
				})
			}
		}(w)
	}

	// Wait for all writers to finish
	writerWg.Wait()

	// Single consumer: traverse and delete all
	count := 0
	for !list.IsEmpty() {
		node := list.PeekHead()
		for node != nil {
			next := node.GetNext()
			list.RemoveNode(node)
			count++
			node = next
		}
	}

	expectedTotal := numWriters * packagesPerWriter
	if count != expectedTotal {
		t.Errorf("Expected to consume %d packages, got %d", expectedTotal, count)
	}
}
