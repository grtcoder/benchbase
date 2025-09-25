package dag

import (
	"errors"
	"fmt"
	"lockfreemachine/pkg/commons"
	"sync"
	"sync/atomic"
)

// TODO: Use this to interact with LSM tree
func executeTransaction(tx *commons.Transaction) {
	for _, op := range tx.Operations {
		switch op.Op {
		case 1:
			// Handle write
			fmt.Printf("[T%d] WRITE key=%s, value=%s\n", tx.Id, op.Key, op.Value)
		case 2:
			// Handle delete
			fmt.Printf("[T%d] DELETE key=%s\n", tx.Id, op.Key)
		case 3:
			// Handle read
			fmt.Printf("[T%d] READ key=%s\n", tx.Id, op.Key)
		default:
			fmt.Printf("[T%d] Unknown operation: %d\n", tx.Id, op.Op)
		}
	}
}

func ExecuteParallel(txs []*commons.Transaction, normalOut map[int]map[int]bool) []int {
	n := len(txs)
	inDegree := make(map[int]int)
	for u := range normalOut {
		for v := range normalOut[u] {
			inDegree[v]++
		}
	}

	var mu sync.Mutex
	readyCh := make(chan int, n)
	for i := 0; i < n; i++ {
		if inDegree[i] == 0 {
			readyCh <- i
		}
	}

	ordering:= []int{}
	// Spawn worker goroutines
	var numProcessed atomic.Int64
	for txID := range readyCh {
				go func(txID int) {
					executeTransaction(txs[txID])
					numProcessed.Add(1)
					// Record execution order
					ordering=append(ordering,txID)
						mu.Lock()
						for v := range normalOut[txID] {
							inDegree[v]--
							if inDegree[v] == 0 {
								readyCh <- v
							}
						}
						mu.Unlock()
						if int(numProcessed.Load()) == n {
							close(readyCh)
						}
				}(txID)
	}
	
	return ordering
}

// Schedule applies the Multicopy Directionality Algorithm to a list of transactions and returns timestamps assigned and dependency graph for execution
func Schedule(txs []*commons.Transaction,counter *int) (map[int]map[int]bool, error) {
	if len(txs) == 0 {
		return nil, errors.New("no transactions to schedule")
	}
	n := len(txs)

	// --- Phase 0: Initialize structures ---
	loopIn, loopOut := make(map[int]map[int]bool), make(map[int]map[int]bool)
	for i := 0; i < n; i++ {
		loopIn[i], loopOut[i] = map[int]bool{}, map[int]bool{}
	}

	// Track write operations
	writes := make(map[string][]int)
	for i, tx := range txs {
		for _, op := range tx.Operations {
			if op.Op == 1 { // Write
				writes[op.Key] = append(writes[op.Key], i)
			}
		}
	}

	// --- Phase 1: Build Loop Edges (Read-after-write dependencies) ---
	for j, tx := range txs {
		for _, op := range tx.Operations {
			if op.Op == 3 { // Read
				for _, i := range writes[op.Key] {
					loopOut[i][j] = true
					loopIn[j][i] = true
				}
			}
		}
	}

	// --- Phase 2: Resolve Ideal Nodes (No outgoing loop edges) ---
	U := make(map[int]bool, n)
	for i := 0; i < n; i++ {
		U[i] = true
	}
	TS := make([]int, n)
	assigned := make([]bool, n)

	for {
		ideal := []int{}
		for i := range U {
			if len(loopOut[i]) == 0 {
				ideal = append(ideal, i)
			}
		}
		if len(ideal) == 0 {
			break
		}

		best := ideal[0]
		bestCnt := len(loopIn[best])
		for _, id := range ideal[1:] {
			cnt := len(loopIn[id])
			// TODO : identifier need not be integer always
			if cnt > bestCnt || (cnt == bestCnt && id < best) {
				best, bestCnt = id, cnt
			}
		}

		// Resolve best ideal node
		for m := range loopIn[best] {
			delete(loopOut[m], best)
		}
		loopIn[best] = map[int]bool{}
		TS[best] = *counter
		assigned[best] = true
		*counter++
		delete(U, best)
	}

	// --- Phase 3: Convert Loop Edges to Normal Edges ---
	normalOut := make(map[int]map[int]bool)
	for i := 0; i < n; i++ {
		normalOut[i] = map[int]bool{}
	}

	for len(U) > 0 {
		// Pick unresolved node with largest loopIn
		best, bestCnt := -1, -1
		for i := range U {
			cnt := len(loopIn[i])
			if cnt > bestCnt || (cnt == bestCnt && i < best) {
				best, bestCnt = i, cnt
			}
		}

		// Convert remaining loop edges to normal
		for m := range loopOut[best] {
			normalOut[best][m] = true
		}
		for m := range loopIn[best] {
			delete(loopOut[m], best)
		}
		for m := range loopOut[best] {
			delete(loopIn[m], best)
		}
		loopIn[best], loopOut[best] = map[int]bool{}, map[int]bool{}

		TS[best] = *counter
		*counter++
		delete(U, best)
	}

	// --- Phase 4: Build Final Execution Order ---
	for id,t := range TS {
		txs[id].Timestamp = int64(t)
	}
	// execOrder := make([]int, n)
	// for id, t := range TS {
	// 	execOrder[t] = id
	// }

	// scheduled := make([]*commons.Transaction, n)
	// for i, txID := range execOrder {
	// 	scheduled[i] = txs[txID]
	// }

	return normalOut, nil
}