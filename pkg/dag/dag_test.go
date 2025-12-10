package dag

import (
	"fmt"
	"lockfreemachine/pkg/commons"
	"math/rand"
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestBasic(t *testing.T) {
	withDB(t, true, func(db *badger.DB) {
	counter := 1
	txs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val1"},
			},
			Timestamp: int64(0),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: ""},
				{Op: 3, Key: "B", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "B", Value: "val2"},
			},
			Timestamp: int64(0),
		},
	}
	expectedTxs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val1"},
			},
			Timestamp: int64(2),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: ""},
				{Op: 3, Key: "B", Value: ""},
		},
		Timestamp: int64(1),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "B", Value: "val2"},
		},
		Timestamp: int64(3),
		},
	}
	normalOut,err := Schedule(txs,&counter)
	if err!=nil {
		t.Errorf("Error in scheduling: %s",err)
	}

	if !reflect.DeepEqual(txs, expectedTxs) {
		t.Errorf("Expected scheduled map to be %#v, got %#v", expectedTxs,txs)
	}
	ordering := ExecuteParallel(db,txs, normalOut)
	t.Log(ordering)
	for i:=0 ; i<len(ordering); i++{
		for j:=i+1 ; j<len(ordering); j++{
			u:=ordering[i]
			v:=ordering[j]
			t.Logf("Checking edge from %d to %d",v,u)
			if _,ok:=normalOut[v][u];ok {
				t.Errorf("Unexpected edge from %d to %d in normalOut found",v,u)
			}
		}
	}
	})
}

func TestSelfLoop(t *testing.T) {
}

func TestBug(t *testing.T) {
	withDB(t, true, func(db *badger.DB) {
	counter := 1
	txs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val1"},
			},
			Timestamp: int64(0),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val2"},
			},
			Timestamp: int64(0),
		},
		{
			Id: 3,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: "val2"},
			},
			Timestamp: int64(0),
		},
		{
			Id: 4,
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val2"},
			},
			Timestamp: int64(0),
		},
	}
	expectedTxs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val1"},
			},
			Timestamp: int64(3),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: ""},
			},
			Timestamp: int64(1),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val2"},
			},
			Timestamp: int64(4),
		},
		{
			Id: 3,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: "val2"},
			},
			Timestamp: int64(2),
		},
		{
			Id: 4,
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val2"},
			},
			Timestamp: int64(5),
		},
	}
	normalOut,err := Schedule(txs,&counter)
	if err!=nil {
		t.Errorf("Error in scheduling: %s",err)
	}

	if !reflect.DeepEqual(txs, expectedTxs) {
		t.Errorf("Expected scheduled map to be %#v, got %#v", expectedTxs,txs)
	}
	ordering := ExecuteParallel(db,txs, normalOut)
	t.Log(ordering)
	for i:=0 ; i<len(ordering); i++{
		for j:=i+1 ; j<len(ordering); j++{
			u:=ordering[i]
			v:=ordering[j]
			t.Logf("Checking edge from %d to %d",v,u)
			if _,ok:=normalOut[v][u];ok {
				t.Errorf("Unexpected edge from %d to %d in normalOut found",v,u)
			}
		}
	}
	})
}
func TestLoop(t *testing.T) {
	withDB(t, true, func(db *badger.DB) {
	counter := 1
	txs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val1"},
				{Op: 3, Key: "C", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: ""},
				{Op: 1, Key: "B", Value: "val1"},
			},
			Timestamp: int64(0),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "C", Value: ""},
				{Op: 3, Key: "B", Value: ""},
			},
			Timestamp: int64(0),
		},
	}
	expectedTxs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val1"},
				{Op: 3, Key: "C", Value: ""},
			},
			Timestamp: int64(1),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: ""},
				{Op: 1, Key: "B", Value: "val1"},
			},
			Timestamp: int64(3),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "C", Value: ""},
				{Op: 3, Key: "B", Value: ""},
			},
			Timestamp: int64(2),
		},
	}
	normalOut,err := Schedule(txs,&counter)
	if err!=nil {
		t.Errorf("Error in scheduling: %s",err)
	}

	if !reflect.DeepEqual(txs, expectedTxs) {
		t.Errorf("Expected scheduled map to be %#v, got %#v", expectedTxs,txs)
	}
	ordering := ExecuteParallel(db,txs, normalOut)
	t.Log(ordering)
	for i:=0 ; i<len(ordering); i++{
		for j:=i+1 ; j<len(ordering); j++{
			u:=ordering[i]
			v:=ordering[j]
			t.Logf("Checking edge from %d to %d",v,u)
			if _,ok:=normalOut[v][u];ok {
				t.Errorf("Unexpected edge from %d to %d in normalOut found",v,u)
			}
		}
	}
	})
}

func TestDoubleEdgedLoop(t *testing.T) {
	withDB(t, true, func(db *badger.DB) {
	counter := 1
	txs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val1"},
				{Op: 3, Key: "B", Value: ""},
				{Op: 3, Key: "E", Value: ""},
				{Op: 1, Key: "F", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: ""},
				{Op: 1, Key: "B", Value: "val1"},
				{Op: 1, Key: "C", Value: ""},
				{Op: 3, Key: "D", Value: "val1"},
			},
			Timestamp: int64(0),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 3, Key: "C", Value: ""},
				{Op: 1, Key: "D", Value: ""},
				{Op: 1, Key: "E", Value: ""},
				{Op: 3, Key: "F", Value: ""},
			},
			Timestamp: int64(0),
		},
	}
	expectedTxs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "A", Value: "val1"},
				{Op: 3, Key: "B", Value: ""},
				{Op: 3, Key: "E", Value: ""},
				{Op: 1, Key: "F", Value: ""},
			},
			Timestamp: int64(1),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "A", Value: ""},
				{Op: 1, Key: "B", Value: "val1"},
				{Op: 1, Key: "C", Value: ""},
				{Op: 3, Key: "D", Value: "val1"},
			},
			Timestamp: int64(2),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 3, Key: "C", Value: ""},
				{Op: 1, Key: "D", Value: ""},
				{Op: 1, Key: "E", Value: ""},
				{Op: 3, Key: "F", Value: ""},
			},
			Timestamp: int64(3),
		},
	}
	normalOut,err := Schedule(txs,&counter)
	if err!=nil {
		t.Errorf("Error in scheduling: %s",err)
	}

	if !reflect.DeepEqual(txs, expectedTxs) {
		t.Errorf("Expected scheduled map to be %#v, got %#v", expectedTxs,txs)
	}
	ordering := ExecuteParallel(db,txs, normalOut)
	t.Log(ordering)
	for i:=0 ; i<len(ordering); i++{
		for j:=i+1 ; j<len(ordering); j++{
			u:=ordering[i]
			v:=ordering[j]
			t.Logf("Checking edge from %d to %d",v,u)
			if _,ok:=normalOut[v][u];ok {
				t.Errorf("Unexpected edge from %d to %d in normalOut found",v,u)
			}
		}
	}
})
}

func TestMultiLoop(t *testing.T) {
	withDB(t, true, func(db *badger.DB) {
	counter := 1
	txs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "K01", Value: "val1"},
				{Op: 1, Key: "K03", Value: "val1"},
				{Op: 1, Key: "K04", Value: "val1"},
				{Op: 3, Key: "K10", Value: ""},
				{Op: 3, Key: "K30", Value: ""},
				{Op: 3, Key: "K40", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K01", Value: ""},
				{Op: 3, Key: "K21", Value: ""},
				{Op: 1, Key: "K10", Value: "val1"},
				{Op: 1, Key: "K12", Value: "val1"},
			},
			Timestamp: int64(0),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K21", Value: "val1"},
				{Op: 1, Key: "K23", Value: "val1"},
				{Op: 3, Key: "K32", Value: ""},
				{Op: 3, Key: "K12", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 3,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K03", Value: ""},
				{Op: 3, Key: "K23", Value: ""},
				{Op: 3, Key: "K43", Value: ""},
				{Op: 1, Key: "K32", Value: "val1"},
				{Op: 1, Key: "K30", Value: "val1"},
				{Op: 1, Key: "K34", Value: "val1"},
			},
			Timestamp: int64(0),
		},
		{
			Id: 4,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K04", Value: ""},
				{Op: 3, Key: "K34", Value: ""},
				{Op: 1, Key: "K40", Value: "val1"},
				{Op: 1, Key: "K43", Value: "val1"},
			},
			Timestamp: int64(0),
		},
	}
	expectedTxs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 1, Key: "K01", Value: "val1"},
				{Op: 1, Key: "K03", Value: "val1"},
				{Op: 1, Key: "K04", Value: "val1"},
				{Op: 3, Key: "K10", Value: ""},
				{Op: 3, Key: "K30", Value: ""},
				{Op: 3, Key: "K40", Value: ""},
			},
			Timestamp: int64(1),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K01", Value: ""},
				{Op: 3, Key: "K21", Value: ""},
				{Op: 1, Key: "K10", Value: "val1"},
				{Op: 1, Key: "K12", Value: "val1"},
			},
			Timestamp: int64(4),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K21", Value: "val1"},
				{Op: 1, Key: "K23", Value: "val1"},
				{Op: 3, Key: "K32", Value: ""},
				{Op: 3, Key: "K12", Value: ""},
			},
			Timestamp: int64(2),
		},
		{
			Id: 3,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K03", Value: ""},
				{Op: 3, Key: "K23", Value: ""},
				{Op: 3, Key: "K43", Value: ""},
				{Op: 1, Key: "K32", Value: "val1"},
				{Op: 1, Key: "K30", Value: "val1"},
				{Op: 1, Key: "K34", Value: "val1"},
			},
			Timestamp: int64(3),
		},
		{
			Id: 4,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K04", Value: ""},
				{Op: 3, Key: "K34", Value: ""},
				{Op: 1, Key: "K40", Value: "val1"},
				{Op: 1, Key: "K43", Value: "val1"},
			},
			Timestamp: int64(5),
		},
	}
	normalOut,err := Schedule(txs,&counter)
	if err!=nil {
		t.Errorf("Error in scheduling: %s",err)
	}

	if !reflect.DeepEqual(txs, expectedTxs) {
		t.Errorf("Expected scheduled map to be %#v, got %#v", expectedTxs,txs)
	}
	ordering := ExecuteParallel(db,txs, normalOut)
	t.Log(ordering)
	for i:=0 ; i<len(ordering); i++{
		for j:=i+1 ; j<len(ordering); j++{
			u:=ordering[i]
			v:=ordering[j]
			t.Logf("Checking edge from %d to %d",v,u)
			if _,ok:=normalOut[v][u];ok {
				t.Errorf("Unexpected edge from %d to %d in normalOut found",v,u)
			}
		}
	}
})
}

func TestIdealWithMultiLoop(t *testing.T) {
	withDB(t, true, func(db *badger.DB) {
	counter := 1
	txs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 3, Key: "K20", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K21", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K20", Value: "val1"},
				{Op: 1, Key: "K21", Value: "val1"},
				{Op: 1, Key: "K23", Value: "val1"},
				{Op: 1, Key: "K24", Value: "val1"},
				{Op: 1, Key: "K25", Value: "val1"},
				{Op: 3, Key: "K32", Value: ""},
				{Op: 3, Key: "K42", Value: ""},
				{Op: 3, Key: "K52", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 3,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K32", Value: "val1"},
				{Op: 1, Key: "K34", Value: "val1"},
				{Op: 1, Key: "K35", Value: "val1"},
				{Op: 3, Key: "K23", Value: ""},
				{Op: 3, Key: "K43", Value: ""},
				{Op: 3, Key: "K53", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 4,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K42", Value: "val1"},
				{Op: 1, Key: "K43", Value: "val1"},
				{Op: 1, Key: "K45", Value: "val1"},
				{Op: 1, Key: "K46", Value: "val1"},
				{Op: 1, Key: "K47", Value: "val1"},
				{Op: 3, Key: "K24", Value: ""},
				{Op: 3, Key: "K34", Value: ""},
				{Op: 3, Key: "K54", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 5,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K52", Value: "val1"},
				{Op: 1, Key: "K53", Value: "val1"},
				{Op: 1, Key: "K54", Value: "val1"},
				{Op: 3, Key: "K35", Value: ""},
				{Op: 3, Key: "K45", Value: ""},
				{Op: 3, Key: "K25", Value: ""},
			},
			Timestamp: int64(0),
		},
		{
			Id: 6,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K46", Value: ""},

			},
			Timestamp: int64(0),
		},
		{
			Id: 7,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K47", Value: ""},
			},
			Timestamp: int64(0),
		},
	}
	expectedTxs := []*commons.Transaction{
		{
			Id: 0, 
			Operations: []*commons.Operation{
				{Op: 3, Key: "K20", Value: ""},
			},
			Timestamp: int64(1),
		},
		{
			Id: 1,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K21", Value: ""},
			},
			Timestamp: int64(2),
		},
		{
			Id: 2,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K20", Value: "val1"},
				{Op: 1, Key: "K21", Value: "val1"},
				{Op: 1, Key: "K23", Value: "val1"},
				{Op: 1, Key: "K24", Value: "val1"},
				{Op: 1, Key: "K25", Value: "val1"},
				{Op: 3, Key: "K32", Value: ""},
				{Op: 3, Key: "K42", Value: ""},
				{Op: 3, Key: "K52", Value: ""},
			},
			Timestamp: int64(5),
		},
		{
			Id: 3,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K32", Value: "val1"},
				{Op: 1, Key: "K34", Value: "val1"},
				{Op: 1, Key: "K35", Value: "val1"},
				{Op: 3, Key: "K23", Value: ""},
				{Op: 3, Key: "K43", Value: ""},
				{Op: 3, Key: "K53", Value: ""},
			},
			Timestamp: int64(6),
		},
		{
			Id: 4,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K42", Value: "val1"},
				{Op: 1, Key: "K43", Value: "val1"},
				{Op: 1, Key: "K45", Value: "val1"},
				{Op: 1, Key: "K46", Value: "val1"},
				{Op: 1, Key: "K47", Value: "val1"},
				{Op: 3, Key: "K24", Value: ""},
				{Op: 3, Key: "K34", Value: ""},
				{Op: 3, Key: "K54", Value: ""},
			},
			Timestamp: int64(7),
		},
		{
			Id: 5,
			Operations: []*commons.Operation{
				{Op: 1, Key: "K52", Value: "val1"},
				{Op: 1, Key: "K53", Value: "val1"},
				{Op: 1, Key: "K54", Value: "val1"},
				{Op: 3, Key: "K35", Value: ""},
				{Op: 3, Key: "K45", Value: ""},
				{Op: 3, Key: "K25", Value: ""},
			},
			Timestamp: int64(8),
		},
		{
			Id: 6,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K46", Value: ""},

			},
			Timestamp: int64(3),
		},
		{
			Id: 7,
			Operations: []*commons.Operation{
				{Op: 3, Key: "K47", Value: ""},
			},
			Timestamp: int64(4),
		},
	}
	normalOut,err := Schedule(txs,&counter)
	if err!=nil {
		t.Errorf("Error in scheduling: %s",err)
	}

	if !reflect.DeepEqual(txs, expectedTxs) {
		t.Errorf("Expected scheduled map to be %#v, got %#v", expectedTxs,txs)
	}
	ordering := ExecuteParallel(db,txs, normalOut)
	t.Log(ordering)
	for i:=0 ; i<len(ordering); i++{
		for j:=i+1 ; j<len(ordering); j++{
			u:=ordering[i]
			v:=ordering[j]
			t.Logf("Checking edge from %d to %d",v,u)
			if _,ok:=normalOut[v][u];ok {
				t.Errorf("Unexpected edge from %d to %d in normalOut found",v,u)
			}
		}
	}
})
}

func generateRandomTransactions(graph map[int]map[int]struct{}, n int64) []*commons.Transaction {
	// Implementation for generating random transactions based on the graph and number of transactions
	txs := []*commons.Transaction{}
	for i := int64(0); i < n; i++ {
		txs = append(txs, &commons.Transaction{
			Id: i,
			Operations: nil,
			Timestamp: int64(0),
		})
	}
	for from, edges := range graph {
		for to := range edges {
			txs[from].Operations = append(txs[from].Operations, &commons.Operation{
				Op: 1,
				Key: fmt.Sprintf("K%d%d", from, to),
				Value: "val",
			})
			txs[to].Operations = append(txs[to].Operations, &commons.Operation{
				Op: 3,
				Key: fmt.Sprintf("K%d%d", from, to),
				Value: "",
			})
		}
	}
	return txs
}
func generateRandomGraph(n int64, edgeProb float64) map[int]map[int]struct{} {
	// Implementation for generating a random graph with n nodes and edge probability edgeProb
	graph := make(map[int]map[int]struct{})
	for i := int64(0); i < n; i++ {
		graph[int(i)] = make(map[int]struct{})
		for j := int64(0); j < n; j++ {
			if rand.Float64() < edgeProb {
				if graph[int(i)] == nil {
					graph[int(i)] = make(map[int]struct{})
				}
				graph[int(i)][int(j)] = struct{}{}
			}
			graph[int(i)] = make(map[int]struct{})
		}
	}
	// Add edges based on edgeProb
	return graph
}

func withDB(tb testing.TB, managed bool, fn func(db *badger.DB)) {
	tb.Helper()

	// Use an isolated temp dir for each test/bench.
	opts := badger.DefaultOptions(tb.TempDir())
	// Keep it simple/fast and reduce background noise.
	opts.NumCompactors = 0
	opts.CompactL0OnClose = false

	var (
		db  *badger.DB
		err error
	)
	if managed {
		db, err = badger.OpenManaged(opts)
	} else {
		db, err = badger.Open(opts)
	}
	if err != nil {
		tb.Fatalf("open DB: %v", err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	fn(db)
}

func TestRandomGraph(t *testing.T){
	counter := 1
	n := int64(1000)
	edgeProb := 0.1
	graph := generateRandomGraph(n, edgeProb)
	txs := generateRandomTransactions(graph, n)
	normalOut,err := Schedule(txs,&counter)
	if err!=nil {
		t.Errorf("Error in scheduling: %s",err)
	}

	for i:=0 ; i<int(n); i++ {
		for j,_ := range graph[i] {
			if _,ok := graph[i][j];ok {
				// Dependency is resolved by setting timestamps.
				if txs[i].Timestamp > txs[j].Timestamp {
					continue
				}
				if _,ok := normalOut[i][j]; !ok {
					t.Errorf("Expected edge from %d to %d in normalOut not found",i,j)
				}
			}
		}
	}
	// Loop check for DAG
}