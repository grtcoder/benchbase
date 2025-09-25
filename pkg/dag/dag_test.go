package dag

import (
	"reflect"
	"testing"
	"lockfreemachine/pkg/commons"
)

func TestBasic(t *testing.T) {
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
				{Op: 1, Key: "B", Value: ""},
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
				{Op: 1, Key: "B", Value: ""},
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
	ordering := ExecuteParallel(txs, normalOut)
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
}
func TestLoop(t *testing.T) {
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
	ordering := ExecuteParallel(txs, normalOut)
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
}

func TestDoubleEdgedLoop(t *testing.T) {
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
	ordering := ExecuteParallel(txs, normalOut)
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
}