package main

import (
	"lockfreemachine/pkg/commons"
	serverpb "lockfreemachine/pkg/gen"
)

func pbToCommonsPackage(p *serverpb.Package) *commons.Package {
	if p == nil {
		return nil
	}
	txs := make([]*commons.Transaction, 0, len(p.GetTransactions()))
	for _, t := range p.GetTransactions() {
		ops := make([]*commons.Operation, 0, len(t.GetOperations()))
		for _, o := range t.GetOperations() {
			ops = append(ops, &commons.Operation{
				Key:       o.GetKey(),
				Value:     o.GetValue(),
				Op:        o.GetOp(),
			})
		}
		txs = append(txs, &commons.Transaction{Timestamp: t.GetTimestamp(),
			Operations: ops})
	}
	return &commons.Package{

		BrokerID:     int(p.GetBrokerId()),
		PackageID:    int(p.GetPackageId()),
		Transactions: txs,
	}
}
