package wal

import (
	"testing"
)

func TestBasic(t *testing.T) {
	wal := New(".")
	//wal.Reset()
	wal.Init()

TODO: how does wal know whether to start a new WAL log or append to current one???

	wal.Append("a", []byte("a string"), false)
	wal.Append("b", []byte("a string"), false)
	wal.Append("c", []byte("a string"), false)
}

// TODO: test failover by running one test to build up a WAL then
// spin up another fresh wal instance and populate it with data
// save by the first one

// func TestRecovery(t *testing.T) {
// }

// TODO: test recovery again from a snapshot. EG: recover up to ID X
