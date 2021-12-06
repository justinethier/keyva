package wal

import (
	"testing"
)

func TestBasic(t *testing.T) {
	wal, _ := New("testdb")
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
