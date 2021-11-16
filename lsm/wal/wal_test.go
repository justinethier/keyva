package wal

import (
  "testing"
  "os"
)

func TestBasic (t *testing.T) {
  os.Remove("wal.log")
  wal := New(".")
  wal.Append("a", []byte("a string"))
  wal.Append("b", []byte("a string"))
  wal.Append("c", []byte("a string"))
}


// TODO: test failover by running one test to build up a WAL then
// spin up another fresh wal instance and populate it with data
// save by the first one

// func TestRecovery(t *testing.T) {
// }


// TODO: test recovery again from a snapshot. EG: recover up to ID X
