package wal

import (
  "testing"
)

func TestBasic (t *testing.T) {
  wal := New(".")
  wal.Append("a", []byte("a string"))
  wal.Append("b", []byte("a string"))
  wal.Append("c", []byte("a string"))
}
