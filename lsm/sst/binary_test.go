package sst

import (
	"log"
	"os"
	"testing"
)

func TestBinary(t *testing.T) {
  f, err := os.Create("mytest.bin")
  if err != nil {
    log.Fatal(err)
  }
  defer f.Close()

  var lis = [] SstEntry {
    SstEntry{"my key 1", []byte("my data 1"), true},
    SstEntry{"my key 2", []byte("my data 2"), false},
    SstEntry{"my key 3", []byte("my data 3"), true},
    SstEntry{"my key 4", []byte("my data 4"), false},
    SstEntry{"my key 5", []byte("my data 5"), false} }

  writeEntries(f, lis)
}
