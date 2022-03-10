package sst

import (
	"log"
	"os"
	"strconv"
	"testing"
)

func TestBinary(t *testing.T) {

  var keys []string
	m := make(map[string]SstEntry)

  for i := 0; i < 10; i++ {
    key := strconv.Itoa(i)
    keys = append(keys, key)
    m[key] = SstEntry{key, []byte("Test Value"), false}
  }

  writeSst("mytest.bin", keys, m, uint64(10))

//  var lis = [] SstEntry {
//    SstEntry{"my key 1", []byte("my data 1"), true},
//    SstEntry{"my key 2", []byte("my data 2"), false},
//    SstEntry{"my key 3", []byte("my data 3"), true},
//    SstEntry{"my key 4", []byte("my data 4"), false},
//    SstEntry{"my key 5", []byte("my data 5"), false} }
//
//  writeEntries(f, lis)
}

func TestBinaryRead(t *testing.T) {
  f, err := os.Open("mytest.bin")
  if err != nil {
    log.Fatal(err)
  }
  defer f.Close()

  lis, err := readEntries(f)
  log.Println("read entries", len(lis))
}
