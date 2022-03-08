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

  binaryWrite(f, SstEntry{"my key", []byte("my data"), false})
}
