package sst

import (
	//"fmt"
	"unicode/utf8"
	"testing"
)

func TestBinary(t *testing.T) {
  f, err := os.Open("mytest.bin")
  defer f.Close()

  binaryWrite(f, SstEntry{"my key", []byte("my data"), false})
}
