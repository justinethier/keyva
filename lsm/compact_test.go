package lsm

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestHeap(t *testing.T) {
	h := &SstEntryHeap{}
	heap.Init(h)
  h.Push(SstEntry{"33", nil, false})
  h.Push(SstEntry{"3", nil, false})
  h.Push(SstEntry{"1", nil, false})
  h.Push(SstEntry{"9", nil, false})
  fmt.Println(h.Pop())
  fmt.Println(h.Pop())
  fmt.Println(h.Pop())
  fmt.Println(h.Pop())
}

func TestSstCompact(t *testing.T) {
  compactSstFiles("testdb")
  //t.Error("TODO: Implement sst compact tests")
}
