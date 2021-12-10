package lsm

import (
	"container/heap"
	//"fmt"
	"testing"
)

func TestMinHeap(t *testing.T) {
  a := SstEntry{"a", nil, false}
  b := SstEntry{"b", nil, false}
  c := SstEntry{"c", nil, false}
  d := SstEntry{"d", nil, false}
  e := SstEntry{"e", nil, false}

// This example inserts several ints into an IntHeap, checks the minimum,
// and removes them in order of priority.
	h := &SstEntryHeap{b, a, c}
	heap.Init(h)
	heap.Push(h, e)
	heap.Push(h, d)
  var s SstEntry = (*h)[0]
  if s.Key != "a" {
    t.Error("Expected min value \"A\" but received", (*h)[0])
  }
  var cur SstEntry
  var next SstEntry
  cur = heap.Pop(h).(SstEntry)
	for h.Len() > 0 {
    next = heap.Pop(h).(SstEntry)
    if next.Key <= cur.Key {
      t.Error("Expected next > cur but received next", next.Key, "and cur", cur.Key)
    }
		//fmt.Println(heap.Pop(h))
	}
}
