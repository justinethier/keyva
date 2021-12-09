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

/*
// Test loading data from the WAL
func TestWal(t *testing.T) {
	//os.Remove("wal.log")
	w, _ := wal.New("testdb")
	w.Append("a", []byte("1"), false)
	w.Append("b", []byte("2"), false)
	w.Append("c", []byte("3"), false)
	w.Append("d", []byte("4"), false)
	w.Append("e", []byte("5"), false)
	w.Append("f", []byte("6"), false)
	w.Append("g", []byte("7"), false)
	w.Close()

	var tbl = New("testdb", 25)
	tbl.Set("h", []byte("8"))
	if v, found := tbl.Get("a"); found {
		if bytes.Compare(v, []byte("1")) != 0 {
			t.Error("Unexpected value", v, "for key", "a")
		}
	} else {
		t.Error("Value not found for key a")
	}
}
*/
