package sst

import (
	"container/heap"
	"fmt"
	"testing"
)

func TestMinHeap(t *testing.T) {
	a := &SstHeapNode{1, &SstEntry{"a", nil, false}, nil}
	b := &SstHeapNode{1, &SstEntry{"b", nil, false}, nil}
	c := &SstHeapNode{1, &SstEntry{"c", nil, false}, nil}
	d := &SstHeapNode{1, &SstEntry{"d", nil, false}, nil}
	e := &SstHeapNode{1, &SstEntry{"e", nil, false}, nil}
	//e_del := &SstEntry{"e", nil, true}

	// This example inserts several ints into an IntHeap, checks the minimum,
	// and removes them in order of priority.
	h := &SstHeap{b, a, c}
	heap.Init(h)
	heap.Push(h, e)
	heap.Push(h, d)
	var s *SstHeapNode = (*h)[0]
	if s.Entry.Key != "a" {
		t.Error("Expected min value \"A\" but received", (*h)[0])
	}
	var cur *SstHeapNode
	var next *SstHeapNode
	cur = heap.Pop(h).(*SstHeapNode)
	fmt.Println(cur.Entry)
	for h.Len() > 0 {
		next = heap.Pop(h).(*SstHeapNode)
		if next.Entry.Key <= cur.Entry.Key {
			t.Error("Expected next > cur but received next", next.Entry.Key, "and cur", cur.Entry.Key)
		}
		fmt.Println(next.Entry)
		cur = next
	}
}

//TODO: how to know which entry is the most recent if there are multiple rows with the same key??
//may need to include ID with SstEntry
/*
func TestMinHeapWithDupes(t *testing.T) {
	a := &SstEntry{"a", nil, false}
	b := &SstEntry{"b", nil, false}
	c := &SstEntry{"c", nil, false}
	d := &SstEntry{"d", nil, false}
	e := &SstEntry{"e", nil, false}
	e_del := &SstEntry{"e", nil, true}

	// This example inserts several ints into an IntHeap, checks the minimum,
	// and removes them in order of priority.
	h := &SstEntryHeap{b, a, c}
	heap.Init(h)
	heap.Push(h, e)
	heap.Push(h, e_del)
	heap.Push(h, e_del)
	heap.Push(h, e)
	heap.Push(h, e_del)
	heap.Push(h, d)
	var s *SstEntry = (*h)[0]
	if s.Key != "a" {
		t.Error("Expected min value \"A\" but received", (*h)[0])
	}
	var cur *SstEntry
	var next *SstEntry
	cur = heap.Pop(h).(*SstEntry)
	//fmt.Println(cur)
	for h.Len() > 0 {
		next = heap.Pop(h).(*SstEntry)
		if next.Key < cur.Key {
			t.Error("Expected next > cur but received next", next.Key, "and cur", cur.Key)
		}
		fmt.Println(next)
		cur = next
	}
}
*/
