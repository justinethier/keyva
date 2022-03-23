package sst

import (
	"bufio"
	"github.com/justinethier/keyva/bloom"
	"os"
	"time"
)

type SstFileHeader struct {
	Seq uint64
}

type SstIndex struct {
  Key string
  offset int
}

type SstLevel struct {
	Files []SstFile
}

type SstFile struct {
	Filename string
	Filter   *bloom.Filter
	Cache    []SstEntry // cached file contents
	CachedAt time.Time  // timestamp when cache was last accessed
	// may convert to seconds (best way to compare???) using -
	//now := time.Now()      // current local time
	//sec := now.Unix()      // number of seconds since January 1, 1970 UTC
	// TODO: longer-term, we will time out the cache and have a GC that
	//       empties it if it has not been accessed for THRESHOLD
	//       will also want a default threshold and a way to change it.
	//       maybe it will be a member of LsmTree
}

type SstEntry struct {
	Key     string
	Value   []byte
	Deleted bool
}

type SstHeapNode struct {
	Seq    uint64
	Entry  *SstEntry
	Reader *bufio.Reader
}

// TODO: replace above with this one
type SstHeapNode2 struct {
	Seq    uint64
	Entry  *SstEntry
	File   *os.File
}

type SstHeap2 []*SstHeapNode2

func (h SstHeap2) Len() int           { return len(h) }
func (h SstHeap2) Less(i, j int) bool { return h[i].Entry.Key < h[j].Entry.Key }
func (h SstHeap2) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *SstHeap2) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*SstHeapNode2))
}

func (h *SstHeap2) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// An min-heap of SST entries
// Provides an easy way to sort large numbers of entries
type SstHeap []*SstHeapNode

func (h SstHeap) Len() int           { return len(h) }
func (h SstHeap) Less(i, j int) bool { return h[i].Entry.Key < h[j].Entry.Key }
func (h SstHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *SstHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*SstHeapNode))
}

func (h *SstHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
