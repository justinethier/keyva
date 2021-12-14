package lsm

import (
	//"container/heap"
	"github.com/huandu/skiplist"
	"github.com/justinethier/keyva/bloom"
	"github.com/justinethier/keyva/lsm/wal"
	"sync"
	"time"
)

type LsmTree struct {
	path string
	// buffer AKA MemTable, used as initial in-memory store of new data
	memtbl        *skiplist.SkipList
	memtblMaxSize int
	filter        *bloom.Filter
	files         []SstFile
	lock          sync.RWMutex
	wal           *wal.WriteAheadLog
	walChan       chan *SstEntry
	wg            sync.WaitGroup
}

type SstFileHeader struct {
	Seq uint64
}

type SstFile struct {
	filename string
	filter   *bloom.Filter
	cache    []SstEntry // cached file contents
	cachedAt time.Time  // timestamp when cache was last accessed
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
  Seq uint64
  Entry *SstEntry
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

// This example inserts several ints into an IntHeap, checks the minimum,
// and removes them in order of priority.
//func main() {
//	h := &IntHeap{2, 1, 5}
//	heap.Init(h)
//	heap.Push(h, 3)
//	fmt.Printf("minimum: %d\n", (*h)[0])
//	for h.Len() > 0 {
//		fmt.Printf("%d ", heap.Pop(h))
//	}
