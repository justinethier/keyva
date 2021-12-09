package lsm

import (
	"github.com/huandu/skiplist"
	"github.com/justinethier/keyva/bloom"
	"github.com/justinethier/keyva/lsm/wal"
	"sync"
	"time"
)

type SstEntry struct {
	Key     string
	Value   []byte
	Deleted bool
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

type LsmTree struct {
	path string
	// buffer AKA MemTable, used as initial in-memory store of new data
	memtbl          *skiplist.SkipList
	memtblMaxSize int
	filter          *bloom.Filter
	files           []SstFile
	lock            sync.RWMutex
	wal             *wal.WriteAheadLog
	walChan         chan *SstEntry
	wg              sync.WaitGroup
}

