package lsm

import (
	"github.com/huandu/skiplist"
	"github.com/justinethier/keyva/bloom"
	"github.com/justinethier/keyva/lsm/sst"
	"github.com/justinethier/keyva/lsm/wal"
	"sync"
)

type LsmTree struct {
	path string
	// buffer AKA MemTable, used as initial in-memory store of new data
	memtbl        *skiplist.SkipList
	bufferSize    int
	filter        *bloom.Filter
	files         []sst.SstFile
	lock          sync.RWMutex
	wal           *wal.WriteAheadLog
	walChan       chan *sst.SstEntry
	wg            sync.WaitGroup
}

