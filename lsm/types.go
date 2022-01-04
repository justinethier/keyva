package lsm

import (
	"github.com/huandu/skiplist"
	"github.com/justinethier/keyva/bloom"
	"github.com/justinethier/keyva/lsm/sst"
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
	files         []sst.SstFile
	lock          sync.RWMutex
	wal           *wal.WriteAheadLog
	walChan       chan *SstEntry
	wg            sync.WaitGroup
}

