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
	wg   sync.WaitGroup
	lock sync.RWMutex
	// MemTable used as initial in-memory store of new data
	memtbl     *skiplist.SkipList
	bufferSize int
	filter     *bloom.Filter
	// Write Ahead Log used to recover data not yet stored to SST
	wal     *wal.WriteAheadLog
	walChan chan *sst.SstEntry
	// SST files are used for long-term storage
	sst      []sst.SstLevel
	merge    MergeSettings
	cooldown int
	// TODO: config Config
}

type Config struct {
	MemtblDataSize uint32
	Merge          MergeSettings
}

// Define parameters for managing the SST levels
type MergeSettings struct {
	// Merge immediately from main thread if this is set to true
	Immediate bool

	// Maximum number of SST levels
	MaxLevels int

	// Amount of time to wait before checking to see if any levels need a merge
	Interval time.Duration

	// TODO: may be best if we have a job on its own thread checking on an interval (config here)
	// to see if the following conditions are true. If so initiate a merge.
	// that job could run some merges concurrently as long as there is no conflict. Maybe we do
	// that later as an enhancement

	// Compact if data in a level reaches this size
	DataSize uint32

	// Compact if a level contains more files than this
	NumberOfSstFiles int

	// Relocate data from level 0 after this time window (in seconds) is exceeded
	TimeWindow uint32
}
