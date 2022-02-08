// Package lsm implements a log-structured merge-tree (LSM tree) data structure.
// Ttypically used when dealing with write-heavy workloads, the write path is
// optimized by only performing sequential writes.
//
// Records are initially inserted into an in-memory buffer (mem table).
//
// Once too many records are written, all of the records are sorted and
// flushed to a new sorted string table (SST) file on disk.
//
// Bloom filters are used to efficiently scan SST files to find the value
// for a given key.
//
// Experimenting with caching of SST file contents to memory to improve read
// performance.
//
// Other optimizations TBD including write-ahead log, sparse indexes, GC
// of cached files, compaction of SST files, etc.

package lsm

import (
	"encoding/binary"
	//"fmt"
	"github.com/huandu/skiplist"
	"github.com/justinethier/keyva/bloom"
	"github.com/justinethier/keyva/lsm/sst"
	"github.com/justinethier/keyva/lsm/wal"
	//"github.com/justinethier/keyva/util"
	//"io/ioutil"
	"log"
	"os"
	//"regexp"
	"sort"
	"sync"
	"time"
)

func New(path string, bufSize int) *LsmTree {
	// Create data directory if it does not exist
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.Mkdir(path, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}

	lock := sync.RWMutex{}
	buf := skiplist.New(skiplist.String)
	f := bloom.New(bufSize, 200)
	wal, entries := wal.New(path)
	log.Println("DEBUG wal seq =", wal.Sequence())
	log.Println("DEBUG wal =", entries)
	var files sst.SstLevel
	var sstLevels []sst.SstLevel
	sstLevels = append(sstLevels, files)
	chn := make(chan *sst.SstEntry)
	tree := LsmTree{path: path, memtbl: buf, bufferSize: bufSize,
		filter: f, sst: sstLevels, lock: lock, wal: wal, walChan: chn}
	seq := tree.load() // Read all SST files on disk and generate bloom filters

	log.Println("loaded LSM tree seq =", seq)

	if wal.Sequence() < seq {
		wal.SetSequence(seq + 1)
	}

	// if there are entries in wal that are not in SST files,
	// load them into memory
	if entries != nil {
		for _, e := range entries {
			if e.Id > seq {
				log.Println("DEBUG loading wal id", e.Id, "entry", e.Key)
				tree.setInMemtbl(e.Key, e.Value, e.Deleted)
			}
		}
	}

	go tree.walJob()
	return &tree
}

func (tree *LsmTree) SetMergeSettings(s MergeSettings) {
	tree.merge = s
}

func (tree *LsmTree) ResetDB() {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	tree.sst = make([]sst.SstLevel, 1) // Clear from memory
	sstFilenames := tree.getSstFilenames()
	for _, filename := range sstFilenames {
		os.Remove(tree.path + "/" + filename) // ... and remove from disk
	}

  sstLevels := sst.Levels(tree.path)
  for _, level := range sstLevels {
    os.RemoveAll(tree.path + "/" + level)
  }
}

func (tree *LsmTree) Set(k string, value []byte) {
	tree.set(k, value, false)
}

func (tree *LsmTree) Delete(k string) {
	var val []byte
	tree.set(k, val, true)
}

func (tree *LsmTree) Increment(k string) uint32 {
	var result uint32

	// get/set operations are synchronized to guarantee the next number is always returned
	tree.lock.Lock()
	var entry sst.SstEntry
	val, ok := tree.get(k)
	if ok {
		n := binary.LittleEndian.Uint32(val)
		n++
		binary.LittleEndian.PutUint32(val, n)
		tree.setInMemtbl(k, val, false)
		entry = sst.SstEntry{k, val, false}
		result = n
	} else {
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, 0)
		tree.setInMemtbl(k, bs, false)
		entry = sst.SstEntry{k, bs, false}
		result = 0
	}
	// Add entry to Wal, flush SST if ready
	tree.walChan <- &entry
	tree.lock.Unlock()

	return result
}

//func (tree *LsmTree) Flush() {
//	tree.lock.Lock()
//	defer tree.lock.Unlock()
//	tree.flush(tree.wal.Sequence())
//}

func (tree *LsmTree) Get(k string) ([]byte, bool) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	val, ok := tree.get(k)
	return val, ok
}

// Only set in memory do not update WAL or SST, useful for loading data at startup
func (tree *LsmTree) setInMemtbl(k string, value []byte, deleted bool) {
	entry := sst.SstEntry{k, value, deleted}
	tree.memtbl.Set(k, entry)
	tree.filter.Add(k)
}

func (tree *LsmTree) set(k string, value []byte, deleted bool) {
  var empty []byte
  if deleted {
    value = empty
  }
	entry := sst.SstEntry{k, value, deleted}

	tree.lock.Lock()
	// Add entry to Wal, flush SST if ready
	tree.walChan <- &entry
	tree.memtbl.Set(k, entry)
	tree.filter.Add(k)
	tree.lock.Unlock()
}

func (tree *LsmTree) load() uint64 {
	var seq uint64
	seq = tree.loadLevel(tree.path, 0)
	level := 0

	for _, dir := range sst.Levels(tree.path) {
		var files sst.SstLevel
		level = level + 1
		tree.sst = append(tree.sst, files)
		levelSeq := tree.loadLevel(tree.path+"/"+dir, level)
		if levelSeq > seq {
			seq = levelSeq
		}
		log.Println("DEBUG: loaded data from SST level", level)
	}

	return seq
}

func (tree *LsmTree) loadLevel(path string, level int) uint64 {
	var seq uint64
	sstFilenames := sst.Filenames(path)
	for _, filename := range sstFilenames {
		log.Println("DEBUG: loading bloom filter from file", filename)
		entries, header := sst.Load(path + "/" + filename)
		log.Println("DEBUG: sst", path, level, header)
		if header.Seq > seq {
			seq = header.Seq
		}
		filter := bloom.New(tree.bufferSize, 200)
		for _, entry := range entries {
			filter.Add(entry.Key)
		}
		var sstfile = sst.SstFile{filename, filter, []sst.SstEntry{}, time.Now()}
		tree.sst[level].Files = append(tree.sst[level].Files, sstfile)
	}

	return seq
}

func (tree *LsmTree) flush(seqNum uint64) {
	if tree.memtbl.Len() == 0 || tree.memtbl.Len() < tree.bufferSize {
		return
	}

	log.Println("DEBUG called flush()")

	// Remove duplicate entries
	m := make(map[string]sst.SstEntry)
	for elem := tree.memtbl.Front(); elem != nil; elem = elem.Next() {
		e := elem.Value
		m[elem.Key().(string)] = e.(sst.SstEntry)
	}

	// sort list of keys and setup bloom filter
	filter := bloom.New(tree.bufferSize, 200)
	keys := make([]string, 0, len(m))
	for k := range m {
		filter.Add(k)
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Flush memtbl to disk
	var filename = tree.nextSstFilename()
	sst.Create(tree.path+"/"+filename, keys, m, seqNum)

	//log.Println("DEBUG wrote new sst file", filename)

	// Add information to memory
	var sstfile = sst.SstFile{filename, filter, []sst.SstEntry{}, time.Now()}
	tree.sst[0].Files = append(tree.sst[0].Files, sstfile)

	// Clear memtbl
	tree.memtbl = skiplist.New(skiplist.String)

	// Switch to new wal
	tree.wal.Next()
}

// Merge takes all of the current SST files at level and merges them with the
// SST files at the next level of the LSM tree. Data is compacted during this
// process and any older key values or tombstones are permanently removed.
func (tree *LsmTree) Merge(level int) {
  // Overall algorithm
  // Steps to do, could make sense in sst package instead of here:
  //
  // - find path for level, get all sst files
  // - find path for level+1, get all sst files
  // - load file contents into heap (future: stream them)
  // - write files back out to new temp directory
  // - acquire tree lock
  // - swap level+1 with new directory
  // - delete all old files
  // - clear all in-memory data for files
  // - release locks, merge is done
  // - log to syslog, consider WAL

  lPath := sst.PathForLevel(tree.path, level)
  lNextPath := sst.PathForLevel(tree.path, level + 1)

  log.Println("Debug load files from", lPath, lNextPath)

  files := sst.Filenames(lPath)
  for i, _ := range files {
    files[i] = lPath + "/" + files[i]
  }
  nextLvlFiles := sst.Filenames(lNextPath)
  for i, _ := range nextLvlFiles {
    nextLvlFiles[i] = lNextPath + "/" + nextLvlFiles[i]
  }

  files = append(files, nextLvlFiles...)
  log.Println("Files", files)

  // TODO: set to true if safe to remove tombstones
  //       may only be safe if we are merging into the highest level of the tree
  removeDeleted := false 

  tmpDir, err := sst.Compact(files, tree.path, tree.bufferSize, removeDeleted)
  log.Println("Files in", tmpDir, err)

  tree.lock.Lock()
  defer tree.lock.Unlock()

  for _, filename := range files {
    os.Remove(filename)
  }

  err = os.RemoveAll(lNextPath)
  if err != nil {
    log.Fatal(err)
  }

  err = os.Rename(tmpDir, lNextPath)
  if err != nil {
    log.Fatal(err)
  }

  log.Println(tree.sst)

  // Drop and reload cache for all files from l/l+1
  // TODO: more efficient solution?
  var a, b sst.SstLevel
  tree.sst[level] = a
  tree.loadLevel(lPath, level)

  log.Println(tree.sst, len(tree.sst), level)

  if len(tree.sst) <= (level + 1) {
    log.Println("Add new level", b, "to tree")
    tree.sst = append(tree.sst, b)
  } else {
    log.Println("Update tree at level", b)
    tree.sst[level+1] = b
  }
  tree.loadLevel(lNextPath, level+1)
  log.Println("Done with merge")
}

func (tree *LsmTree) walJob() {
	for {
		v := <-tree.walChan

		//log.Println("walJob received", v)

		if v == nil {
			tree.wg.Done()
			break
		}

		tree.wal.Append(v.Key, v.Value, v.Deleted)
		//if len(tree.walChan) == 0 {
		//	tree.wal.Sync()
		//}

		// Flush SST to disk if ready
		// TODO: "right" way to do this is to make it immutable now and fire a goroutine
		//       or have a background job that does the actual flushing
		//tree.lock.Lock()
		if tree.memtbl.Len() > tree.bufferSize {
			log.Println("flushing memtable to SST", tree.wal.Sequence())
			tree.flush(tree.wal.Sequence())
		}
		//tree.lock.Unlock()
	}
}

func (tree *LsmTree) nextSstFilename() string {
	return sst.NextFilename(tree.path)
}

func (tree *LsmTree) getSstFilenames() []string {
	return sst.Filenames(tree.path)
}

func (tree *LsmTree) getSstLevels() []string {
	return sst.Levels(tree.path)
}

func (tree *LsmTree) findLatestBufferEntryValue(key string) (sst.SstEntry, bool) {
	var empty sst.SstEntry

	// Early exit if we have never seen this key
	if !tree.filter.Test(key) {
		return empty, false
	}

	elem := tree.memtbl.Get(key)
	if elem != nil {
		return elem.Value.(sst.SstEntry), true
	}

	return empty, false
}

func (tree *LsmTree) loadEntriesFromSstFile(filename string) ([]sst.SstEntry, sst.SstFileHeader) {
	return sst.Load(tree.path + "/" + filename)
}

func (tree *LsmTree) get(k string) ([]byte, bool) {
	// Check in-memory buffer
	if latestBufEntry, ok := tree.findLatestBufferEntryValue(k); ok {
		if latestBufEntry.Deleted {
			return latestBufEntry.Value, false
		} else {
			return latestBufEntry.Value, true
		}
	}

	// Not found, search the sst files
	val, found := sst.Find(k, tree.sst, tree.path)
	if found {
		return val, true
	}

	// Key not found
	return val, false
}
