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

// TODO: not thread safe!
package lsm

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/huandu/skiplist"
	"github.com/justinethier/keyva/bloom"
	"github.com/justinethier/keyva/lsm/wal"
	"github.com/justinethier/keyva/util"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
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
	fmt.Println("DEBUG wal seq = ", wal.Sequence())
	fmt.Println("DEBUG wal = ", entries)
	var files []SstFile
	chn := make(chan *SstEntry, 1000)
	tree := LsmTree{path: path, memtbl: buf, memtblMaxSize: bufSize, 
                  filter: f, files: files, lock: lock, wal: wal, walChan: chn}
	seq := tree.load() // Read all SST files on disk and generate bloom filters

	fmt.Println("loaded LSM tree seq =", seq)

	// if there are entries in wal that are not in SST files,
	// load them into memory
	if entries != nil {
		for _, e := range entries {
			if e.Id > seq {
				util.Trace("DEBUG loading wal id", e.Id, "entry", e.Key)
				tree.setInMemtbl(e.Key, e.Value, e.Deleted)
			}
		}
	}

	go tree.walJob()
	return &tree
}

func (tree *LsmTree) ResetDB() {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	tree.files = make([]SstFile, 0) // Clear from memory
	sstFilenames := tree.getSstFilenames()
	for _, filename := range sstFilenames {
		os.Remove(tree.path + "/" + filename) // ... and remove from disk
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
	var entry SstEntry
	val, ok := tree.get(k)
	if ok {
		n := binary.LittleEndian.Uint32(val)
		n++
		binary.LittleEndian.PutUint32(val, n)
		tree.setInMemtbl(k, val, false)
		entry = SstEntry{k, val, false}
		result = n
	} else {
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, 0)
		tree.setInMemtbl(k, bs, false)
		entry = SstEntry{k, bs, false}
		result = 0
	}
tree.lock.Unlock()

	// Add entry to Wal, flush SST if ready
	// Release locks before we do this to avoid possibility of deadlock
	tree.walChan <- &entry
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
	entry := SstEntry{k, value, deleted}
	tree.memtbl.Set(k, entry)
	tree.filter.Add(k)
}

func (tree *LsmTree) set(k string, value []byte, deleted bool) {
	entry := SstEntry{k, value, deleted}

	tree.lock.Lock()
	tree.memtbl.Set(k, entry)
	tree.filter.Add(k)
	tree.lock.Unlock()

	// Add entry to Wal, flush SST if ready
	// Release locks before we do this to avoid possibility of deadlock
	tree.walChan <- &entry
}

// Read all sst files from disk and load a bloom filter for each one into memory
func (tree *LsmTree) load() uint64 {
	var seq uint64
	sstFilenames := tree.getSstFilenames()
	for _, filename := range sstFilenames {
		//fmt.Println("DEBUG: loading bloom filter from file", filename)
		entries, header := tree.loadEntriesFromSstFile(filename)
		if header.Seq > seq {
			seq = header.Seq
		}
		filter := bloom.New(tree.memtblMaxSize, 200)
		for _, entry := range entries {
			filter.Add(entry.Key)
		}
		var sstfile = SstFile{filename, filter, []SstEntry{}, time.Now()}
		tree.files = append(tree.files, sstfile)
	}

	return seq
}

func (tree *LsmTree) flush(seqNum uint64) {
	if tree.memtbl.Len() == 0 || tree.memtbl.Len() < tree.memtblMaxSize {
		return
	}

	//fmt.Println("DEBUG called flush()")

	// Remove duplicate entries
	m := make(map[string]SstEntry)
	for elem := tree.memtbl.Front(); elem != nil; elem = elem.Next() {
		e := elem.Value
		m[elem.Key().(string)] = e.(SstEntry)
	}

	// sort list of keys and setup bloom filter
	filter := bloom.New(tree.memtblMaxSize, 200)
	keys := make([]string, 0, len(m))
	for k := range m {
		filter.Add(k)
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Flush memtbl to disk
	var filename = tree.nextSstFilename()
	createSstFile(tree.path + "/" + filename, keys, m, seqNum)

	//fmt.Println("DEBUG wrote new sst file", filename)

	// Add information to memory
	var sstfile = SstFile{filename, filter, []SstEntry{}, time.Now()}
	tree.files = append(tree.files, sstfile)

	// Clear memtbl
	tree.memtbl = skiplist.New(skiplist.String)

	// Switch to new wal
	tree.wal.Next()
}

func (tree *LsmTree) walJob() {
	for {
		v := <-tree.walChan

		//fmt.Println("walJob received", v)
		util.Trace("walJob received", v)

    // Flush SST to disk if ready
    // TODO: "right" way to do this is to make it immutable now and fire a goroutine
    //       or have a background job that does the actual flushing
		tree.lock.Lock()
		if (tree.memtbl.Len() >= tree.memtblMaxSize) {
		  util.Trace("flushing memtable to SST", tree.wal.Sequence())
			tree.flush(tree.wal.Sequence())
		}
			tree.lock.Unlock()

		if v == nil {
      tree.wg.Done()
      break;
    }

		tree.wal.Append(v.Key, v.Value, v.Deleted)
		if len(tree.walChan) == 0 {
			tree.wal.Sync()
		}
	}
}

func (tree *LsmTree) WaitForJobsToFinish() {
  tree.wg.Add(1)
  tree.walChan <- nil
  tree.wg.Wait()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func createSstFile(filename string, keys []string, m map[string]SstEntry, seqNum uint64) {
	f, err := os.Create(filename)
	check(err)

	defer f.Close()

	header := SstFileHeader{seqNum}
	b, err := json.Marshal(header)
	check(err)
	_, err = f.Write(b)
	check(err)
	_, err = f.Write([]byte("\n"))
	check(err)

	for _, k := range keys {
		b, err := json.Marshal(m[k])
		check(err)

		_, err = f.Write(b)
		check(err)

		_, err = f.Write([]byte("\n"))
		check(err)
	}
}

func (tree *LsmTree) nextSstFilename() string {
	files, err := ioutil.ReadDir(tree.path)
	if err != nil {
		log.Fatal(err)
	}

	var sstFiles []string
	for _, file := range files {
		matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
		if matched && !file.IsDir() {
			//fmt.Println(file.Name(), file.IsDir())
			sstFiles = append(sstFiles, file.Name())
		}
	}

	if len(sstFiles) > 0 {
		var latest = sstFiles[len(sstFiles)-1][20:24]
		n, _ := strconv.Atoi(latest)
		return fmt.Sprintf("sorted-string-table-%04d.json", n+1)
	}

	return "sorted-string-table-0000.json"
}

func (tree *LsmTree) getSstFilenames() []string {
	files, err := ioutil.ReadDir(tree.path)
	if err != nil {
		log.Fatal(err)
	}

	var sstFiles []string
	for _, file := range files {
		matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
		if matched && !file.IsDir() {
			sstFiles = append(sstFiles, file.Name())
		}
	}

	return sstFiles
}

func (tree *LsmTree) findLatestBufferEntryValue(key string) (SstEntry, bool) {
	var empty SstEntry

	// Early exit if we have never seen this key
	if !tree.filter.Test(key) {
		return empty, false
	}

	elem := tree.memtbl.Get(key)
	if elem != nil {
		return elem.Value.(SstEntry), true
	}

	return empty, false
}

func (tree *LsmTree) loadEntriesFromSstFile(filename string) ([]SstEntry, SstFileHeader) {
	var buf []SstEntry
	var header SstFileHeader

	f, err := os.Open(tree.path + "/" + filename)
	if err != nil {
		return buf, header
	}

	defer f.Close()

	r := bufio.NewReader(f)
	str, e := util.Readln(r)
	check(e)
	err = json.Unmarshal([]byte(str), &header)
	check(e)
	//fmt.Println("DEBUG SST header", header)

	str, e = util.Readln(r)
	for e == nil {
		var data SstEntry
		err = json.Unmarshal([]byte(str), &data)
		check(err)
		//fmt.Println(data)
		buf = append(buf, data)
		str, e = util.Readln(r)
	}

	return buf, header
}

func (tree *LsmTree) findEntryValue(key string, entries []SstEntry) (SstEntry, bool) {
	var entry SstEntry
	var left = 0
	var right = len(entries) - 1

	for left <= right {
		mid := left + int((right-left)/2)
		//fmt.Println("DEBUG FEV", key, left, right, mid, entries[mid])

		// Found the key
		if entries[mid].Key == key {
			return entries[mid], true
		}

		if entries[mid].Key > key {
			right = mid - 1 // Key would be found before this entry
		} else {
			left = mid + 1 // Key would be found after this entry
		}
	}

	return entry, false
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
	// Search in reverse order, newest file to oldest
	for i := len(tree.files) - 1; i >= 0; i-- {
		//fmt.Println("DEBUG loading entries from file", tree.files[i].filename)
		if tree.files[i].filter.Test(k) {
			// Only read from disk if key is in the filter
			var entries []SstEntry

			if len(tree.files[i].cache) == 0 {
				// No cache, read file from disk and cache entries
				entries, _ = tree.loadEntriesFromSstFile(tree.files[i].filename)
				tree.files[i].cache = entries
			} else {
				entries = tree.files[i].cache
			}
			tree.files[i].cachedAt = time.Now() // Update cached time

			// Search for key in the file's entries
			if entry, found := tree.findEntryValue(k, entries); found {
				if entry.Deleted {
					return entry.Value, false
				} else {
					return entry.Value, true
				}
			}
		}
	}

	// Key not found
	var val []byte
	return val, false
}
