package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/justinethier/keyva/util"
	"log"
	"os"
	//"sync"
	"io/ioutil"
	"regexp"
	"strconv"
	"time"
)

// TODO: write-ahead log

// Notes from https://medium.com/swlh/log-structured-merge-trees-9c8e2bea89e8
//
// WAL is a write-ahead log that is used to provide the durability of data during system failures, what it means is that when a request for write comes in, the data is first added to a WAL file (sometimes called journal) and flushed to the disk (using direct io) before updating the in-memory data structure.
// This allows for systems to recover from the WAL if it crashes before persisting the in-memory data structure to disk.
// Why not directly update the write to the disk instead of updating WAL? it’s because WAL updates are cheaper as it’s append-only and doesn’t require any restructuring of data on disk.

// Other considerations:
// - general background: https://martinfowler.com/articles/patterns-of-distributed-systems/wal.html
// - fsync (maybe use initially?) - https://stackoverflow.com/a/10862573/101258

//log entry - id, data, timestamp (?)
//id can just be an int we increment, since we need to sync access to log anyway for writing
//provide operations - append, getAll (for using log to reconstruct on startup)
//also need some means of restricting log growth. eg: segment, see links

// TODO: no thread safety, for now we rely on the caller to hold the proper locks
type WriteAheadLog struct {
	nextId uint64
	path   string
	file   *os.File
}

type Entry struct {
	Id      uint64
	Key     string
	Value   []byte
	Deleted bool
	Time    int64
}

// New creates a new instance of WriteAheadLog. It also checks to
// see if there are entries on disk from the current log, and if so
// it returns them so those entries can be loaded into memory.
func New(path string) (*WriteAheadLog, []Entry) {
	wal := WriteAheadLog{0, path, nil}
	entries := wal.entries()

	// Append to existing log
	wal.openLog(wal.currentFilename())
	return &wal, entries
}

// TODO: this is broken because SST might have a capacity of, say, 50 but that could correspond to thousands of wal records if there are updates, deletes, etc.
// maybe a better solution is to not pass an arg at all and just keep appending to the same log. Then when sst calls flush it can switch to a new wal at that time.
// everything still works because we track ID's, so partial data can be read from the wal and applied to the memtable as needed. it is OK if the wal contains entries
// that span sst files (or at least, that needs to be OK). also it is extremely unlikely that new would ever cause us to start a new log unless there are no entries
// at all. otherwise it is much more likely we would append from existing log. and if we append on a full log that is fine, because we will make everything robust enough
//  to still work in that case.

// Next closes the current log on disk and opens the next one for writing.
func (wal *WriteAheadLog) Next() {
	wal.openLog(wal.nextFilename())
}

func (wal *WriteAheadLog) Sequence() uint64 {
	return wal.nextId
}

// Reset deletes all wal files from disk
func (wal *WriteAheadLog) Reset() {
	// tree.lock.Lock()
	// defer tree.lock.Unlock()

	filenames := wal.getFilenames()
	for _, filename := range filenames {
		os.Remove(filename) // ... and remove from disk
	}
}

// openLog opens the given file as the current write-ahead-log
func (wal *WriteAheadLog) openLog(filename string) {
	wal.Close()

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	wal.file = f
}

// Entries retrives all entries from the most recent write ahead log file.
// It is presumed older entries are already written to an SST file.
func (wal *WriteAheadLog) entries() []Entry {
	filename := wal.currentFilename()
	entries, id := load(filename)
	wal.nextId = id
	return entries
}

func (wal *WriteAheadLog) Append(key string, value []byte, deleted bool) uint64 {
	//wal.lock.Lock()
	//defer wal.lock.Unlock()

	wal.nextId++
	id := wal.nextId
	e := Entry{id, key, value, deleted, time.Now().Unix()}
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	_, err = wal.file.Write(b)
	if err != nil {
		panic(err) // TODO: probably don't want to do this... ???
	}

	_, err = wal.file.Write([]byte("\n"))
	if err != nil {
		panic(err)
	}

	return id
}

func (wal *WriteAheadLog) Sync() {
	// Ensure data is written to file system (performance issue?)
	err := wal.file.Sync()
	if err != nil {
		panic(err)
	}
}

//
func load(filename string) ([]Entry, uint64) {
	var buf []Entry
	fp, err := os.Open(filename)
	if os.IsNotExist(err) {
		return buf, 0 // Empty log
	} else if err != nil {
		panic(err)
	}
	defer fp.Close()

	var i uint64 = 0
	r := bufio.NewReader(fp)
	str, e := util.Readln(r)
	for e == nil {
		var data Entry
		err = json.Unmarshal([]byte(str), &data)
		i = data.Id
		//fmt.Println(data)
		buf = append(buf, data)
		str, e = util.Readln(r)
	}

	return buf, i
}

func (wal *WriteAheadLog) Close() {
	if wal.file != nil {
		wal.file.Close()
	}
}

func (wal *WriteAheadLog) nextFilename() string {
	n := wal.latestFileId()
	return fmt.Sprintf("write-ahead-log-%04d.json", n+1)
}

func (wal *WriteAheadLog) currentFilename() string {
	n := wal.latestFileId()
	if n < 0 {
		n = 0
	}
	return fmt.Sprintf("write-ahead-log-%04d.json", n)
}

func (wal *WriteAheadLog) id2Filename(id int) string {
	return fmt.Sprintf("write-ahead-log-%04d.json", id)
}

func (wal *WriteAheadLog) latestFileId() int {
	files, err := ioutil.ReadDir(wal.path)
	if err != nil {
		log.Fatal(err)
	}

	var walFiles []string
	for _, file := range files {
		matched, _ := regexp.Match(`^write-ahead-log-[0-9]*\.json`, []byte(file.Name()))
		if matched && !file.IsDir() {
			//fmt.Println(file.Name(), file.IsDir())
			walFiles = append(walFiles, file.Name())
		}
	}

	if len(walFiles) > 0 {
		var latest = walFiles[len(walFiles)-1][16:20]
		n, _ := strconv.Atoi(latest)
		return n
	}

	return -1 // No WAL yet
}

func (wal *WriteAheadLog) getFilenames() []string {
	files, err := ioutil.ReadDir(wal.path)
	if err != nil {
		log.Fatal(err)
	}

	var walFiles []string
	for _, file := range files {
		matched, _ := regexp.Match(`^write-ahead-log-[0-9]*\.json`, []byte(file.Name()))
		if matched && !file.IsDir() {
			walFiles = append(walFiles, file.Name())
		}
	}

	return walFiles
}
