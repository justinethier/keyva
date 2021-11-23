package wal

import (
	"bufio"
	"encoding/json"
  "fmt"
	"github.com/justinethier/keyva/util"
  "log"
	"os"
	//"sync"
  "regexp"
  "strconv"
	"time"
  "io/ioutil"
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
	nextId  uint64
	path    string
	file    *os.File
}

type Entry struct {
	Id      uint64
	Key     string
	Value   []byte
	Deleted bool
	Time    int64
}

func New(path string) *WriteAheadLog {
	wal := WriteAheadLog{0, path, nil}
	return &wal
}

func (wal *WriteAheadLog) Init() {
	filename := wal.nextFilename()
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
  wal.file = f
}

// Entries retrives all entries from the most recent write ahead log file.
// It is presumed older entries are already written to an SST file.
func (wal *WriteAheadLog) Entries() []Entry{
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

	// Ensure data is written to file system (performance issue?)
//	err = wal.file.Sync()
//	if err != nil {
//		panic(err)
//	}

	// TODO: start a new log file if the current one is too large
	//   maybe do this as a separate function, if we are using a channel then
	//   we want to finish append, respond to caller, then switch logs without
	//   explicitly blocking any callers
	return id
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
	wal.file.Close()
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
