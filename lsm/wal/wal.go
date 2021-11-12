package wal

import (
 "os"
 "encoding/json"
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

type WriteAheadLog struct {
  NextId uint64
  path string
  file *os.File
  // TODO: channel?
  // want to be able to send requests to the channel from go threads,
  // and have a WAL go thread that receives data, appends to log, etc
}

type Entry struct {
  Id uint64
  Key string
  Value []byte
  Time int64
}

func New(path string) *WriteAheadLog {
  filename := "wal.log"
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
  if err != nil {
    panic(err)
  }
  wal := WriteAheadLog{0, path, f}
  return &wal
}

func (wal *WriteAheadLog) Append(key string, value []byte) {
  wal.NextId++
  e := Entry{wal.NextId, key, value, time.Now().Unix()}
  b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
  _, err = wal.file.Write(b)
	if err != nil {
		panic(err) // TODO: probably don't want to do this... ???
	}

  // TODO: start a new log file if the current one is too large
  //   maybe do this as a separate function, if we are using a channel then
  //   we want to finish append, respond to caller, then switch logs without
  //   explicitly blocking any callers
}

func (wal *WriteAheadLog) Entries() []Entry {
  var e []Entry
  // TODO: read from filesystem
  return e
}

// TODO: supply ID we need to read back to. ideally we have this so the entire log
//       does not have to be traversed all the time. eventually it will grow much
//       too large...
//
func loadFromFile(filename string) {
//  fp, err := os.Open(filename)
//	if err != nil {
//		panic(err)
//	}
//  defer os.Close(fp)

// 	r := bufio.NewReader(f)
// 	str, e := util.Readln(r)
// 	for e == nil {
// 		var data SstEntry
// 		err = json.Unmarshal([]byte(str), &data)
// 		//fmt.Println(data)
// 		buf = append(buf, data)
// 		str, e = util.Readln(r)
// 	}

}

func (wal *WriteAheadLog) Close() {
  wal.file.Close()
}

