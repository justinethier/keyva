package wal

import (
 "os"
 "encoding/json"
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
}

type Entry struct {
  Key string
  Value []byte
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
  e := Entry{key, value}
  b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
  _, err = wal.file.Write(b)
	if err != nil {
		panic(err)
	}
}

func (wal *WriteAheadLog) Entries() []Entry {
  var e []Entry
  // TODO: read from filesystem
  return e
}

func (wal *WriteAheadLog) Close() {
  wal.file.Close()
}

