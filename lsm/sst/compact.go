package sst

import (
	"container/heap"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// Algorithm to compact large SST files using streams of data:
// Goal here is to compact without having to keep everything in memory at once
//
// - Read at least one entry from each Sst
// - Put all into the heap
// - Take first one and write to new sst
// - Read at least one more entry from the stream that the heap entry was from
// - Loop
//
// TODO: how to handle same entry in multiple SST files. only most recent is the "live" one
// TODO: func compactSstFileStreams (){}

// TODO: implementing both above streaming algorithm and a basic algorithm that
// loads all file contents into memory. Then we have both and can benchmark / compare
// them against different data sets.

// TODO: modify Compact function below to -
// - accept directory of lower-level-path (EG: L0), directory of higher-level path (previous L + 1)
// - higher-level dir may not exist
// - ... but if it does exist, need to merge those files in too
// - generate multiple new files of size n (specified by caller?? config somewhere??)
// - return names of old files to caller so they can be deleted

// At a high level we want to do the following:
// - create new compacted files. This can be done without holding any locks because all of the SST files are immutable
// - write new files into directories off to the side somewhere, so they will not be accessed
// - caller locks LSM, swaps (moves?) new files in, swaps old files out, and deletes them
// - consider using manifest file or such to indicate which directories/files are used by the LSM for its SSTs
// - when locked and caller does swaps, it also needs to clear any old files from memory, including cached contents

// deleting old files
//   - do with the appropriate locks
//   - replace all files in l+1 ?
//   - delete all files from l that were compacted with higher level. may still be files remaining in l if a flush was performed while compaction was running
//   - delete (or some portions) may need to be done by LSM because it caches SST file contents

func readNext(h *SstHeap, node *SstHeapNode, seq uint64) {
    entry, err := Readln(node.Reader)
    if err == nil {
      heap.Push(h, &SstHeapNode{seq, &entry, node.Reader})
    }
}

func Compact2(filenames []string, path string, recordsPerSst int, removeDeleted bool) (string, error){
	h := &SstHeap{}
	heap.Init(h)

  // load header, reader from each SST file
	var seqNum uint64 = 0
	for _, filename := range filenames {
		_, reader, header, err := Open(filename)
    if err != nil {
      return "", err
    }
		if header.Seq > seqNum {
			seqNum = header.Seq
		}

    entry, err := Readln(reader)
    if err == nil {
      heap.Push(h, &SstHeapNode{header.Seq, &entry, reader})
    }
  }

	tmpDir, err := ioutil.TempDir(path, "merged-sst")
	if err != nil {
		log.Fatal(err)
		return "", err
	}

	count := 0
	filename := NextFilename(path)
	f, err := os.Create(path + "/" + filename)
	check(err)
	writeSstFileHeader(f, seqNum)

	var cur, next *SstHeapNode
	if h.Len() > 0 {
		cur = heap.Pop(h).(*SstHeapNode)
    // TODO: read another entry from cur's reader
	}
	for h.Len() > 0 {
    // Get next heap entry
    next := heap.Pop(h).(*SstHeapNode)

    // TODO: write it to file (open new file if necessary)
    // TODO: see next/cur logic in createSstFiles to account for duplicate keys
    //       may need to rework this whole loop, and possibly read cur prior to looping

    // read next entry from the entry's file
    entry, err := Readln(cur.Reader)
    if err == nil {
      cur.Entry = &entry
      heap.Push(h, cur)
    }
  }

  // TODO: special case from end of createSstFiles -

	// log.Println("before special case", cur, next)
	// // Special case, only one SST entry
	// if next == nil {
	// 	if cur != nil {
	// 		writeSstEntry(f, cur.Entry, removeDeleted)
	// 	}
	// } else {
	// 	writeSstEntry(f, next.Entry, removeDeleted)
	// }

	// log.Println("done writing sst files")
	// f.Close()

  return tmpDir, nil
}

// Compact implements a simple algorithm to load all SST files at the given path into memory, compact their contents, and write the contents back out to filename.
// TODO: specify a max size per new SST file, and allow creating multiple new files
func Compact(filenames []string, path string, recordsPerSst int, removeDeleted bool) (string, error) {
	// load all data into min heap
	h := &SstHeap{}
	heap.Init(h)

	var seqNum uint64 = 0
	for _, filename := range filenames {
		entries, header := Load(filename)
		if header.Seq > seqNum {
			seqNum = header.Seq
		}
		for _, entry := range entries {
			var e SstEntry = entry
			//fmt.Println(e)
			heap.Push(h, &SstHeapNode{header.Seq, &e, nil})
		}
	}

	fmt.Println("JAE DEBUG seq num", seqNum)
	tmpDir, err := ioutil.TempDir(path, "merged-sst")
	if err != nil {
		log.Fatal(err)
		return "", err
	}
	// write data out to new file(s)
	createSstFiles(tmpDir, h, seqNum, recordsPerSst, removeDeleted)
	return tmpDir, nil
}

// createSstFiles writes the contents of the given heap to a new file specified by filename.
func createSstFiles(path string, h *SstHeap, seqNum uint64, recordsPerSst int, removeDeleted bool) {
	count := 0
	filename := NextFilename(path)
	f, err := os.Create(path + "/" + filename)
	check(err)

	writeSstFileHeader(f, seqNum)
	log.Println("after sst file header")
	var cur, next *SstHeapNode
	if h.Len() > 0 {
		cur = heap.Pop(h).(*SstHeapNode)
	}
	for h.Len() > 0 {
		next := heap.Pop(h).(*SstHeapNode)
		// Account for duplicate keys
		if next.Entry.Key == cur.Entry.Key {
			if next.Seq > cur.Seq {
				cur = next
			}
			continue
		}
		writeSstEntry(f, cur.Entry, removeDeleted)
		cur = next
		count++
		if count > recordsPerSst {
			count = 0
			f.Close()
			filename = NextFilename(path)
			f, err = os.Create(path + "/" + filename)
			check(err)
			writeSstFileHeader(f, seqNum)
		}
	}

	log.Println("before special case", cur, next)
	// Special case, only one SST entry
	if next == nil {
		if cur != nil {
			writeSstEntry(f, cur.Entry, removeDeleted)
		}
	} else {
		writeSstEntry(f, next.Entry, removeDeleted)
	}

	log.Println("done writing sst files")
	f.Close()
}
