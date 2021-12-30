// TODO: code to compact sst files on disk.
package lsm

import (
	"container/heap"
	"encoding/json"
	"fmt"
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

// compactSstFileStreams implements a simple algorithm to load all SST files at the given path into memory, compact their contents, and write the contents back out to filename.
func compactSstFiles(path string, filename string) {
	filenames := getSstFilenames(path)
	fmt.Println(filenames)

	// load all data into min heap
	h := &SstHeap{}
	heap.Init(h)

	var seqNum uint64 = 0
	for _, filename := range filenames {
		entries, header := loadEntriesFromSstFile(filename, path)
		if header.Seq > seqNum {
			seqNum = header.Seq
		}
		for _, entry := range entries {
			var e SstEntry = entry
			//fmt.Println(e)
			heap.Push(h, &SstHeapNode{header.Seq, &e})
		}
	}

  // write data out to new file(s)
	createSstFileFromHeap(filename, h, seqNum)

 // TODO: delete old files? or provide a separate function to do that
 //       might be cleanest to return list of files from above, that we can then
 //       have a caller delete, perhaps while holding the appropriate locks
}

// createSstFileFromHeap writes the contents of the given heap to a new file specified by filename.
func createSstFileFromHeap(filename string, h *SstHeap, seqNum uint64) {
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
		writeSstHeapEntry(cur, f)
		cur = next
	}

	// Special case, only one SST entry
	if next == nil {
		writeSstHeapEntry(cur, f)
	} else {
		writeSstHeapEntry(next, f)
	}
}

func writeSstHeapEntry(e *SstHeapNode, f *os.File) {
	if e.Entry.Deleted {
		return
	}

	b, err := json.Marshal(&e.Entry)
	check(err)

	_, err = f.Write(b)
	check(err)

	_, err = f.Write([]byte("\n"))
	check(err)
}
