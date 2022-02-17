package sst

import (
	"container/heap"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// pushNextToHeap reads the next entry from the given file reader and pushes it onto the heap.
func pushNextToHeap(h *SstHeap, node *SstHeapNode) {
    entry, err := Readln(node.Reader)
    if err == nil {
      heap.Push(h, &SstHeapNode{node.Seq, &entry, node.Reader})
    }
}

// Compact performs a k-way merge of data from the given SST files. 
// 
// As data in each input SST file is sorted, that data is streamed from each
// file one entry at a time and added to a min heap. We then read from the 
// heap to get the next sorted element and write it out to a new SST file. 
//
// Ultimately a new set of SST files is generated containing sorted, non-overlapping data.
// 
// Thus we can handle large files as only a small portion of data is kept in memory at once.
//
// If there are any duplicate keys, only the most recent entry (IE largest sequence number)
// is written. Note this is only applicable to SST level 0 which contains SST files that
// may contain overlapping data.
//
func Compact(filenames []string, path string, recordsPerSst int, removeDeleted bool) (string, error){
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
	filename := NextFilename(tmpDir)
	f, err := os.Create(tmpDir + "/" + filename)
	check(err)
	writeSstFileHeader(f, seqNum)

	var cur, next *SstHeapNode
	if h.Len() > 0 {
		cur = heap.Pop(h).(*SstHeapNode)
    pushNextToHeap(h, cur)
	}
	for h.Len() > 0 {
    // Get next heap entry
    next := heap.Pop(h).(*SstHeapNode)
    pushNextToHeap(h, next)

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
			filename = NextFilename(tmpDir)
			f, err = os.Create(tmpDir + "/" + filename)
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

  return tmpDir, nil
}

// Compact implements a simple algorithm to load all SST files at the given path into memory, compact their contents, and write the contents back out to filename.
func CompactOld(filenames []string, path string, recordsPerSst int, removeDeleted bool) (string, error) {
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
