package sst

import (
	"bufio"
	"container/heap"
	"io/ioutil"
	"log"
	"os"
)

// Compact performs a k-way merge of data from the given SST files under
// the given path.
//
// The recordsPerSst parameter determines the maximum number of records written
// to each new SST file.
//
// The removeDeleted parameter indicates whether tombstones will be permanently
// removed (true) or carried through to the new SST files (false).
//
// Data must be sorted within each input SST file. That data is streamed from
// each file one entry at a time and added to a min heap. We then read from the
// heap to get the next sorted element and write it out to a new SST file.
//
// This process is performed for all input data, generating a new set of SST
// files containing sorted and non-overlapping data.
//
// Thus we can handle large files as only a small portion of data is kept in memory at once.
//
// If there are any duplicate keys, only the most recent entry (IE largest sequence number)
// is written. Note this is only applicable to SST level 0 which contains SST files that
// may contain overlapping data.
//
func Compact(filenames []string, path string, recordsPerSst int, removeDeleted bool) (string, error) {
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

		pushNextToHeap(h, reader, header.Seq)
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
		pushNextToHeap(h, cur.Reader, cur.Seq)
	}
	for h.Len() > 0 {
		// Get next heap entry
		next := heap.Pop(h).(*SstHeapNode)
		pushNextToHeap(h, next.Reader, next.Seq)

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

// pushNextToHeap is a helper function to read the next entry from the given file reader and push it onto the heap.
func pushNextToHeap(h *SstHeap, reader *bufio.Reader, seq uint64) {
	entry, err := Readln(reader)
	if err == nil {
		heap.Push(h, &SstHeapNode{seq, &entry, reader})
	}
}

func pushNextToHeap2(h *SstHeap, f *os.File, seq uint64) {
	entry, err := readEntry(f)
	if err == nil {
		heap.Push(h, &SstHeapNode2{seq, &entry, f})
	}
}

// Lot of logic duplicated with writeSst. consider consolidation??
func Compact2(filenames []string, path string, recordsPerSst int, keysPerSegment int, removeDeleted bool) (string, error) {
	h := &SstHeap{}
	heap.Init(h)

	// load header, file pointer from each SST
	var seqNum uint64 = 0
	for _, filename := range filenames {
    _, header, err := readIndexFile(filename)
		if err != nil {
			return "", err
		}
		if header.Seq > seqNum {
			seqNum = header.Seq
		}
    f, err := os.Open(filename)
		if err != nil {
      log.Fatal(err)
			return "", err
		}
    defer f.Close()

		pushNextToHeap2(h, f, header.Seq)
	}

	tmpDir, err := ioutil.TempDir(path, "merged-sst")
	if err != nil {
		log.Fatal(err)
		return "", err
	}

TODO: 

  // create index/sst files
  count := 0
  offset := 0
  createFiles := func() (*os.File, *os.File) {
    filename := NextFilename(tmpDir)
    indexFilename := indexFileForBin(filename)
    fbin, err := os.Create(filename)
    check(err)
    fidx, err := os.Create(indexFilename)
    check(err)
    return fbin, fidx 
  }
  myWriteEntry := func (f *os.File, data SstEntry) (int, error) {
TODO: 
	if (e.Deleted && removeDeleted) || e == nil {
		return
	}
    bytes, err := writeEntry(f, data)
    return bytes, err
  }
  fbin, fidx := createFiles()
	// write seq header to index file
	err = binary.Write(fidx, binary.LittleEndian, seqNum)
  check(err)

  // while data
	var cur, next *SstHeapNode2
	if h.Len() > 0 {
		cur = heap.Pop(h).(*SstHeapNode2)
		pushNextToHeap2(h, cur.File, cur.Seq)
	}
	for h.Len() > 0 {
     // get next entry
		// Get next heap entry
		next := heap.Pop(h).(*SstHeapNode2)
		pushNextToHeap2(h, next.File, next.Seq)

		// Account for duplicate keys
		if next.Entry.Key == cur.Entry.Key {
			if next.Seq > cur.Seq {
				cur = next
			}
			continue
		}

     // write data to index and SST file
     bytes, err := writeEntry(fbin, cur.Entry) TODO: removedeleted
     count++
     if count > recordsPerSst {
        count = 0
        fbin.Close()
        fidx.Close()
        fbin, fidx = createFiles()
	      err = binary.Write(fidx, binary.LittleEndian, seqNum)
        check(err)
     }
  // handle special cases

  log.Println("before special case", cur, next)
  // Special case, only one SST entry
  if next == nil {
  	if cur != nil {
  		// TODO: writeSstEntry(f, cur.Entry, removeDeleted)
  	}
  } else {
  	// TODO: writeSstEntry(f, next.Entry, removeDeleted)
  }

  log.Println("done writing sst files")
  fbin.Close()
  fidx.Close()

  return tmpDir, nil
}
