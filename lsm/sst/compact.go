package sst

import (
	"container/heap"
	"encoding/binary"
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
func Compact(filenames []string, path string, recordsPerSst int, keysPerSegment int, removeDeleted bool) (string, error) {
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

		pushNextToHeap(h, f, header.Seq)
	}

	tmpDir, err := ioutil.TempDir(path, "merged-sst")
	if err != nil {
		log.Fatal(err)
		return "", err
	}

  // create index/sst files
  count := 0
	var offset int = 0
  createFiles := func() (*os.File, *os.File) {
    filename := NextFilename(tmpDir)
    indexFilename := indexFileForBin(filename)
    fbin, err := os.Create(tmpDir + "/" + filename)
    check(err)
    fidx, err := os.Create(tmpDir + "/" + indexFilename)
    check(err)
    return fbin, fidx 
  }
  myWriteEntry := func (f *os.File, fidx *os.File, e *SstEntry, removeDeleted bool) {
	  if (e.Deleted && removeDeleted) || e == nil {
		  return
	  }
		log.Println("Debug compact writing entry", e.Key)
    bytes, _ := writeEntry(f, e)
    offset += bytes
		if (count % keysPerSegment) == 0 {
			log.Println("Debug compact writing to index", e.Key)
			writeKeyToIndex(fidx, e.Key, offset)
		}
  }
  fbin, fidx := createFiles()
	// write seq header to index file
	err = binary.Write(fidx, binary.LittleEndian, seqNum)
  check(err)

  // while data
	var cur, next *SstHeapNode
	if h.Len() > 0 {
		cur = heap.Pop(h).(*SstHeapNode)
		pushNextToHeap(h, cur.File, cur.Seq)
	}
	for h.Len() > 0 {
		// Get next heap entry
		next := heap.Pop(h).(*SstHeapNode)
		pushNextToHeap(h, next.File, next.Seq)

		// Account for duplicate keys
		if next.Entry.Key == cur.Entry.Key {
			if next.Seq > cur.Seq {
				cur = next
			}
			continue
		}

     // write data to index and SST file
     myWriteEntry(fbin, fidx, cur.Entry, removeDeleted)
     cur = next
     count++
     if count > recordsPerSst {
        count = 0
        fbin.Close()
        fidx.Close()
        fbin, fidx = createFiles()
	      err = binary.Write(fidx, binary.LittleEndian, seqNum)
        check(err)
     }
  }

  log.Println("before special case", cur, next)
  // Special case, only one SST entry
  if next == nil {
  	if cur != nil {
      myWriteEntry(fbin, fidx, cur.Entry, removeDeleted)
  	}
  } else {
    myWriteEntry(fbin, fidx, next.Entry, removeDeleted)
  }

  log.Println("done writing sst files")
  fbin.Close()
  fidx.Close()

  return tmpDir, nil
}

func pushNextToHeap(h *SstHeap, f *os.File, seq uint64) {
	entry, err := readEntry(f)
	if err == nil {
		heap.Push(h, &SstHeapNode{seq, &entry, f})
	}
}

