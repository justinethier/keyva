package lsm

import (
  "container/heap"
  "fmt"
)

// TODO: code to compact sst files on disk.



// consideration - any way to stream this, to handle large datasets? maybe not easily, this will be a secondary concern

// TODO: could we use min heap to flush a single SST?

func compactSstFiles(path string) {
  filenames := getSstFilenames(path)
  fmt.Println(filenames)
  // TODO: start with X sst files

  // for ?? {
  // }

  // load all data into min heap
	h := &SstEntryHeap{b, a, c}
	heap.Init(h)
  // after data load, take data out of the heap one row at a time and write to file
}

