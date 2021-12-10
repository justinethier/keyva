// TODO: code to compact sst files on disk.
package lsm

import (
  "container/heap"
  "fmt"
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
// TODO: func compactSstFileStreams (){}

// TODO: implementing both above streaming algorithm and a basic algorithm that 
// loads all file contents into memory. Then we have both and can benchmark / compare 
// them against different data sets.


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

