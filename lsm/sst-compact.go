package lsm

// TODO: code to compact sst files on disk.


// TODO: start with X sst files
// load all data into min heap
// after data load, take data out of the heap one row at a time and write to file

// consideration - any way to stream this, to handle large datasets? maybe not easily, this will be a secondary concern

// TODO: could we use min heap to flush a single SST?
