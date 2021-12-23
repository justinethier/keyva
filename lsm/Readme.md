
# Data structures

LSM Tree - log-structured merge tree
WAL - Write ahead log, disk storage for a transaction log of sorts. This allows reconstructing the in-memory portion of the tree in the event of service crash/restart, for data that has not been flushed to SST yet
memtable - In-memory table holding some portion of the LSM tree
SST - sorted string table, primary data representation for storing LSM on disk

# Notes

How we ideally want this to work:

## Set

* Add to WAL
* Add to memtable
  (necessary for memtable to have a bloom filter? Or does it have efficient existance checks?)
* Flush to SST
  right now we do this after writing to WAL, so we can empty WAL at the same time. Is that a good idea?

## Get

TODO

## Other stuff

* GC cached data
* compact the SST
  how do we do this concurrently? How do we know which level of the SST to read data from?
