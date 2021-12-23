
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

* check memtable(s)
  - could be more than one if we support immutable memtables
* check SST files
  - right now assumes one level
  - ideally want N levels of trees. but which level to check in that case??

## SST

### Caching
 
 * right now an SST file is cached in memory when read. need a GC job for this

### Levels

* if we have multiple levels, what happens if a key is in more than one level?
* once a level is compacted do we replace SST files with compacted version?
  * A very simple algorithm would hold off SST flushes and simply swap out files after compaction. Might be feasible for an MVP
  * how to do this concurrently?
* do we consider generalizing the code to support multiple sst levels, either now or in the future?

### Indexing

* need a way to index into the files

### binary encoding

* would probably be more efficient to write SST files in binary format instead of JSON
* might make it easier to index into a file

## Other stuff

* GC cached data
* optional TTL for keys?
* compact the SST
  when to do this? 
   * background job? 
   * time threshold?
   * have an optional on-demand way to do it
  how do we do this concurrently? How do we know which level of the SST to read data from?
