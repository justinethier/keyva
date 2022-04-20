
![](lsm-Data Structures.drawio.png "")
![](lsm-SST Files - Level 0.drawio.png "")
![](lsm-SST Files - Level 1.drawio.png "")
![](lsm-SST w_Index.drawio.png "")

# Data structures

LSM Tree - log-structured merge tree
WAL - Write ahead log, disk storage for a transaction log of sorts. This allows reconstructing the in-memory portion of the tree in the event of service crash/restart, for data that has not been flushed to SST yet
Memtable - In-memory table holding some portion of the LSM tree
SST - sorted string table, primary data representation for storing LSM on disk

# Data Layout

- (WAL)
- Memtable - All data goes here first, an in-memory cache
- SST
  - Level - Data divided into multiple levels, starting at 0. Files at level 0 may contain overlapping data. Higher levels contain data in non-overlapping, sorted order across all files.
  - Segment - Data is divided into segments on disk, one per SST file
  - Block - Data within an SST is divided into blocks. There is one sparse index per block

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
* check SST files at level 0
* check SST files at level 1, ...

## SST

### Caching
 
 * right now an SST file is cached in memory when read. need a GC job and background thread for this

### Levels

* if we have multiple levels, what happens if a key is in more than one level?
  * check level 0, level 1, etc
* once a level is compacted do we replace SST files with compacted version? (Yes)
  * A very simple algorithm would hold off SST flushes and simply swap out files after compaction. Might be feasible for an MVP
  * how to do this concurrently?
* do we consider generalizing the code to support multiple sst levels, either now or in the future?

### Compact

* Take files from level `n`
* Compact into new file(s) at level `n + 1`
* Lock the LSM, swap in new files, and delete old files from first step

When to do this? Want a web API function and potentially a background job as well.

See article on this. Can compact at thresholds, time intervals (EG: time series DB), etc. Ultimately would want this to be flexible.

### Indexing

* need a way to index into the files
* probably makes more sense with binary files
* sparse index can index into various portions of file (store index in separate file?),
* then sequential search to find entry
* do we even cache file contents at all? maybe if more than N reads in T time

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
