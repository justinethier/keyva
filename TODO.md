# Basic
- Change New if needed such that all database files are placed in given directory. If dir does not exist, it will be created
- Allow passing directory optionally from keyva executable
- This is from rocks - can we do this? - "Once a memtable is full, it becomes immutable and replaced by a new memtable. A background thread will flush the content of the memtable into a SST file, after which the memtable can be destroyed."
- Proper header comments for packages, review package exports, etc

# Robustness

- instead of flushing immediately, replace the memtable with a new one and keep the old memtable as an immutable table in memory (we just read from it, never write) while it is being flushed.
- random links with some interesting ideas:
  https://dev.to/creativcoder/what-is-a-lsm-tree-3d75
  https://titanwolf.org/Network/Articles/Article?AID=0e39c4d6-f59b-409f-b3e4-259285d5a9b0
  https://george24601.github.io/2019/12/27/lsm.html
  https://stackoverflow.com/questions/256511/skip-list-vs-binary-search-tree/28270537

- Expand testing to better handle below cases
  - consider chaos monkey that adds random keys over fast/slow time intervals
- Add GC for cached content
- Add a compaction thread/phase
- Key will be preserving functionality and speed while adding these "real-world" features
- other optimizations? optimal locking? sparse indexes?

# Binary (efficient) SST file format

- SST file contains header, entries
- header contains sequence number, possibly CRC, anything else?
- entry contains key length, key contents, data length, data contents, deleted flag
  - data can be set to 0 length as an optimization when it is deleted
- all length and seq number fields defined as 64-bit integers. or lower for length??
- can use single byte for deleted flag
- separate manifest, index files
  - manifest could contain header information, possibly indicate if a file is scheduled for deletion
  - index file contains sparse set of keys and their location within the file
- compress SST file on disk for additional savings (?)
- will want `cmd` tools for dealing with binary data.
  - at a minimum want a tool to convert from a binary to text (json?) format to inspect data
  - if we are going to do that it would be handy to have a converter from that text format back to binary, to allow any changes to be made in a straightforward way

# Web 
- Have a static web page that makes it easy to perform CRUD operations. EG: post entered data to a key, or update/delete that key
- Use http.DetectContentType if content type is not supplied (EG: empty string)
- API function to see all keys??
- Store content-type as separate key (key/content-type ??)

# Deployment
- Prior to making any releases, consider using `internal` directory for project layout - https://eli.thegreenplace.net/2019/simple-go-project-layout-with-modules/
- Fix bloom filter layout, maybe remove entirely and just use the remote module how go is intended to work

# Roadmap
- Build prototype (here or elsewhere) with a b-tree
- Consider original LSM paper and newer literature
- How to make everything more robust / scalable?

# Datasets

Need to build some examples using datasets on the Internet.

Some ideas:

- Take snapshots of IOT data. Ideally would be a time series oriented DB
- Snapshots of Asmodee data. Could be time series, though some data might not be (EG: dump of all players, etc)
- Finance applications? historic prices? could be time series oriented
- What else?

