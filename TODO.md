# Basic
- Proper header comments for packages, review package exports, etc

# Robustness
- Add a write-ahead-log (WAL) to handle crashes. otherwise un-flushed data might be lost
  do we need direct I/O? - https://stackoverflow.com/questions/33095053/how-do-i-skip-the-filesystem-cache-when-reading-a-file-in-golang

    kind of works, need to flush wal when sst is flushed, so wal doesn't get too big

- instead of flushing immediately, replace the memtable with a new one and keep the old memtable as an immutable table in memory (we just read from it, never write) while it is being flushed.
- random links with some interesting ideas:
  https://dev.to/creativcoder/what-is-a-lsm-tree-3d75
  https://titanwolf.org/Network/Articles/Article?AID=0e39c4d6-f59b-409f-b3e4-259285d5a9b0
  https://george24601.github.io/2019/12/27/lsm.html
  https://stackoverflow.com/questions/256511/skip-list-vs-binary-search-tree/28270537

- Add GC for cached content
- Add a compaction thread/phase
- Key will be preserving functionality and speed while adding these "real-world" features
- other optimizations? optimal locking? sparse indexes?

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
