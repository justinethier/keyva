# Basic
- Proper header comments for packages, review package exports, etc
- Repeated writes to a single key will create an SST file with only one entry (see seq bench example)

# Robustness
- Thread safe LSB operations and/or use a dedicated thread with a buffered channel for other threads to send (and load??) data to the LSB. This is very important before we cut web over to the SST solution. OK for now if locking is not optimal as long as it works.
- Add a write-ahead-log (WAL) to handle crashes. otherwise un-flushed data might be lost
- Add GC for cached content
- Add a compaction thread/phase
- Key will be preserving functionality and speed while adding these "real-world" features
- other optimizations? optimal locking? sparse indexes?

# Web 
- Hook SST implementation up to web interface when ready
- Have a static web page that makes it easy to perform RUD operations. EG: post entered data to a key, or update/delete that key
- Use http.DetectContentType if content type is not supplied (EG: empty string)

# Deployment
- Prior to making any releases, consider using `cmd` and `internal` directories for project layout - https://eli.thegreenplace.net/2019/simple-go-project-layout-with-modules/
- Fix bloom filter layout, maybe remove entirely and just use the remote module how go is intended to work

# Roadmap
- Build prototype (here or elsewhere) with a b-tree
- Consider original LSM paper and newer literature
- How to make everything more robust / scalable?
