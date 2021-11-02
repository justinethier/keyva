# Basic
- Extract sst out into its own package. we can have a log package for the old log implementation
- other optimizations? sparse indexes?
- Reformat everything using `gofmt -w file`. Add a makefile directive to reformat everything
- Add makefile directive to generate go documentation
- Review project layout - https://eli.thegreenplace.net/2019/simple-go-project-layout-with-modules/
- Proper header comments for packages, review package exports, etc
- Fix bloom filter layout, maybe remove entirely and just use the remote module how go is intended to work

# Robustness
- Thread safe LSB operations and/or use a dedicated thread with a buffered channel for other threads to send (and load??) data to the LSB. This is very important before we cut web over to the SST solution.
- Add a write-ahead-log (WAL) to handle crashes. otherwise un-flushed data might be lost
- Add GC for cached content
- Add a compaction thread/phase
- Key will be preserving functionality and speed while adding these "real-world" features

# Web 
- Hook LSB implementation up to web interface when ready
- Have a static web page that makes it easy to perform RUD operations. EG: post entered data to a key, or update/delete that key
- Use http.DetectContentType if content type is not supplied (EG: empty string)
