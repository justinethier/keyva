- New project name - keva. Or maybe "keyva"
- Extract sst out into its own package. Also have a util package as well, and move util.go there. we can have a log package for the old log implementation
- other optimizations? sparse indexes?
- Consider a hybrid in-memory map/cache and disked-back LSB for both speed an reliability
- Reformat everything using `gofmt -w file`. Add a makefile directive to reformat everything

# Robustness
- Thread save LSB operations and/or use a dedicated thread with a buffered channel for other threads to send (and load??) data to the LSB
- Add a write-ahead-log (WAL) to handle crashes. otherwise un-flushed data might be lost

# Web 
- Hook LSB implementation up to web interface when ready
- Have a static web page that makes it easy to perform RUD operations. EG: post entered data to a key, or update/delete that key
- Use http.DetectContentType if content type is not supplied (EG: empty string)
