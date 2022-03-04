# Basic
- This is from rocks - can we do this? - "Once a memtable is full, it becomes immutable and replaced by a new memtable. A background thread will flush the content of the memtable into a SST file, after which the memtable can be destroyed."
- Proper header comments for packages, review package exports, etc

# Robustness

- random links with some interesting ideas:
  https://dev.to/creativcoder/what-is-a-lsm-tree-3d75
  https://titanwolf.org/Network/Articles/Article?AID=0e39c4d6-f59b-409f-b3e4-259285d5a9b0
  https://george24601.github.io/2019/12/27/lsm.html
  https://stackoverflow.com/questions/256511/skip-list-vs-binary-search-tree/28270537

- Expand testing to better handle below cases
  - consider chaos monkey that adds random keys over fast/slow time intervals
- Add GC for cached content
- other optimizations? optimal locking? sparse indexes?

# Web 
- Have a static web page that makes it easy to perform CRUD operations. EG: post entered data to a key, or update/delete that key
- Use http.DetectContentType if content type is not supplied (EG: empty string)
- API function to see all keys??
- Store content-type as separate key (key/content-type ??)

# Deployment
- Prior to making any releases, consider using `internal` directory for project layout - https://eli.thegreenplace.net/2019/simple-go-project-layout-with-modules/
- Fix bloom filter layout, maybe remove entirely and just use the remote module how go is intended to work

# Datasets

Need to build some examples using datasets on the Internet.

Some ideas:

- Take snapshots of IOT data. Ideally would be a time series oriented DB
- Snapshots of Asmodee data. Could be time series, though some data might not be (EG: dump of all players, etc)
- Finance applications? historic prices? could be time series oriented
- What else?

