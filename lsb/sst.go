// TODO: not thread safe!
package lsb

import (
  "bufio"
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
//  "math"
  "os"
  "regexp"
  "sort"
  "strconv"
  "keyva/bloom"
  "time"
)

// TODO: remove Sst from all of these names once SST is its own package
type SstBuf struct {
  Path string
  Buffer []SstEntry
  BufferSize int
  MaxBufferLength int
  filter *bloom.Filter
  files []SstFile
}

type SstFile struct {
  filename string
  filter *bloom.Filter
  cache []SstEntry // cached file contents
  cachedAt time.Time // timestamp when cache was last accessed
// may convert to seconds (best way to compare???) using -
//now := time.Now()      // current local time
//sec := now.Unix()      // number of seconds since January 1, 1970 UTC
  // TODO: longer-term, we will time out the cache and have a GC that
  //       empties it if it has not been accessed for THRESHOLD
  //       will also want a default threshold and a way to change it.
  //       maybe it will be a member of SstBuf
}

type SstEntry struct {
  Key string
  Value Value
  Deleted bool
}

func NewSstBuf(path string, bufSize int) *SstBuf {
  buf := make([]SstEntry, bufSize)
  f := bloom.New(bufSize, 200)
  var files []SstFile
  sstbuf := SstBuf{path, buf, 0, bufSize, f, files}
  sstbuf.LoadFilters() // Read all SST files on disk and generate bloom filters
  return &sstbuf
}

func (s *SstBuf) Set(k string, value Value) {
  s.set(k, value, false)
}

func (s *SstBuf) Delete(k string) {
  var val Value
  s.set(k, val, true)
}

func (s *SstBuf) set(k string, value Value, deleted bool) {
  entry := SstEntry{k, value, deleted}
  i := s.BufferSize
  s.Buffer[i] = entry
  s.BufferSize++

  s.filter.Add(k)

  if (s.BufferSize < s.MaxBufferLength) {
    // Buffer is not full yet, we're good
    return
  }

  s.Flush()
}

// Read all sst files from disk and load a bloom filter for each one into memory
func (s *SstBuf) LoadFilters() {
  sstFilenames := s.GetSstFilenames()
  for _, filename := range sstFilenames {
    fmt.Println("DEBUG: loading bloom filter from file", filename)
    entries := s.LoadEntriesFromSstFile(filename)
    filter := bloom.New(s.MaxBufferLength, 200)
    for _, entry := range entries {
      filter.Add(entry.Key)
    }
    var sstfile = SstFile{filename, filter, []SstEntry{}, time.Now()}
    s.files = append(s.files, sstfile)
  }
}

func (s *SstBuf) Flush() {
  if s.BufferSize == 0 {
    return
  }

  // Remove duplicate entries
  m := make(map[string]SstEntry)
  for i := 0; i < s.BufferSize; i++ {
    e := s.Buffer[i]
    m[e.Key] = e
  }

  // sort list of keys and setup bloom filter
  filter := bloom.New(s.BufferSize, 200)
  keys := make([]string, 0, len(m))
  for k := range m {
    filter.Add(k)
    keys = append(keys, k)
  }
  sort.Strings(keys)

  // Flush buffer to disk
  var filename = s.NextSstFilename()
  CreateSstFile(filename, keys, m)

  // Add information to memory
  var sstfile = SstFile{filename, filter, []SstEntry{}, time.Now()}
  s.files = append(s.files, sstfile)

  // Clear buffer
  s.BufferSize = 0
}

func check(e error) {
  if e != nil {
    panic(e)
  }
}

func CreateSstFile(filename string, keys []string, m map[string]SstEntry) {
  f, err := os.Create(filename)
  check(err)

  defer f.Close()

  for _, k := range keys {
    b, err := json.Marshal(m[k])
    check(err)

    _, err = f.Write(b)
    check(err)

    _, err = f.Write([]byte("\n"))
    check(err)
  }
}

func (s *SstBuf) NextSstFilename() string {
  files, err := ioutil.ReadDir(s.Path)
  if err != nil {
      log.Fatal(err)
  }

  var sstFiles []string
  for _, file := range files {
    matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
    if matched && !file.IsDir() {
      //fmt.Println(file.Name(), file.IsDir())
      sstFiles = append(sstFiles, file.Name())
    }
  }

  if len(sstFiles) > 0 {
    var latest = sstFiles[len(sstFiles)-1][20:24]
    n, _ := strconv.Atoi(latest)
    return fmt.Sprintf("sorted-string-table-%04d.json", n + 1)
  }

  return "sorted-string-table-0000.json"
}

func (s *SstBuf) GetSstFilenames() []string {
  files, err := ioutil.ReadDir(s.Path)
  if err != nil {
      log.Fatal(err)
  }

  var sstFiles []string
  for _, file := range files {
    matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
    if matched && !file.IsDir() {
      sstFiles = append(sstFiles, file.Name())
    }
  }

  return sstFiles
}

func (s *SstBuf) findLatestBufferEntryValue(key string) (SstEntry, bool){
  var empty SstEntry

  // Early exit if we have never seen this key
  if !s.filter.Test(key) {
    return empty, false
  }

  for i := 0; i < s.BufferSize; i++ {
    entry := s.Buffer[i]
    if entry.Key == key {
      return entry, true
    }
  }

  return empty, false
}

func (s *SstBuf) LoadEntriesFromSstFile(filename string) []SstEntry{
  var buf []SstEntry

    f, err := os.Open(filename)
    if err != nil {
      return buf
    }

    defer f.Close()

    r := bufio.NewReader(f)
    str, e := Readln(r)
    for e == nil {
        var data SstEntry
        err = json.Unmarshal([]byte(str), &data)
        //fmt.Println(data)
        buf = append(buf, data)
        str, e = Readln(r)
    }

  return buf
}

func (s *SstBuf) FindEntryValue(key string, entries []SstEntry) (SstEntry, bool) {
  var entry SstEntry
  var left = 0
  var right = len(entries) - 1

  for left <= right {
    mid := left + int((right - left) / 2)
    //fmt.Println("DEBUG FEV", key, left, right, mid, entries[mid])

    // Found the key
    if entries[mid].Key == key {
      return entries[mid], true
    }

    if entries[mid].Key > key {
      right = mid - 1 // Key would be found before this entry
    } else {
      left = mid + 1 // Key would be found after this entry
    }
  }

  return entry, false
}

func (s *SstBuf) Get(k string) (Value, bool) {
  // Check in-memory buffer
  if latestBufEntry, ok := s.findLatestBufferEntryValue(k); ok {
    if latestBufEntry.Deleted {
      return latestBufEntry.Value, false
    } else {
      return latestBufEntry.Value, true
    }
  }

  // Not found, search the sst files
  // Search in reverse order, newest file to oldest
  for i := len(s.files) - 1; i >= 0; i-- {
    //fmt.Println("DEBUG loading entries from file", sstFilenames[i])
    if s.files[i].filter.Test(k) {
      // Only read from disk if key is in the filter
      var entries []SstEntry

      if len(s.files[i].cache) == 0 {
        // No cache, read files from disk and cache them
        entries = s.LoadEntriesFromSstFile(s.files[i].filename)
        s.files[i].cache = entries
      } else {
        entries = s.files[i].cache
      }
      s.files[i].cachedAt = time.Now() // Update cached time

      // Search for key in the file's entries
      if entry, found := s.FindEntryValue(k, entries); found {
        if entry.Deleted {
          return entry.Value, false
        } else {
          return entry.Value, true
        }
      }
    }
  }

  // Key not found
  var val Value
  return val, false
}

func (s *SstBuf) ResetDB() {
  s.files = make([]SstFile, 0) // Clear from memory
  sstFilenames := s.GetSstFilenames()
  for _, filename := range sstFilenames {
    os.Remove(filename) // ... and remove from disk
  }
}

