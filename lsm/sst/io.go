package sst

import (
	"bufio"
	"encoding/json"
	"github.com/justinethier/keyva/util"
	"log"
	"os"
)

// Create creates a new SST file from given data
func Create(filename string, keys []string, m map[string]SstEntry, seqNum uint64) {
	f, err := os.Create(filename)
	check(err)

	defer f.Close()

	writeSstFileHeader(f, seqNum)

	for _, k := range keys {
    var e SstEntry
    e = m[k]
    writeSstEntry(f, &e, false)
	}
}

// Open opens the given file and returns the SST file header along with a buffered reader.
func Open(filename string) (*os.File, *bufio.Reader, SstFileHeader, error) {
  var header SstFileHeader
  var reader *bufio.Reader
  f, err := os.Open(filename)
  if err != nil {
    return f, reader, header, err
  }

	reader = bufio.NewReader(f)
	str, e := util.Readln(reader)
	check(e)
	err = json.Unmarshal([]byte(str), &header)
	check(e)
	//fmt.Println("DEBUG SST header", header)
  return f, reader, header, nil
}

// Readln reads the next entry from an SST file using the given buffered reader.
func Readln(reader *bufio.Reader) (SstEntry, error) {
  var entry SstEntry
  str, err := util.Readln(reader)
  if err != nil {
    return entry, err
  }
  err = json.Unmarshal([]byte(str), &entry)
  check(err)
  return entry, err
}

//
func Load(filename string) ([]SstEntry, SstFileHeader) {
	var buf []SstEntry
	var header SstFileHeader

	f, err := os.Open(filename)
	if err != nil {
		log.Println("Load error", err)
		return buf, header
	}

	defer f.Close()

	r := bufio.NewReader(f)
	str, e := util.Readln(r)
	check(e)
	err = json.Unmarshal([]byte(str), &header)
	check(e)
	//fmt.Println("DEBUG SST header", header)

	str, e = util.Readln(r)
	for e == nil {
		var data SstEntry
		err = json.Unmarshal([]byte(str), &data)
		check(err)
		//fmt.Println(data)
		buf = append(buf, data)
		str, e = util.Readln(r)
	}

	return buf, header
}

func writeSstFileHeader(f *os.File, seqNum uint64) {
	header := SstFileHeader{seqNum}
	b, err := json.Marshal(header)
	check(err)
	_, err = f.Write(b)
	check(err)
	_, err = f.Write([]byte("\n"))
	check(err)
}

func writeSstEntry(f *os.File, e *SstEntry, removeDeleted bool) {
	if e.Deleted && removeDeleted {
		return
	}

	b, err := json.Marshal(&e)
	check(err)

	_, err = f.Write(b)
	check(err)

	_, err = f.Write([]byte("\n"))
	check(err)
}
