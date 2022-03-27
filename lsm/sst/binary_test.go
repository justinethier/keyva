package sst

import (
	"bytes"
	"log"
	"os"
	"strconv"
	"testing"
)

func TestBinary(t *testing.T) {
	var keys []string
	m := make(map[string]SstEntry)

	for i := 0; i < 10; i++ {
		key := "Key " + strconv.Itoa(i)
		keys = append(keys, key)
		m[key] = SstEntry{key, []byte("Test Value " + key), false}
	}

	writeSst("mytest", keys, m, uint64(10), 3)
}

func TestBinaryRead(t *testing.T) {
	f, err := os.Open("mytest.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	findex, err := os.Open("mytest.index")
	if err != nil {
		log.Fatal(err)
	}
	defer findex.Close()

	// validate index file contents
	index, header, err := readIndex(findex)
	if len(index) != 4 {
		t.Error("Expected index of length 4 but received one of length", len(index))
	}
	if header.Seq != uint64(10) {
		t.Error("Unexpected sequence number", header.Seq)
	}

	// Validate contents of index
	for i, e := range index {
		key := "Key " + strconv.Itoa(i*3)
		if key != e.Key {
			t.Error("Expected index key", key, "but received", e.Key)
		}
		offset := i * 90
		if offset != e.offset {
			t.Error("Expected index offset", offset, "but received", e.offset)
		}
	}

	// Validate contents of SST
	lis := readEntries(f)
	log.Println("read entries", len(lis))
	for i, e := range lis {
		key := "Key " + strconv.Itoa(i)
		if key != e.Key {
			t.Error("Expected key", key, "but received", e.Key)
		}
		if bytes.Compare(e.Value, []byte("Test Value " + key)) != 0 {
			t.Error("Unexpected data", e.Value)
		}
		if e.Deleted != false {
			t.Error("Unexpected deleted flag", e.Deleted)
		}
	}

	files := []string{"mytest.bin"}
	tmpdir, _ := Compact(files, ".", 40, 2, false)
	log.Println("Compacted to", tmpdir)
}
