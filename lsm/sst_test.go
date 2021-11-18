package lsm

import (
  "os"
	"bytes"
	"math/rand"
	"strconv"
	"testing"
	"github.com/justinethier/keyva/lsm/wal"
)

var tbl = New(".", 5000)

func BenchmarkSstKeyValueSet(b *testing.B) {
	//tbl = New(".", 5000)
	tbl.ResetDB()

	for i := 0; i < b.N; i++ {
		token := make([]byte, 8)
		rand.Read(token)
		j := rand.Intn(b.N)
		tbl.Set(strconv.Itoa(j), Value{Data: token /*, ContentType: "test content"*/})
	}
}

func BenchmarkSstKeyValueGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := rand.Intn(b.N)
		tbl.Get(strconv.Itoa(j))
	}
}

func BenchmarkSstKeyValueDelete(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := rand.Intn(b.N)
		tbl.Delete(strconv.Itoa(j))
	}
	tbl.ResetDB()
}

// Test loading data from the WAL
func TestWal(t *testing.T) {
  os.Remove("wal.log")
  wal := wal.New(".")
  wal.Append("a", []byte("1"), false)
  wal.Append("b", []byte("2"), false)
  wal.Append("c", []byte("3"), false)
  wal.Append("d", []byte("4"), false)
  wal.Append("e", []byte("5"), false)
  wal.Append("f", []byte("6"), false)
  wal.Append("g", []byte("7"), false)
  wal.Close()

	var tbl = New(".", 25)
  if v, found := tbl.Get("a"); found {
			if bytes.Compare(v.Data, []byte("1")) != 0 {
				t.Error("Unexpected value", v.Data, "for key", "a")
			}
  } else {
    t.Error("Value not found for key a")
  }
  os.Remove("wal.log")
}

// TODO: second WAL test case, setup wal and sst files, test
// only loading part of the data from WAL and using rest from
// sst files

func TestSstInternals(t *testing.T) {
	var tbl = New(".", 25)

	tbl.ResetDB()

	tbl.Set("test value", Value{[]byte("1")})
	val, ok := tbl.findLatestBufferEntryValue("test value")

	if !ok || bytes.Compare(val.Value.Data, []byte("1")) != 0 {
		t.Error("Unexpected test value", val, ok)
	}
}

func TestSstKeyValue(t *testing.T) {
	var N = 100
	var tbl = New(".", 25)

	tbl.ResetDB()

	for i := N - 1; i >= 0; i-- {
		// encode predictable value for i
		tbl.Set(strconv.Itoa(i), Value{Data: []byte(strconv.Itoa(i)) /*, ContentType: "test content"*/ })
	}

	tbl.Delete(strconv.Itoa(100))
	//tbl.Flush()

	// verify i contains expected value
	for i := 0; i < N; i++ {
		if v, found := tbl.Get(strconv.Itoa(i)); found {
			if bytes.Compare(v.Data, []byte(strconv.Itoa(i))) != 0 {
				t.Error("Unexpected value", v.Data, "for key", i)
			}
		} else {
			t.Error("Value not found for key", i)
		}
	}

	for i := 0; i < N; i++ {
		tbl.Delete(strconv.Itoa(i))
	}

	// verify key does not exist for i
	for i := 0; i < N; i++ {
		if val, found := tbl.Get(strconv.Itoa(i)); found {
			t.Error("Unexpected value", val.Data, "for deleted key", i)
		}
	}

	// add a key back
	tbl.Set("abcd", Value{[]byte("test")})

	// verify that key exists now
	if val, found := tbl.Get("abcd"); found {
		if string(val.Data) != "test" {
			t.Error("Unexpected value", val.Data, "for key", "abcd")
		}
	} else {
		t.Error("Value not found for key", "abcd")
	}

	tbl.Flush()
	// Verify again now that key is on disk
	if val, found := tbl.Get("abcd"); found {
		if string(val.Data) != "test" {
			t.Error("Unexpected value", val.Data, "for key", "abcd")
		}
	} else {
		t.Error("Value not found for key", "abcd")
	}
	tbl.ResetDB()
}
