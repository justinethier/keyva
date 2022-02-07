package lsm

import (
	"bytes"
	"github.com/justinethier/keyva/lsm/sst"
	"github.com/justinethier/keyva/lsm/wal"
	"math/rand"
	//"os"
	"strconv"
	"testing"
)

var tbl *LsmTree

func init() {
	//os.Remove("wal.log")
	tbl = New("testdb", 5000)
}

func BenchmarkSstKeyValueSet(b *testing.B) {
	//tbl = New("testdb", 5000)
	tbl.ResetDB()

	for i := 0; i < b.N; i++ {
		token := make([]byte, 8)
		rand.Read(token)
		j := rand.Intn(b.N)
		tbl.Set(strconv.Itoa(j), token)
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
	//os.Remove("wal.log")
	w, _ := wal.New("testdb")
	w.Append("a", []byte("1"), false)
	w.Append("b", []byte("2"), false)
	w.Append("c", []byte("3"), false)
	w.Append("d", []byte("4"), false)
	w.Append("e", []byte("5"), false)
	w.Append("f", []byte("6"), false)
	w.Append("g", []byte("7"), false)
	w.Close()

	var tbl = New("testdb", 25)
	tbl.Set("h", []byte("8"))
	if v, found := tbl.Get("a"); found {
		if bytes.Compare(v, []byte("1")) != 0 {
			t.Error("Unexpected value", v, "for key", "a")
		}
	} else {
		t.Error("Value not found for key a")
	}
}

// TODO: second WAL test case, setup wal and sst files, test
// only loading part of the data from WAL and using rest from
// sst files

func TestSstInternals(t *testing.T) {
	var tbl = New("testdb", 25)

	tbl.ResetDB()

	tbl.Set("test value", []byte("1"))
	val, ok := tbl.findLatestBufferEntryValue("test value")

	if !ok || bytes.Compare(val.Value, []byte("1")) != 0 {
		t.Error("Unexpected test value", val, ok)
	}
}

func TestSstKeyValue(t *testing.T) {
	var N = 100
	var tbl = New("testdb", 25)

	tbl.ResetDB()

	for i := N - 1; i >= 0; i-- {
		// encode predictable value for i
		tbl.Set(strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}

	tbl.Delete(strconv.Itoa(100))
	//tbl.Flush()

	// verify i contains expected value
	for i := 0; i < N; i++ {
		if v, found := tbl.Get(strconv.Itoa(i)); found {
			if bytes.Compare(v, []byte(strconv.Itoa(i))) != 0 {
				t.Error("Unexpected value", v, "for key", i)
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
			t.Error("Unexpected value", val, "for deleted key", i)
		}
	}

	// add a key back
	tbl.Set("abcd", []byte("test"))

	// verify that key exists now
	if val, found := tbl.Get("abcd"); found {
		if string(val) != "test" {
			t.Error("Unexpected value", val, "for key", "abcd")
		}
	} else {
		t.Error("Value not found for key", "abcd")
	}

	tbl.ResetDB()
}

func TestSstKeyValueWithMerge(t *testing.T) {
	var N = 100
	var tbl = New("testdb", 25)

	tbl.ResetDB()

	for i := N - 1; i >= 0; i-- {
		// encode predictable value for i
		tbl.Set(strconv.Itoa(i), []byte(strconv.Itoa(i)))
	}

	tbl.Delete(strconv.Itoa(100))
	//tbl.Flush()

levels := sst.Levels("testdb")
if len(levels) != 0 {
	t.Error("Found SST levels prior to merge", levels)
}

// Explicitly merge L0 to L1
tbl.Merge(0)

levels = sst.Levels("testdb")
if len(levels) != 1 {
	t.Error("Did not find SST levels after merge", levels)
}

	// verify i contains expected value
	for i := 0; i < N; i++ {
		if v, found := tbl.Get(strconv.Itoa(i)); found {
			if bytes.Compare(v, []byte(strconv.Itoa(i))) != 0 {
				t.Error("Unexpected value", v, "for key", i)
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
			t.Error("Unexpected value", val, "for deleted key", i)
		}
	}

	// add a key back
	tbl.Set("abcd", []byte("test"))

	// verify that key exists now
	if val, found := tbl.Get("abcd"); found {
		if string(val) != "test" {
			t.Error("Unexpected value", val, "for key", "abcd")
		}
	} else {
		t.Error("Value not found for key", "abcd")
	}

	tbl.ResetDB()
}
