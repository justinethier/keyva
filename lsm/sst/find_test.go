package sst

import (
	"testing"
)

func TestFindIndex(t *testing.T) {
	e := SstIndex{"ee", 0}
	j := SstIndex{"jj", 100}
	m := SstIndex{"pp", 200}
	u := SstIndex{"uu", 300}
	index := []SstIndex{e, j, m, u}

	check := func(key string, start int, end int, idx int, found bool) {
		iKey, iNext, i, f := findIndex(key, index)
		var s, e int

   if iKey != nil {
     s = iKey.offset
   }
   if iNext != nil {
     e = iNext.offset
   }

		if f != found {
			t.Error("Key", key, "Expected found", found, "but received", f)
			return
		}
		if i != idx {
			t.Error("Key", key, "Expected index", idx, "but received", i)
			return
		}
		if s != start {
			t.Error("Key", key, "Expected starting offset", start, "but received", s)
		}
		if e != end {
			t.Error("Key", key, "Expected ending offset", end, "but received", e)
		}
	}
	check("aa", 0, 0, -1, false)
	check("b", 0, 0, -1, false)
	check("ccc", 0, 0, -1, false)
	check("dc", 0, 0, -1, false)
	check("ee", 0, 100, 0, true)
	check("ff", 0, 100, 0, true)
	check("gg", 0, 100, 0, true)
	check("jj", 100, 200, 1, true)
	check("ll", 100, 200, 1, true)
	check("qq", 200, 300, 2, true)
	check("ss", 200, 300, 2, true)
	check("xx", 300, 0, 3, true)
	check("zz", 300, 0, 3, true)
}
