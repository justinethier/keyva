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

	check := func(key string, start int, end int, found bool) {
		s, e, f := findIndex(key, index)
		if f != found {
			t.Error("Key", key, "Expected found", found, "but received", f)
			return
		}
		if s != start {
			t.Error("Key", key, "Expected starting offset", start, "but received", s)
		}
		if e != end {
			t.Error("Key", key, "Expected starting offset", start, "but received", s)
		}
	}
	check("aa", 0, 0, false)
	check("b", 0, 0, false)
	check("ccc", 0, 0, false)
	check("dc", 0, 0, false)
	check("ee", 0, 100, true)
	check("ff", 0, 100, true)
	check("gg", 0, 100, true)
	check("jj", 100, 200, true)
	check("ll", 100, 200, true)
	check("qq", 200, 300, true)
	check("ss", 200, 300, true)
	check("xx", 300, -1, true)
	check("zz", 300, -1, true)
}
