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

	check := func(key string, start int, end int, err bool) {
		s, e, er := findIndex("ee", index)
		if er != err {
			t.Error("Expected error of", err, "but received", er)
			return
		}
		if s != start {
			t.Error("Expected starting offset", start, "but received", s)
		}
		if e != end {
			t.Error("Expected starting offset", start, "but received", s)
		}
	}
	check("ee", 0, 0, false) // TODO: fix this
}
