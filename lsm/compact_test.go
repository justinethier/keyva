package lsm

import (
	"testing"
)

func TestSstCompact(t *testing.T) {
	compactSstFiles("testdb")
	//t.Error("TODO: Implement sst compact tests")
}
