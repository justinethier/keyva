package lsm

import (
	"testing"
	"github.com/justinethier/keyva/util"
)

func TestSstCompact(t *testing.T) {
	compactSstFiles("testdb", "newsst")
 if !util.DeepCompare("newsst", "testdb/compacted.sst") {
		t.Error("Compacted SST file does not contain expected contents", "newsst")
 }
}
