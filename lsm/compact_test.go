package lsm

import (
	"testing"
	"github.com/justinethier/keyva/util"
)

func TestSstCompact(t *testing.T) {
	compactSstFiles("test-data", "newsst")
 if !util.DeepCompare("newsst", "test-data/compacted.sst") {
		t.Error("Compacted SST file does not contain expected contents", "newsst")
 }
}
