package sst

import (
	"github.com/justinethier/keyva/util"
	"testing"
)

func TestSstCompact(t *testing.T) {
	var files []string

	files = append(files, "./test-data/sorted-string-table-0000.json")
	files = append(files, "./test-data/sorted-string-table-0001.json")
	files = append(files, "./test-data/sorted-string-table-0002.json")
	newdir, _ := Compact(files, "test-data", 100, 10, false)
	if !util.DeepCompare(newdir+"/sorted-string-table-0000.json", "test-data/compacted.sst") {
		t.Error("Compacted SST file does not contain expected contents", "newsst")
	}
}
