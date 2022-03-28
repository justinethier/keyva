package sst

import (
	"github.com/justinethier/keyva/util"
	"testing"
)

func TestSstCompact(t *testing.T) {
	var files []string

	files = append(files, "./test-data/sst-0000.bin")
	files = append(files, "./test-data/sst-0001.bin")
	//files = append(files, "./test-data/sst-0002.bin")
	newdir, _ := Compact(files, "test-data", 100, 10, false)
	if !util.DeepCompare(newdir+"/sst-0000.bin", "test-data/compacted.bin") {
		t.Error("Compacted SST file does not contain expected contents", "newsst")
	}
}
