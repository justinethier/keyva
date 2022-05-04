package main

import (
	"github.com/justinethier/keyva/lsm/sst"
	"os"
	"strings"
)

func main() {
	//util.OpenSyslog()

	// TODO: need args to:
	//
	// print index
	// print bin
	// usage
	filename := os.Args[1]
	if strings.HasSuffix(filename, ".index") {
		sst.DumpIndex(filename)
	} else {
		sst.DumpBin(filename)
	}
}
