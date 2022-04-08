package main

import (
	"github.com/justinethier/keyva/lsm/sst"
	"os"
)

func main() {
	//util.OpenSyslog()

  // TODO: need args to:
  //
  // print index
  // print bin
  // usage
  filename := os.Args[1]
  sst.DumpBin(filename)
}
