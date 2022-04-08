package main

import (
	"github.com/justinethier/keyva/lsm/sst"
	"os"
)

//TODO: modify to generate 1 MB, then 100 MB+ of data for the DB
//TODO: assess merge performance by using repl

func main() {
	//util.OpenSyslog()
	filename := os.Args[1]
  sst.DumpBin(filename)
}
