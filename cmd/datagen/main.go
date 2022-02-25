package main

import (
	//"bytes"
	"fmt"
	//"io/ioutil"
	//"log"
	//"net/http"
	"github.com/justinethier/keyva/lsm"
	"github.com/justinethier/keyva/util"
	//"math/rand"
	//"time"
)

//TODO: modify to generate 1 MB, then 100 MB+ of data for the DB
//TODO: assess merge performance by using repl

func main() {
	util.OpenSyslog()
	tbl := lsm.New("data", 1024)
	// May need to merge separately; data will fill faster than merge job can keep up
	tbl.SetMergeSettings(lsm.MergeSettings{Immediate: true, MaxLevels: 2, NumberOfSstFiles: 10})
	tbl.ResetDB()

	for i := 0; i < 1000 * 150; i++ {
    //TODO: merge every N? Try to do multiple levels here?
    //TODO: would be nice if we had an *immediate* mode to simulate what would 
    //      happen over time with a real server

		key := fmt.Sprintf("%d", i); //rand.Intn(100))
		doc := fmt.Sprintf("%d", i); //time.Now().UnixNano())
		//set(key, "text/plain", []byte(doc))
		tbl.Set(key, []byte(doc))
	}
	
	// Explicitly merge out of level 0 after creating data.
  //lsm.Merge(0)
}
