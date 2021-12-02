package main

import (
	//"bytes"
  "fmt"
	//"io/ioutil"
	//"log"
	//"net/http"
  "time"
	"github.com/justinethier/keyva/lsm"
)

func main() {
	tbl := lsm.New(".", 5000)
  tbl.ResetDB()

  for i := 0; i < 100000; i++ {
    key := fmt.Sprintf("%d", i)
    doc := fmt.Sprintf("%d", time.Now().UnixNano())
	  //set(key, "text/plain", []byte(doc))
    tbl.Set(key, lsm.Value{[]byte(doc)})
  }
}
