package main

import (
	//"bytes"
  "fmt"
	//"io/ioutil"
	//"log"
	//"net/http"
  "math/rand"
  "time"
	"github.com/justinethier/keyva/lsm"
)

func main() {
	tbl := lsm.New("data", 5)
  //tbl.ResetDB()

  for i := 0; i < 25; i++ {
    key := fmt.Sprintf("%d", rand.Intn(100))
    doc := fmt.Sprintf("%d", time.Now().UnixNano())
	  //set(key, "text/plain", []byte(doc))
    tbl.Set(key, []byte(doc))
  }

  // Poor man's wait
  for tbl.HaveBufferedChanges() {
    time.Sleep(1 * time.Second)
  }
}
