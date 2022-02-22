package main

import (
	//"bytes"
	"fmt"
	//"io/ioutil"
	//"log"
	//"net/http"
	"github.com/justinethier/keyva/lsm"
	"math/rand"
	"time"
)

TODO: modify to generate 1 MB, then 100 MB+ of data for the DB
TODO: increase buffer size below
TODO: configure merge settings

func main() {
	tbl := lsm.New("data", 5)
	//tbl.ResetDB()

	for i := 0; i < 125; i++ {
		key := fmt.Sprintf("%d", rand.Intn(100))
		doc := fmt.Sprintf("%d", time.Now().UnixNano())
		//set(key, "text/plain", []byte(doc))
		tbl.Set(key, []byte(doc))
	}
}
