package lsm

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

// TODO:
// func (m *Sequence) ServeHTTP(w http.ResponseWriter, req *http.Request) {
// 	switch req.Method {
// 	case "GET":
// 		result := m.Increment(req.URL.Path)
// 		fmt.Fprintln(w, result)
// 	case "DELETE":
// 		m.Delete(req.URL.Path)
// 		fmt.Fprintln(w, "Deleted sequence")
// 	}
// }

func (m *LsmTree) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		if val, ok := m.Get(req.URL.Path); ok {
			w.Header().Set("Content-Type", val.ContentType)
			w.Write(val.Data)
		} else {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "Resource not found")
		}
	case "POST", "PUT":
		b, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Fatalln(err)
		}
		var val Value
		val.ContentType = req.Header.Get("Content-Type")
		val.Data = b //string(b)

		m.Set(req.URL.Path, val)
		fmt.Fprintln(w, "Stored value")
	case "DELETE":
		m.Delete(req.URL.Path)
		fmt.Fprintln(w, "Deleted value")
	}
}
