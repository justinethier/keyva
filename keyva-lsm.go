// TODO: some interesting ideas from:
//
// https://dgraph.io/docs/badger/get-started/
// - allow specifying a duration for a key, so it will be GC'd after time is up
// - some form of persistence would be nice, so store can be restored if the service is restarted
//   - ultimately this also ties in to having a more efficient backing store other than maps
//   - Consider using LSM, see: https://learndb.net/key-value-store/filesystem/
//     roadmap would be - benchmarks, unoptimized LSM, optimzations - bloom filters, etc. 
//     critical to have benchmarks to assess optimization effectiveness
// - should setup test programs, benchmarks, and a chaos monkey
// - At some point, separate the backing key/value store from the web interface. KV store is a GO library whereas web is potentially a library, and a front-end program
// - other ideas??
//

package main

import (
  "github.com/justinethier/keyva/lsm"
  "net/http"
  "fmt"
  "log"
  "os"
  //"sort"
)

func ArgServer(w http.ResponseWriter, req *http.Request) {
  fmt.Fprintln(w, os.Args)
}

func main() {
  mux := http.NewServeMux()
  m := lsm.New(".", 50) // TODO: optionally, make these parameters configurable
                        // TODO: use a larger default (5000?). This is small for testing purposes

  // Background on http handlers -
  // https://stackoverflow.com/questions/6564558/wildcards-in-the-pattern-for-http-handlefunc
  // https://www.honeybadger.io/blog/go-web-services/
  mux.Handle("/api/args", http.HandlerFunc(ArgServer))
//  mux.HandleFunc("/api/stats", func(w http.ResponseWriter, req *http.Request) {
//    (*m).Lock.RLock()
//    fmt.Fprintln(w, "Number of key/value pairs = ", len(m.Data))
//    fmt.Fprintln(w, "Keys:")
//    keys := make([]string, len(m.Data))
//    i := 0
//    for k := range m.Data {
//      keys[i] = k
//      i++
//      //fmt.Fprintln(w, k)
//    }
//    sort.Strings(keys)
//    for _, k := range keys {
//      fmt.Fprintln(w, k)
//    }
//    (*m).Lock.RUnlock()
//  })
  // mux.Handle("/seq/", s)
  mux.HandleFunc("/seq/", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case "GET":
			result := m.Increment(req.URL.Path)
			fmt.Fprintln(w, result)
		case "DELETE":
			m.Delete(req.URL.Path)
			fmt.Fprintln(w, "Deleted sequence")
		}
  })
  mux.Handle("/kv/", m)

  // TODO: allow optionally running an HTTPS server based on command-line flag(s):
  // https://medium.com/rungo/secure-https-servers-in-go-a783008b36da
  //
  // TODO: longer-term ties into potentially having authentication and user-based logins / permissions

  log.Fatal(http.ListenAndServe(":8080", mux))
}
