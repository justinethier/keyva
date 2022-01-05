package util

import (
  "bufio"
  "bytes"
  //"fmt"
  "io"
  "log"
  "log/syslog"
  "os"
)

// Readln returns a single line (without the ending \n)
// from the input buffered reader.
// An error is returned iff there is an error with the
// buffered reader.
//
// From: https://stackoverflow.com/a/12206584
func Readln(r *bufio.Reader) (string, error) {
  var (isPrefix bool = true
       err error = nil
       line, ln []byte
      )
  for isPrefix && err == nil {
      line, isPrefix, err = r.ReadLine()
      ln = append(ln, line...)
  }
  return string(ln),err
}

// Use log and OpenSyslog instead...
// // debug controls whether debug information is logged to stdout
// var debug bool = false
// 
// // Trace writes the given parameters to stdout if debug output is enabled
// func Trace(str ...interface{}) {
//  if debug {
//    fmt.Println(str...)
//    return
//  }
// }

// OpenSyslog opens a writer to the local syslog and redirects log output to that writer
func OpenSyslog() {
  logwriter, e := syslog.New(syslog.LOG_NOTICE, "keyva")
  if e == nil {
    log.SetOutput(logwriter)
  }
}

// Compare two files for equality
// From: https://stackoverflow.com/a/30038571/101258
const chunkSize = 64000

func DeepCompare(file1, file2 string) bool {
    // Check file size ...

    f1, err := os.Open(file1)
    if err != nil {
        log.Fatal(err)
    }
    defer f1.Close()

    f2, err := os.Open(file2)
    if err != nil {
        log.Fatal(err)
    }
    defer f2.Close()

    for {
        b1 := make([]byte, chunkSize)
        _, err1 := f1.Read(b1)

        b2 := make([]byte, chunkSize)
        _, err2 := f2.Read(b2)

        if err1 != nil || err2 != nil {
            if err1 == io.EOF && err2 == io.EOF {
                return true
            } else if err1 == io.EOF || err2 == io.EOF {
                return false
            } else {
                log.Fatal(err1, err2)
            }
        }

        if !bytes.Equal(b1, b2) {
            return false
        }
    }
}
