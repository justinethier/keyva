package util

import (
  "bufio"
  "fmt"
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

// debug controls whether debug information is logged to stdout
var debug bool = false

// Trace writes the given parameters to stdout if debug output is enabled
func Trace(str ...interface{}) {
 if debug {
   fmt.Println(str...)
   return
 }
}
