package main

import (
    "log"
    "fmt"
    "io/ioutil"
    "regexp"
    "strconv"
)

func nextSstFilename(path string) string {
  files, err := ioutil.ReadDir(path)
  if err != nil {
      log.Fatal(err)
  }

  var sstFiles []string
  for _, file := range files {
    matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
    if matched && !file.IsDir() {
      //fmt.Println(file.Name(), file.IsDir())
      sstFiles = append(sstFiles, file.Name())
    }
  }

  if len(sstFiles) > 0 {
    var latest = sstFiles[len(sstFiles)-1][20:24]
    n, _ := strconv.Atoi(latest)
    return fmt.Sprintf("sorted-string-table-%04d.json", n + 1)
  }

  return "sorted-string-table-0000.json"
}

func main() {
  fmt.Println(nextSstFilename("."))
}

