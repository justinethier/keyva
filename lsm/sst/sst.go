package sst

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/justinethier/keyva/util"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"
)

// Create creates a new SST file from given data
func Create(filename string, keys []string, m map[string]SstEntry, seqNum uint64) {
	f, err := os.Create(filename)
	check(err)

	defer f.Close()

	header := SstFileHeader{seqNum}
	b, err := json.Marshal(header)
	check(err)
	_, err = f.Write(b)
	check(err)
	_, err = f.Write([]byte("\n"))
	check(err)

	for _, k := range keys {
		b, err := json.Marshal(m[k])
		check(err)

		_, err = f.Write(b)
		check(err)

		_, err = f.Write([]byte("\n"))
		check(err)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// Filenames returns names of the SST files under path
func Filenames(path string) []string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	var sstFiles []string
	for _, file := range files {
		matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
		if matched && !file.IsDir() {
			sstFiles = append(sstFiles, file.Name())
		}
	}

	return sstFiles
}

// NextFilename returns the name of the next SST file in given directory
func NextFilename(path string) string {
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
		return fmt.Sprintf("sorted-string-table-%04d.json", n+1)
	}

	return "sorted-string-table-0000.json"
}

//
func Load(filename string, path string) ([]SstEntry, SstFileHeader) {
	var buf []SstEntry
	var header SstFileHeader

	f, err := os.Open(path + "/" + filename)
	if err != nil {
		return buf, header
	}

	defer f.Close()

	r := bufio.NewReader(f)
	str, e := util.Readln(r)
	check(e)
	err = json.Unmarshal([]byte(str), &header)
	check(e)
	//fmt.Println("DEBUG SST header", header)

	str, e = util.Readln(r)
	for e == nil {
		var data SstEntry
		err = json.Unmarshal([]byte(str), &data)
		check(err)
		//fmt.Println(data)
		buf = append(buf, data)
		str, e = util.Readln(r)
	}

	return buf, header
}

// find searches the given entries for key and returns the corresponding value if found.
func findValue(key string, entries []SstEntry) (SstEntry, bool) {
	var entry SstEntry
	var left = 0
	var right = len(entries) - 1

	for left <= right {
		mid := left + int((right-left)/2)
		//log.Println("DEBUG FEV", key, left, right, mid, entries[mid])

		// Found the key
		if entries[mid].Key == key {
			return entries[mid], true
		}

		if entries[mid].Key > key {
			right = mid - 1 // Key would be found before this entry
		} else {
			left = mid + 1 // Key would be found after this entry
		}
	}

	return entry, false
}

func Find(key string, lvl []SstLevel, path string) ([]byte, bool) {
	// Search in reverse order, newest file to oldest
	for i := len(lvl[0].Files) - 1; i >= 0; i-- {
		//log.Println("DEBUG loading entries from file", lvl[0].Files[i].Filename)
		if lvl[0].Files[i].Filter.Test(key) {
			// Only read from disk if key is in the filter
			var entries []SstEntry

			if len(lvl[0].Files[i].Cache) == 0 {
				// No cache, read file from disk and cache entries
				entries, _ = Load(lvl[0].Files[i].Filename, path)
				lvl[0].Files[i].Cache = entries
			} else {
				entries = lvl[0].Files[i].Cache
			}
			lvl[0].Files[i].CachedAt = time.Now() // Update cached time

			// Search for key in the file's entries
			if entry, found := findValue(key, entries); found {
				if entry.Deleted {
					return entry.Value, false
				} else {
					return entry.Value, true
				}
			}
		}
	}
  var rv []byte
  return rv, false
}
