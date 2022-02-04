package sst

import (
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// Levels returns the names of any directories containing consolidated
// SST files at levels greater than level 0. This implies the data is
// organized in non-overlapping regions across files at that level.
func Levels(path string) []string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	var lvls []string
	for _, file := range files {
		matched, _ := regexp.Match(`^level-[0-9]*`, []byte(file.Name()))
		if matched && file.IsDir() {
			lvls = append(lvls, file.Name())
		}
	}

	return lvls
}

func PathForLevel(base string, level int) string {
  if level == 0 {
    return base
  }

  return fmt.Sprintf("%s/level-%d", base, level)
}

// Filenames returns names of the SST files under path
func Filenames(path string) []string {
	var sstFiles []string
	files, err := ioutil.ReadDir(path)
	if err == nil {
		for _, file := range files {
			matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
			if matched && !file.IsDir() {
				sstFiles = append(sstFiles, file.Name())
			}
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
	for l := 0; l < len(lvl); l++ {
		for i := len(lvl[l].Files) - 1; i >= 0; i-- {
			log.Println("DEBUG loading entries from file", lvl[l].Files[i].Filename)
			if lvl[l].Files[i].Filter.Test(key) {
				// Only read from disk if key is in the filter
				var entries []SstEntry

				if len(lvl[l].Files[i].Cache) == 0 {
					// No cache, read file from disk and cache entries
					entries, _ = Load(PathForLevel(path, l) + "/" + lvl[l].Files[i].Filename)
					lvl[l].Files[i].Cache = entries
				} else {
					entries = lvl[l].Files[i].Cache
				}
				lvl[l].Files[i].CachedAt = time.Now() // Update cached time

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
	}
	var rv []byte
	return rv, false
}
