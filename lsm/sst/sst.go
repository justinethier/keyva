package sst

import (
	"log"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
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
			if lvl[l].Files[i].Filter.Test(key) {
				// Only read from disk if key is in the filter
				var entries []SstEntry

				if len(lvl[l].Files[i].Cache) == 0 {
					// No cache, read file from disk and cache entries
					log.Println("DEBUG loading and caching entries from file", lvl[l].Files[i].Filename)
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
