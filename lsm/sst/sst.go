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

// findIndex finds the index that may contain the given key. That is, the key is between the starting point of
// that index and the starting point of the next index. EG: Key "bbb" is between index A at "aaa" and index B
// at "bbb". So we return index A.
//
// TODO: return SstIndex? Or more useful to return start/end offsets??
func findIndex(key string, tbl []SstIndex) (int, int, bool) {
	var left = 0
	var right = len(tbl) - 1

	for left <= right {
		mid := left + int((right-left)/2)
		log.Println("DEBUG findIndex", key, left, right, mid, tbl[mid])

		// Found the index?

		// TODO: possible cases here are:
		//
		// K[m] == key - use this index (key is an index, unlikely but will happen)
		// K[m] < key && K[m+1] > key - use this index (key between K[m] and K[m+1])
		// K[m] < key && (m+1 == len(K)) > key - use this index (its the last one)
		// K[m] > key && (m == 0) - a failure case, key is before first index entry

		if tbl[mid].Key == key {
			return 0, 0, true //tbl[mid], true
		}

		if tbl[mid].Key > key {
			right = mid - 1 // Key would be found before this entry
		} else {
			left = mid + 1 // Key would be found after this entry
		}
	}

	// TODO: even possible? What does this even mean?
	return 0, 0, false //entry, false
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
