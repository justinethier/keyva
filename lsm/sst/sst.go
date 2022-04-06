package sst

import (
	//"log"
	//"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// findIndex finds the index that may contain the given key. That is, the key is between the starting point of
// that index and the starting point of the next index. EG: Key "bbb" is between index A at "aaa" and index B
// at "bbb". So we return data for index A.
func findIndex(key string, tbl []SstIndex) (*SstIndex, *SstIndex, int, bool) {
	var left = 0
	var right = len(tbl) - 1

	for left <= right {
		mid := left + int((right-left)/2)
		//log.Println("DEBUG findIndex", key, left, right, mid, tbl[mid])

		// Have we found the appropriate index?
		if tbl[mid].Key == key {
			var next *SstIndex
			if mid+1 < len(tbl) {
				next = &tbl[mid+1]
			}
			return &tbl[mid], next, mid, true
		} else if tbl[mid].Key < key {
			if (mid+1) < len(tbl) && tbl[mid+1].Key > key { // Key between mid and mid+1
				return &tbl[mid], &tbl[mid+1], mid, true
			} else if (mid + 1) == len(tbl) { // Key is in last index
				return &tbl[mid], nil, mid, true
			}
		} else if mid == 0 && tbl[mid].Key > key {
			return nil, nil, -1, false // There is no index that contains key
		}

		if tbl[mid].Key > key {
			right = mid - 1 // Key would be found before this entry
		} else {
			left = mid + 1 // Key would be found after this entry
		}
	}

	// Don't think this is possible but if we got here there is no matching index
	return nil, nil, -1, false
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

// TODO:
//        // Find the appropriate SstIndex data block.
//        if thisIndex, nextIndex, idx, found := findIndex(key, lvl[l].Files[i].Index); found {
//          // from there, read that data block from disk into memory (seek to offset, read to next offset). need a function to do that, and test
//          // once we have the data block contents, cache it in memory
//          // then assign "entries" to that data and use it below to findValue
//        }

//				if len(lvl[l].Files[i].Cache) == 0 {
//					// No cache, read file from disk and cache entries
//					log.Println("DEBUG loading and caching entries from file", lvl[l].Files[i].Filename)
//					entries, _ = Load(PathForLevel(path, l) + "/" + lvl[l].Files[i].Filename)
//					lvl[l].Files[i].Cache = entries
//				} else {
//					entries = lvl[l].Files[i].Cache
//				}
//				lvl[l].Files[i].CachedAt = time.Now() // Update cached time

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
