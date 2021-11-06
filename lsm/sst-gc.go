package lsm

import (
	"fmt"
	"time"
)

var CacheGcTimeout = 5.0

// Remove old entries from the cache
func (tree *LsmTree) CacheGC() {
	// TODO: would it be more efficient if we lock at the segment (sstfile) level?
	tree.lock.Lock()
	defer tree.lock.Unlock()

	for _, file := range tree.files {
		if len(file.cache) > 0 &&
			time.Since(file.cachedAt).Seconds() > CacheGcTimeout {
			fmt.Println("Clear cache for file", file.filename)
			var empty []SstEntry
			file.cache = empty
		}
	}
}
