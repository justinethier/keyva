package sst

import (
	// "fmt"
	// "time"
)

// CacheGcTimeout is the amount of time in seconds to keep a cached SST file in
// memory before it is eligble for collection.
var CacheGcTimeout = 300.0

// TODO: need to divide up / port to lsm package -
//
// // CacheGC removes old entries from the cache
// func (tree *LsmTree) CacheGC() {
// 	// TODO: would it be more efficient if we lock at the segment (sstfile) level?
// 	tree.lock.Lock()
// 	defer tree.lock.Unlock()
// 
// 	var empty []SstEntry
// 	for i := 0; i < len(tree.files); i++ {
// 		fmt.Println("File", tree.files[i].filename, "cached", len(tree.files[i].cache))
// 		if len(tree.files[i].cache) > 0 &&
// 			time.Since(tree.files[i].cachedAt).Seconds() > CacheGcTimeout {
// 			fmt.Println("Clear cache for file", tree.files[i].filename)
// 			tree.files[i].cache = empty
// 		}
// 	}
// }
