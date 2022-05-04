package lsm

import (
//"fmt"
//"github.com/justinethier/keyva/lsm/sst"
//"time"
)

// CacheGcTimeout is the amount of time in seconds to keep a cached SST file in
// memory before it is eligble for collection.
var CacheGcTimeout = 300.0

// CacheGC removes old entries from the cache
//func (tree *LsmTree) CacheGC() {
//	// TODO: would it be more efficient if we lock at the segment (sstfile) level?
//	tree.lock.Lock()
//	defer tree.lock.Unlock()
//
//	var empty []sst.SstEntry
//	for i := 0; i < len(tree.sst[0].Files); i++ {
//		fmt.Println("File", tree.sst[0].Files[i].Filename, "cached", len(tree.sst[0].Files[i].Cache))
//		if len(tree.sst[0].Files[i].Cache) > 0 &&
//			time.Since(tree.sst[0].Files[i].CachedAt).Seconds() > CacheGcTimeout {
//			fmt.Println("Clear cache for file", tree.sst[0].Files[i].Filename)
//			tree.sst[0].Files[i].Cache = empty
//		}
//	}
//}
