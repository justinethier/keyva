package lsm

import (
	"errors"
	"fmt"
	"github.com/justinethier/keyva/lsm/sst"
	"log"
	"os"
	"time"
)

func (tree *LsmTree) SetMergeSettings(s MergeSettings) {
	tree.merge = s
}

// Merge takes all of the current SST files at level and merges them with the
// SST files at the next level of the LSM tree. Data is compacted during this
// process and any older key values or tombstones are permanently removed.
func (tree *LsmTree) Merge(level int) error {
	// Overall algorithm
	//
	// - find path for level, get all sst files
	// - find path for level+1, get all sst files
	// - load file contents into heap (future: stream them)
	// - write files back out to new temp directory
	// - acquire tree lock
	// - swap level+1 with new directory
	// - delete all old files
	// - clear all in-memory data for files
	// - release locks, merge is done
	// - log to syslog, consider WAL

	// TODO: if level == tree.merge.MaxLevels, then compact that level instead of merging into l+1

	highestTreeLevel := len(tree.sst) - 1

	if level > highestTreeLevel {
		desc := fmt.Sprintf("Merge cannot process level %d because the tree only has %d levels", level, highestTreeLevel)
		log.Println(desc)
		return errors.New(desc)
	} else if level > 0 && level == tree.merge.MaxLevels {
		// Cannot merge above highest level so compact it instead
		tree.Compact(level)
		return nil
	}

	lPath := sst.PathForLevel(tree.path, level)
	lNextPath := sst.PathForLevel(tree.path, level+1)

	log.Println("Debug load files from", lPath, lNextPath)

	files := sst.Filenames(lPath)
	for i, _ := range files {
		files[i] = lPath + "/" + files[i]
	}
	nextLvlFiles := sst.Filenames(lNextPath)
	for i, _ := range nextLvlFiles {
		nextLvlFiles[i] = lNextPath + "/" + nextLvlFiles[i]
	}

	files = append(files, nextLvlFiles...)
	log.Println("Files", files)

	removeDeleted := false
	if level == highestTreeLevel {
		log.Println("Merging highest level of tree", level, "deleted keys will be permanently removed")
		removeDeleted = true
	}

	tmpDir, err := sst.Compact(files, tree.path, tree.bufferSize, removeDeleted)
	log.Println("Files in", tmpDir, err)

	if !tree.merge.Immediate {
		tree.lock.Lock()
		defer tree.lock.Unlock()
	}

	for _, filename := range files {
		os.Remove(filename)
	}

	err = os.RemoveAll(lNextPath)
	if err != nil {
		log.Fatal(err)
	}

	err = os.Rename(tmpDir, lNextPath)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(tree.sst)

	// Drop and reload cache for all files from l/l+1
	// TODO: more efficient solution?
	var a, b sst.SstLevel
	tree.sst[level] = a
	tree.loadLevel(lPath, level)

	log.Println(tree.sst, len(tree.sst), level)

	if len(tree.sst) <= (level + 1) {
		log.Println("Add new level", b, "to tree")
		tree.sst = append(tree.sst, b)
	} else {
		log.Println("Update tree at level", b)
		tree.sst[level+1] = b
	}
	tree.loadLevel(lNextPath, level+1)
	log.Println("Done with merge")
	return nil
}

// Compact is similar to Merge but will only merge files within the same level. This is
// intended to be done at the highest level of the tree so that any tombstones can be
// permanently deleted.
func (tree *LsmTree) Compact(level int) {
	highestTreeLevel := len(tree.sst) - 1

	if level == 0 {
		log.Println("Cannot compact files in level 0 of the SST")
		return
	} else if level > highestTreeLevel {
		log.Println("Compact cannot process level", level, "because the tree only has", highestTreeLevel, "levels")
		return
	}

	lPath := sst.PathForLevel(tree.path, level)

	log.Println("Debug load files from", lPath)

	files := sst.Filenames(lPath)
	for i, _ := range files {
		files[i] = lPath + "/" + files[i]
	}
	log.Println("Files", files)

	removeDeleted := false
	if level == highestTreeLevel {
		log.Println("Compacting highest level of tree", level, "deleted keys will be permanently removed")
		removeDeleted = true
	}

	tmpDir, err := sst.Compact(files, tree.path, tree.bufferSize, removeDeleted)
	log.Println("Files in", tmpDir, err)

	if !tree.merge.Immediate {
		tree.lock.Lock()
		defer tree.lock.Unlock()
	}

	for _, filename := range files {
		os.Remove(filename)
	}

	err = os.RemoveAll(lPath)
	if err != nil {
		log.Fatal(err)
	}

	err = os.Rename(tmpDir, lPath)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(tree.sst)

	// Drop and reload cache for all files from this level
	// TODO: more efficient solution?
	var a sst.SstLevel
	tree.sst[level] = a
	tree.loadLevel(lPath, level)

	log.Println("Done with compact")
}

// MergeJob runs as a background thread and coordinates when to check SST levels for merging.
func (tree *LsmTree) MergeJob() {
	if tree.merge.Interval == 0 {
		log.Println("MergeJob interval not set, stopping goroutine")
		return
	}

	for {
		// sleep for interval
		// TODO: use time.NewTicker instead?
		time.Sleep(tree.merge.Interval)
		log.Println("LSM merge job woke up")

		tree.mergeJob()
	}
}

// mergeJob determines if a level needs to be merged and runs Merge() as needed.
func (tree *LsmTree) mergeJob() { // TODO: any state to receive?
	// find SST levels
	levels := sst.Levels(tree.path)

	// Don't forget level 0
	levels = append([]string{"."}, levels...)

	//log.Println("mergeJob path", tree.path, "levels", levels)

	// for each level
	for i, dir := range levels {
		// has a threshold been exceeded?
		lPath := tree.path + "/" + dir
		files := sst.Filenames(lPath)

		//log.Println("mergJob lPath", lPath, "files", files)
		merge := false

		// TODO: need to multiply num files by level #?
		//       what prevents a cascade? EG: merge l0, then immediately merge l1, l2, etc
		//       This is why we allow (num_files * level) files below here -
		if tree.merge.NumberOfSstFiles > 0 &&
			len(files) > tree.merge.NumberOfSstFiles*(i+1) {
			log.Println("Merge level", i, " - Number of files", len(files), "exceeded merge threshold", tree.merge.NumberOfSstFiles)
			merge = true

			if i == len(levels) {
				// last level is maxed out, need to even-out merges to avoid constantly merging
				// that last level, which will absolutely kill performance
				if tree.cooldown == 0 {
					tree.cooldown = tree.merge.NumberOfSstFiles * (i + 1)
				} else {
					merge = false
					tree.cooldown--
				}
			}

		}
		// TODO: DataSize
		// TODO: TimeWindow

		if merge {
			tree.Merge(i)
		}
	}
}
