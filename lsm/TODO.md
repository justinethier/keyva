TODO list for SST / Merge:

- Update Get to allow reading data from an SST for level 0, 1, 2, etc
- Provide a `Compact` function that we can call to compact level `n` of the SST.
- Once that works, handle cache, file deletion, etc
- Provide some means of automating compaction
