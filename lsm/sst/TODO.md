# sst.Find()

TODO: setup TDD for below, and get started...

How to make this work.

## Level 0

- Search files in reverse order (newest to oldest) - already do this
- Binary search on index entries to find the right index
- Load and cache that index's entries
- Binary search on those entries to find our key

## Other Levels

Everything is in sorted order so can we take advantage of that? Should be able to do a binary search to find the appropriate SST file.

Do with a new function, eg: findSortedSst or such.

From there do the same steps as for level 0.
