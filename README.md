# lsmtree

[![Build Status](https://travis-ci.com/elliotcourant/lsmtree.svg?branch=master)](https://travis-ci.com/elliotcourant/lsmtree)
[![](https://godoc.org/github.com/elliotcourant/lsmtree?status.svg)](http://godoc.org/github.com/elliotcourant/lsmtree)

This is my attempt at creating an LSM-Tree in Go. This is primarily a research project
to learn about the inner workings and limitations of LSM-Tree's the hard way. At the
moment this library is not meant to be used.

## Background

Here are some links to information or research that I'm using to build this library.

- [Log Structured Merge Trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree)
- [WiscKey: Separating Keys from Values in SSD-conscious Storage](https://lrita.github.io/images/blog/WiscKey-separating-keys-from-values-in-SSD-Conscious-storage.pdf)

## Design

This library's primary goal is to implement an LSM-Tree based Key-Value store. 

The goals of this library:

- [ ] Transactional
    - [ ] SNAPSHOT Isolation
        - [ ] When transaction X begins, all changes committed after that point in time _are not_
              visible to transaction X. But all changes committed before _are_ visible.
        - [ ] If a value is read during transaction X and that value changes _before_ transaction X
              is committed, then transaction X must fail due to a conflict. This is to make sure
              that changes made during X are not based off of stale data.
    - [ ] READ COMMITTED Isolation
        - [ ] When transaction X reads values, it will only be able to see values from other
              transactions that have been completely committed. If a transaction is not committed or
              is partially committed then it's values should not be visible to X.
        - [ ] If a value is read during transaction X and that value changes _before_ transaction X
              is committed, then transaction X must fail due to a conflict. This is to make sure
              that changes made during X are not based off of stale data.
    - [ ] A transaction can have multiple iterators. But writes during that transaction will not
          be visible to existing iterators.
- [ ] Multiple individual managed LSM-Trees (referred to as Tables).
    - [ ] Writes to any table is still written to a single WAL.
    - [ ] Reads can only target a single table.
    - [ ] Each table has it's own set of Heap files for Key storage.
- [ ] Values are stored separately from keys (called Value Files).
    - [ ] Values should be stored in their own files and should be broken up into X sized chunks.
    - [ ] Each value should have a checksum to ensure the value has not been corrupted.
- [ ] Keys are stored in their own files (called Heap Files).
    - [ ] Heap files are specific to a single table.
    - [ ] Each heap file should be sorted (descending) by key and transaction timestamp.
    - [ ] Heap files are created when the number of keys in memory reaches a certain threshold.
          The keys are then flushed to the disk in the form of a heap file. The highest number heap
          file for a given table is the most recent.
    - [ ] Heap file names consist of:
        - [ ] 1 Byte indicating it is a heap file.
        - [ ] 2 Bytes indicating the tableId.
        - [ ] 8 Bytes indicating the heapId.
        - [ ] 4 Bytes indicating the number of times this heap has been merged.
    - [ ] (Compaction) When a multiple heaps are merged, they will be use the largest heapId of the
          heaps being merged. The resulting file will increment the number of times that older files
          have been merged into that heap.
        - [ ] If a merge results in a single heap, then the merge counter can be reset to 0.
    - [ ] (Compaction) Heaps will be merged asynchronously and will be merged when the number of
          heaps exceeds a certain threshold or when the oldest heap is more than a X hours old.
        - [ ] Heaps should be merged into a single resulting heap, at which point the pointer for
              a range of keys should be updated asynchronously and should not block reads or writes
              if possible.
- [ ] Write Ahead Log
    - [ ] When writes are committed, they must first be written to the WAL file. If the writes fail
          to write to the WAL then the commit should fail.
    - [ ] Once a batch of writes has been persisted to the WAL the writes should be committed to the
          in memory data set. This should be done in such a way that transactions that begin during
          this window will not be able to see the changes at least until AFTER the memtable change
          has completed.
    - [ ] Items should only be written to heap and value files AFTER they have been written to the
          WAL and the memtable. Making written data available to the user is far more important than
          storing that data in it's on medium.
    - [ ] When the database is opened check the WAL index for the last items written to the disk. If
          there are items that have not been written to the disk yet then write them before building
          the memtables.