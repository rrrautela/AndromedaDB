# AndromedaDB

[![Java](https://img.shields.io/badge/Java-17%2B-orange?logo=openjdk)]()
[![Status](https://img.shields.io/badge/status-learning%20project-blue)]()
[![Storage Engine](https://img.shields.io/badge/type-LSM--style%20KV%20engine-purple)]()

AndromedaDB is a compact Java key-value storage engine built from scratch to explore the core ideas behind LSM-style systems such as RocksDB, LevelDB, and Cassandra. The project is intentionally small, source-first, and implementation-focused: it writes to a WAL, stores ordered data in a memtable, flushes immutable SSTables, prunes reads with sparse key ranges and Bloom filters, and periodically compacts older files.

> Observed benchmark result: 500,000 writes in roughly 5.05 seconds, or about 99,000 writes/sec in the project’s local benchmark run.

## At a glance

| Area | Implementation |
|---|---|
| Language | Java |
| Storage model | LSM-style key-value engine |
| Core architecture | WAL → Memtable → SSTable → Compaction |
| Durability mechanism | Write-ahead log plus replay on startup |
| Concurrency model | `ConcurrentSkipListMap`, `ExecutorService`, `AtomicBoolean`, synchronized metadata access |
| Benchmark result | ~99,000 writes/sec on the repo’s local benchmark |

---

## Overview

AndromedaDB is a learning-oriented storage engine project designed to make the internals of modern write-optimized databases concrete. Instead of treating WALs, memtables, SSTables, compaction, and Bloom filters as abstract concepts, this code implements them directly in Java using plain file I/O and standard library data structures.

The overall goal is not to match production database maturity. It is to understand the runtime behavior, trade-offs, and structural decisions behind systems that prioritize fast append-heavy writes and cheap recovery.

---

## What it implements

| Component | Status |
|---|---|
| Write-ahead log (WAL) | Implemented |
| Memtable | Implemented via `ConcurrentSkipListMap<Long, String>` |
| Immutable SSTables | Implemented |
| Bloom filters | Implemented |
| Sparse key-range metadata | Implemented |
| Tombstones | Implemented as `__DELETED__` |
| Background memtable flush | Implemented |
| Background compaction | Implemented |
| Crash recovery via WAL replay | Implemented |
| Concurrent background scheduling | Implemented with executors and `AtomicBoolean` |

This is best described as an LSM-inspired prototype rather than a production database implementation.

---

## Architecture

The high-level flow in the project is straightforward:

```text
Write
  → WAL append
  → Memtable insert
  → Freeze on size threshold
  → Background flush
  → SSTable file
  → Compaction of older SSTables
```

Reads follow a layered pattern:

```text
GET
  → Memtable lookup
  → Sparse key-range filtering
  → Bloom filter check
  → SSTable scan (newest to oldest)
```

This matches the actual implementation in `Main.java`, `StorageEngine.java`, `WriteAheadLog.java`, `SSTable.java`, and `Compactor.java`.

---

## Write path

The write path is intentionally simple and follows the project’s code flow:

1. A value is generated in `Main.main()`.
2. The engine appends the key/value pair to the active WAL using `engine.appendToWAL(...)`.
3. The same key/value is written into the active in-memory memtable using `memTable.put(...)`.
4. When `memTable.size() >= 1000`, the active WAL is flushed and closed, a fresh WAL segment is opened, and the old memtable is swapped out.
5. The frozen memtable is submitted to the background flush executor.
6. `SSTable.flushMemTable()` creates a new `data_<n>.db` file, serializes the sorted entries, builds a Bloom filter, updates sparse metadata, and removes the WAL segment associated with that memtable.

This is a classic LSM-style write batching pattern: append quickly, then persist to disk asynchronously.

---

## Read path

Reads in `StorageEngine.get(...)` are ordered to minimize unnecessary disk I/O:

1. Check the active memtable first.
2. Search SSTables from newest to oldest.
3. Skip any SSTable whose key range cannot possibly contain the target key.
4. Use the SSTable’s Bloom filter to reject keys that are definitely absent.
5. Only if the key remains plausible do a sequential scan of the file.

This gives the project a cheap pre-filtering strategy while keeping the actual lookup logic simple and readable.

> The Bloom filter is probabilistic: it can produce false positives, but it does not produce false negatives in the implementation.

---

## Compaction

Compaction is triggered once the sparse index reaches a threshold (`sparseIndex.size() >= 4` in the flush logic). The implementation then repeatedly merges the oldest and newest SSTables with `Compactor.compact(...)` and continues until a single SSTable remains, or until there are no more files to merge.

The merge logic is pairwise and simple:

```text
olderFile + newerFile -> compacted output
```

It preserves the newest value for any duplicate key and drops tombstones when they are merged away. A delete is represented as a tombstone value, `__DELETED__`, which ensures stale values are superseded by newer writes before the old value is physically discarded during compaction.

This is an educational implementation of compaction, not a production multi-level LSM compaction engine.

---

## Crash recovery

On restart, the engine rebuilds its in-memory state by replaying WAL files from disk. `WriteAheadLog.recoverFromWAL(...)` scans files named like `wal_<n>.log`, sorts them numerically, and replays each record into a fresh `ConcurrentSkipListMap`.

This restores the state from recent writes before new work begins. The project’s crash-recovery model is therefore WAL-driven recovery, not a fully durable storage protocol with fsync semantics or a manifest-based commit log.

This project is intentionally transparent about the trade-off: it uses WAL replay to reconstruct recent state, but it does not use `fsync` or a more robust persistent write protocol in the implementation shown here.

---

## Concurrency

The project uses a lightweight concurrency model rather than a fully general-purpose storage-engine synchronization system.

| Component | Role |
|---|---|
| `ConcurrentSkipListMap<Long, String>` | Memtable storage with ordered, thread-safe inserts and lookups |
| `ExecutorService` | Background flush and compaction workers |
| `AtomicBoolean compactionRunning` | Prevents duplicate compaction jobs from being scheduled |
| `synchronized (engine.sparseIndex)` | Protects shared sparse metadata updates during flush and compaction |

The main write path stays in the caller thread, while flush and compaction run asynchronously. That is sufficient for the learning prototype, but it is not a claim that the whole engine is a fully hardened concurrent storage system.

---

## Performance

The repo includes a local benchmark that writes 500,000 records and reports the following result:

| Metric | Result |
|---|---:|
| Writes | 500,000 |
| Memtable threshold | 1,000 |
| Insert time | ~5.05 s |
| Throughput | ~99,000 writes/sec |
| WAL segments generated | 500 |
| SSTables generated | 500 |
| SSTables after compaction | 1 |

This should be read as a project benchmark, not as a production database claim. The GitHub/README notes in this repo also highlight an important optimization: keeping the WAL stream open across writes was a major contributor to the observed throughput, rather than reopening the WAL file for each write.

### Benchmark note

This is a local single-process Java benchmark for the repository itself. It is useful for understanding the project’s write path and the effect of file I/O choices, but it should not be compared directly to optimized production storage systems.

---

## Project structure

| File | Responsibility |
|---|---|
| `Main.java` | Benchmark driver and lifecycle entry point |
| `StorageEngine.java` | Main orchestrator for WAL, memtable, reads, flush, and compaction state |
| `WriteAheadLog.java` | WAL segment creation, append, replay, and cleanup |
| `SSTable.java` | SSTable serialization, sparse metadata construction, and flush behavior |
| `Compactor.java` | SSTable merging and tombstone-aware compaction |
| `BloomFilter.java` | In-memory probabilistic key membership checks |
| `SparseIndexEntry.java` | Per-file metadata: first key, last key, filename, and Bloom filter |
| `Entry.java` | Lightweight record wrapper used for file parsing |

---

## Design decisions

The strongest choices in this project are the ones that directly match the LSM pattern:

- **Why WAL first?** Because the project wants recent writes to be recoverable even before their memtable is flushed.
- **Why `ConcurrentSkipListMap`?** Because it provides ordered keys and a simple thread-safe in-memory structure for the memtable.
- **Why immutable SSTables?** Because each flush creates a stable snapshot that is easier to reason about and easier to compact safely.
- **Why Bloom filters?** Because they make negative lookups cheap before scanning a file.
- **Why background flush/compaction?** Because the write path stays append-heavy and the maintenance work happens asynchronously.
- **Why tombstones?** Because deletes need to override older values before those stale entries are physically removed by compaction.

These are practical design decisions for a teaching implementation, not a claim of full production parity.

---

## Getting started

Requirements: a recent JDK (the project is described as Java 17+ in this repo’s documentation and compiles cleanly with a standard Java toolchain).

```bash
git clone https://github.com/rrrautela/AndromedaDB.git
cd AndromedaDB
javac *.java
java Main
```

Running `Main` executes the benchmark path, writes 500,000 records, triggers background flush and compaction, and performs a couple of lookups at the end.

---

## Current limitations

This implementation is intentionally compact and educational. The repository itself highlights some important caveats:

- No explicit `fsync` or file-force call before a write is considered durable.
- No manifest or journal for metadata integrity.
- No atomic rename for newly published files.
- No production-grade multi-level compaction strategy.
- No full database-level concurrency guarantees beyond the specific structures used here.

These are not gaps in the documentation; they are structural limitations of the project as implemented.

---

## What I learned

The project is a strong practical study in the mechanics behind storage-engine design:

- WALs are a simple, effective way to recover recent writes.
- LSM systems optimize for append-heavy ingestion and accept more complexity on read/maintenance paths.
- Compaction is a real operational cost, not just a cleanup step.
- File I/O patterns strongly influence throughput.
- Probabilistic filtering can dramatically reduce useless reads when used appropriately.

---

## Interview relevance

This repo is a useful example for discussing:

- storage-engine fundamentals
- WAL and recovery semantics
- LSM write/read trade-offs
- file-based data structures and serialization
- concurrency with executors and shared metadata
- performance tuning through I/O behavior

It is especially relevant for backend, systems, and storage-engine interviews where the candidate is expected to reason from actual implementation details rather than generic database theory.

---

## Connect

Built by Harshit.

- LinkedIn: [linkedin.com/in/rrrautela](https://linkedin.com/in/rrrautela)
- Email: hs.rautela11@gmail.com
