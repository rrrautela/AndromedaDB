# AndromedaDB

> A custom LSM Tree-inspired storage engine built from scratch in Java featuring WAL recovery, SSTables, Bloom Filters, asynchronous flushing, and background compaction.

AndromedaDB is a write-optimized key-value database built to explore the core ideas behind modern storage engines such as RocksDB, Cassandra, and LevelDB.

The project implements the complete lifecycle of data, from durable WAL writes and in-memory Memtables to immutable SSTables, Bloom Filter-based lookups, crash recovery, and background compaction, while achieving over **90,000 writes/sec** during benchmarking.

---

# Why I Built This

I wanted to understand how modern databases such as RocksDB, Cassandra, and LevelDB work internally instead of treating them as black boxes.

This project was built from scratch to explore storage-engine fundamentals including WALs, Memtables, SSTables, Bloom Filters, crash recovery, background flushing, and compaction.

---

# Performance Highlights

```text
~91,000 Writes/sec
50,000 Write Benchmark
Background Flush
Background Compaction
Segmented WAL Recovery
```

---

# Features

- Segmented Write-Ahead Log (WAL)
- Crash Recovery from WAL
- ConcurrentSkipListMap-based Memtable
- Immutable SSTables stored on disk
- Bloom Filters for fast negative lookups
- Sparse Index metadata
- Tombstone-based deletes
- Per-SSTable Bloom Filters
- SSTable Key-Range Metadata
- Binary File Storage Format
- Background Memtable Flush
- Background SSTable Compaction
- AtomicBoolean-based compaction scheduling
- Thread-safe metadata management
- 90K+ writes/sec benchmark

---

# High Level Architecture

```text
                Write Request
                      │
                      ▼
                Append To WAL
                      │
                      ▼
                   Memtable
                      │
           (Threshold Reached)
                      │
                      ▼
               Freeze Memtable
                      │
          ┌───────────┴───────────┐
          │                       │
          ▼                       ▼
 New Active Memtable      Background Flush
                                   │
                                   ▼
                               SSTable
                                   │
                                   ▼
                            Sparse Index
                                   │
                                   ▼
                      Background Compaction
                                   │
                                   ▼
                           Final SSTable
```

---

# Write Path

## Step 1: Write-Ahead Log (WAL)

Every write is first appended to a WAL segment.

Example:

```text
PUT(100, "hello")
```

gets recorded in:

```text
wal_1.log
```

This guarantees data can be recovered after crashes.

---

## Step 2: Memtable

After WAL append, data is inserted into an in-memory sorted structure:

```java
ConcurrentSkipListMap<Long, String>
```

Benefits:

- Sorted keys
- Fast writes
- Range scan friendly
- Thread-safe

---

## Step 3: Memtable Freeze

When the Memtable reaches its configured threshold:

```text
1000 entries
```

the engine performs a Memtable swap:

```java
oldMemTable = memTable;
memTable = new ConcurrentSkipListMap<>();
```

This allows new writes to continue immediately.

---

## Step 4: Background Flush

The frozen Memtable is handed to a background thread.

That thread converts it into an immutable SSTable on disk.

Example:

```text
data_1.db
data_2.db
data_3.db
```

---

# Read Path

When a key is requested:

```text
GET(key)
```

AndromedaDB performs:

## 1. Memtable Lookup

Check active Memtable first.

---

## 2. Sparse Index Filtering

Identify SSTables whose key range may contain the key.

Example:

```text
SSTable A: 1 - 100
SSTable B: 101 - 200
```

A lookup for:

```text
150
```

immediately skips SSTable A.

---

## 3. Bloom Filter Check

Each SSTable owns its own Bloom Filter.

During reads:

```text
Key Lookup
    ↓
Bloom Filter Check
    ↓
Probably Exists
OR
Definitely Not Present
```

If the Bloom Filter says:

```text
Definitely Not Present
```

AndromedaDB skips that SSTable completely.

This prevents unnecessary disk scans and significantly improves read performance when many SSTables exist.

Bloom Filters may return false positives but never false negatives.

## Sparse Index Metadata

Each SSTable stores metadata containing:

```text
Minimum Key
Maximum Key
File Name
Bloom Filter
```

Example:

```text
SSTable A: 1 → 100
SSTable B: 101 → 200
SSTable C: 201 → 300
```

Searching for key:

```text
250
```

allows AndromedaDB to immediately skip SSTables A and B.

This acts as the first optimization layer before Bloom Filters are consulted.

---

## 4. SSTable Scan

Only candidate SSTables are scanned.

---

# Deletes

Deletes use tombstones.

Instead of immediately removing data:

```text
DELETE(100)
```

becomes:

```text
100 -> __DELETED__
```

This preserves deletion information across SSTables.

During compaction, tombstones are discarded and deleted records disappear permanently.

---

# Compaction

Over time many SSTables accumulate:

```text
data_1.db
data_2.db
data_3.db
...
```

Compaction merges them into larger SSTables.

Example:

```text
data_1 + data_2
          ↓
     compacted_1

compacted_1 + data_3
          ↓
     compacted_2
```

Benefits:

- Removes stale versions
- Removes deleted records
- Reduces SSTable count
- Improves read performance

---

# WAL Recovery

If the database crashes:

```text
Power Failure
Application Crash
System Restart
```

AndromedaDB scans all WAL segments during startup.

Example:

```text
wal_1.log
wal_2.log
wal_3.log
```

Operations are replayed in order to reconstruct the Memtable.

This prevents data loss.

---

# Concurrency Model

## Flush Thread

Responsible for:

```text
Memtable -> SSTable
```

conversion.

---

## Compaction Thread

Responsible for:

```text
SSTable -> Compacted SSTable
```

merges.

---

## AtomicBoolean Guard

Prevents duplicate compaction jobs from being scheduled simultaneously.

---

## Synchronized Sparse Index

Protects shared metadata from concurrent modifications.

---

# Performance

Benchmark Configuration:

```text
50,000 Writes
Memtable Size = 1000
```

Results:

```text
~91,000 Writes/sec
```

A major optimization involved keeping WAL segments open instead of opening and closing files for every write.

This improved throughput from roughly:

```text
37,000 writes/sec
```

to:

```text
91,000 writes/sec
```

---

# Technologies & Concepts

- Java
- ConcurrentSkipListMap
- ExecutorService
- AtomicBoolean
- Bloom Filters
- Write-Ahead Logging (WAL)
- SSTables
- LSM Tree Concepts
- Background Compaction
- File I/O
- Crash Recovery

---

# Future Improvements

# Design Decisions

## Why WAL Before Memtable?

Durability.

If the process crashes after a write, the WAL can be replayed to recover data.

## Why Immutable SSTables?

Immutable files eliminate update-in-place complexity and make compaction simpler.

## Why Background Flush?

Writers should not wait for disk serialization.

## Why Background Compaction?

Compaction is expensive and should not block incoming writes.

## Why ConcurrentSkipListMap?

Provides a sorted, thread-safe Memtable implementation suitable for future range queries and concurrent access.

- Multi-Level Compaction (L0/L1/L2)
- Sparse Index Offsets
- Binary Search SSTables
- SSTable Block Indexes
- Compression
- Distributed Replication
- Consensus Protocol Integration
- Range Query Optimization

---

# Key Takeaway

AndromedaDB demonstrates how modern write-optimized storage engines work internally by combining:

```text
WAL
+
Memtable
+
SSTables
+
Bloom Filters
+
Compaction
```

to achieve durable and scalable storage.