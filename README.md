# Patina

A crash-safe key-value store: an LSM storage engine in Rust, embedded in a Java 25 Spring Boot service through the Foreign Function & Memory API (no JNI).

```
HTTP ──► service/ (Java, Spring Boot) ──FFM──► engine/ (Rust cdylib)
```

A test kills the writer process 120 times and no acknowledged write is lost. On one laptop, Patina is within error of RocksDB on fsynced writes and random gets, ranges from 1.47× faster (100-byte values) to 2.48× slower (4 KiB) on unsynced writes, and loses by 3.68× on 100-byte scans. [Results](#results) explains why.

## Run

Needs Rust 1.85+ and JDK 25. On Windows, also install the Visual Studio C++ Build Tools and run the commands in Git Bash.

```bash
cd service
./gradlew test      # builds the Rust engine, then runs the Java tests against it
./gradlew bootRun   # API on localhost:8080, data in service/data/
```

Rust tests alone: `cd engine && cargo test`. Move the data with `./gradlew bootRun --args='--patina.data-dir=...'`. `--args='--patina.sync-writes=false'` skips the fsync per write: much faster, but an acked write then survives only a process crash, not power loss.

## API

```bash
curl -X PUT localhost:8080/items/user1 -d alice    # 204 (413 over 1 MiB)
curl -X PUT localhost:8080/items/user2 -d bob
curl localhost:8080/items/user1                    # alice (404 if missing)
curl "localhost:8080/items?limit=1"                # {"items":[{"key":"user1","value":"YWxpY2U="}],"next":"user1"}
curl "localhost:8080/items?after=user1&limit=1"    # {"items":[{"key":"user2","value":"Ym9i"}],"next":null}
curl -X DELETE localhost:8080/items/user1          # 204, also for a missing key
```

Keys are one path segment (no `/`). `GET /items` lists keys in byte order after `after` (exclusive), with values in base64; `limit` is 1–1000, default 50. Pass `next` back as `after` (URL-encoded) until it is `null`; a page can stop early at about 4 MiB.

## Design

Rust owns all storage logic; Java is a thin HTTP layer. It's the same embedding model Kafka Streams and Flink use with RocksDB.

**Write-ahead log** ([wal.rs](engine/src/wal.rs)). Every put and delete is appended to `wal.log` and then applied to the memtable (a `BTreeMap`), both under one write lock. Records are little-endian:

```
[payload_len u32][crc32 u32][payload]
payload = [op u8 = 0][key_len u32][key][value_len u32][value]   put
        | [op u8 = 1][key_len u32][key]                         delete
```

A record goes out in one write and is fsynced before the call returns (unless sync-writes is off). If the write or fsync fails, the call errors, the memtable is unchanged, and the WAL refuses further appends until reopened, because a partial record would hide everything written after it.

On open, recovery replays records up to the first incomplete or corrupt one, truncates the file there and syncs it. Tested by cutting a log at every byte offset.

**Flush** ([lib.rs](engine/src/lib.rs)). When writes since the last flush pass 4 MiB, the write that crossed the limit flushes, still under the lock:

1. Write `<seq>.sst.tmp` and fsync it.
2. Rename it to `<seq>.sst`.
3. fsync the directory (Unix only: Rust's std can't open a directory on Windows).
4. Truncate the WAL and fsync it.
5. Clear the memtable.

A crash before step 2 leaves at most a `.tmp`, which open deletes, and the WAL still has the data. A crash between steps 2 and 4 leaves the data in both places; replay rebuilds the same entries into the memtable, which shadows the table harmlessly. A failed flush doesn't fail the write, which is already in the WAL; the next write retries it.

An SSTable is sorted entries plus a footer (entry count, `compacted_through`, crc32, magic). Open verifies the crc and loads every key with its value's offset; values are read on demand. A get checks the memtable, then the tables newest first, and a tombstone ends the search.

**Compaction.** When a flush leaves more than 4 tables, the write starts a background thread that merges them all into one (newest wins, values streamed one at a time) and drops tombstones. At most one runs at a time. Tables are immutable, so the merge reads its inputs without the lock while reads, writes and flushes go on. The output's seq is reserved when the merge starts, so tables flushed meanwhile get higher seqs and stay in front of it, in memory and after a reopen; every table older than the newest input is an input, so dropping tombstones is safe. The output goes through the flush's tmp → fsync → rename → fsync-dir steps and records the highest input seq as `compacted_through`. Then a brief write lock swaps it in for the inputs, and only then are the inputs closed and deleted. A crash can leave inputs behind that still hold values whose tombstones were dropped, so open first deletes every table with a seq at or below the highest `compacted_through`. Deleted keys can't come back. Closing the engine waits for a running compaction.

**Boundary** ([lib.rs](engine/src/lib.rs), [PatinaEngine.java](service/src/main/java/dev/patina/engine/PatinaEngine.java)). Seven C functions: `patina_open`, `patina_close`, `patina_put`, `patina_get`, `patina_delete`, `patina_scan`, `patina_free`.

- Every entry point except `patina_free` (it only drops a buffer) wraps its body in `catch_unwind`, so a panic becomes an error code instead of unwinding into the JVM.
- Whoever allocates a buffer frees it. Java passes keys and values in a per-call `Arena`, and Rust copies what it keeps. Rust allocates the `get` and `scan` results; Java copies them out and calls `patina_free` in a `finally`.
- Java calls hold a read lock and check `closed`; `close()` takes the write lock. The handle is never freed mid-call, and calls after close throw `IllegalStateException`.

**Scans.** `patina_scan(start, limit)` merges the memtable and all tables under one read lock and returns up to `limit` live entries (at most 1000, capped near 4 MiB) plus a `more` flag, in one buffer. Nothing survives the call, so there's no iterator or cursor to leak. HTTP paging scans from `after + 0x00`, the smallest key after the cursor. Each page is consistent on its own; the pages together aren't a snapshot.

## Results

AMD Ryzen 7 5700U, 15.3 GiB RAM, Samsung MZVLB512HBJQ NVMe SSD, Windows 11 Home, Temurin 25.0.5, rustc 1.99.1, rocksdbjni 11.8.1, JMH 1.37. One thread, one fork per row: `±` is the 99.9% interval over the 5 iterations of a single run. Windows Defender real-time scanning was on. To reproduce: `cd service && ./gradlew jmh` (output in `service/build/results/jmh/results.txt`).

### Patina vs RocksDB

Patina runs its defaults: a 4 MiB memtable, flushed inside the write, and a full compaction above 4 tables on a background thread. RocksDB runs with compression off and `WriteOptions.setSync` matching Patina, WAL on and everything else default (64 MiB write buffer, background leveled compaction, no bloom filter, default block cache). Keys are 8 bytes and values random. Writes start from an empty store and keep it from warmup (3 × 1 s) through measurement (5 × 2 s). Reads run on 48 MiB of preloaded data (466,033 keys at 100 B, 12,264 at 4 KiB) after reopening the engine.

The four sync-off write rows come from a later session, after compaction moved to a background thread ([below](#moving-compaction-off-the-write-path)). The other rows come from the first session, when compaction still ran inside the write, and weren't rerun. RocksDB ran 1.58–1.70× slower on the sync-off rows in the later session, so compare within a row, not across sessions.

ops/s, higher is better:

| Benchmark | Value | Sync | Patina | RocksDB |
|---|---|---|---:|---:|
| sequentialPut | 100 B | on | 778.072 ± 119.367 | 760.180 ± 77.750 |
| sequentialPut | 4 KiB | on | 698.252 ± 271.792 | 736.886 ± 17.220 |
| sequentialPut | 100 B | off | 85,181.635 ± 21,522.764 | 76,329.952 ± 5,476.861 |
| sequentialPut | 4 KiB | off | 13,211.166 ± 18,218.807 | 32,726.657 ± 1,554.723 |
| randomPut | 100 B | on | 777.608 ± 34.341 | 719.605 ± 105.333 |
| randomPut | 4 KiB | on | 663.932 ± 317.690 | 724.062 ± 35.447 |
| randomPut | 100 B | off | 84,194.343 ± 12,744.043 | 57,334.793 ± 11,277.550 |
| randomPut | 4 KiB | off | 13,502.171 ± 11,700.699 | 27,271.705 ± 4,094.875 |
| randomGet | 100 B | | 119,601.057 ± 34,748.958 | 130,243.174 ± 5,217.127 |
| randomGet | 4 KiB | | 117,198.350 ± 13,191.961 | 130,032.778 ± 11,034.726 |
| scan100 | 100 B | | 3,271.111 ± 138.891 | 12,030.384 ± 1,346.870 |
| scan100 | 4 KiB | | 1,971.409 ± 416.063 | 1,980.491 ± 462.473 |

### Moving compaction off the write path

Compaction used to run inside the write that triggered it, under the lock. To measure the change, the sync-off write benchmarks ran on the old engine and then on the new one, back to back on the same machine. RocksDB is the control: its code didn't change.

ops/s, higher is better:

| Benchmark | Value | Patina, inline compaction | Patina, background compaction | RocksDB, first run | RocksDB, second run |
|---|---|---:|---:|---:|---:|
| sequentialPut | 100 B | 19,820.125 ± 19,582.858 | 85,181.635 ± 21,522.764 | 63,945.807 ± 8,537.997 | 76,329.952 ± 5,476.861 |
| sequentialPut | 4 KiB | 2,007.568 ± 2,126.176 | 13,211.166 ± 18,218.807 | 29,080.694 ± 2,955.431 | 32,726.657 ± 1,554.723 |
| randomPut | 100 B | 22,478.147 ± 27,246.041 | 84,194.343 ± 12,744.043 | 47,279.888 ± 10,000.114 | 57,334.793 ± 11,277.550 |
| randomPut | 4 KiB | 1,913.188 ± 3,273.246 | 13,502.171 ± 11,700.699 | 24,686.308 ± 2,272.613 | 27,271.705 ± 4,094.875 |

Patina got 3.75–7.06× faster. RocksDB also ran 10–21% faster in the second run, so part of that is the machine; divided out, the gain is 3.09–6.39×. An earlier run of the new engine landed within error of these numbers. The first session's inline numbers for these rows (35,653.526, 1,953.610, 32,695.540 and 1,698.491 ops/s) aren't comparable: RocksDB ran 1.74–1.94× faster in that session than in the first run here.

### FFM cost per call

One hot key in the memtable, read and written through `PatinaEngine` with sync off, against a `ConcurrentSkipListMap`. Warmup 10 × 2 s, measurement 5 × 2 s. Separate runs varied by up to about 20%, more than `±` shows.

ns/op, lower is better:

| Operation | Value | Patina through FFM | `ConcurrentSkipListMap` |
|---|---|---:|---:|
| get | 100 B | 445.889 ± 10.986 | 9.337 ± 1.325 |
| get | 4 KiB | 1,191.682 ± 62.078 | 9.096 ± 0.189 |
| put, sync off | 100 B | 3,907.308 ± 58.858 | 19.322 ± 0.286 |
| put, sync off | 4 KiB | 16,946.746 ± 896.255 | 19.533 ± 0.802 |

A get crosses the boundary twice (`patina_get`, `patina_free`) and copies the value twice (out of the memtable, then into a `byte[]`): 436.552 ns more than the skip list at 100 B, 1,182.586 ns at 4 KiB. A put adds a WAL write and, every 4 MiB of puts, a flush that fsyncs.

### Why Patina loses

- **Unsynced writes, 2.02–2.48× slower at 4 KiB.** At 100 B Patina is now within error (sequential) or 1.47× faster (random). Compaction used to run inside the write: each one rewrote every live entry and the store grew all trial, so throughput fell through each trial (randomPut at 100 B: 31,900.964 down to 13,402.410 ops/s over the five iterations) and the error bars were as wide as the scores. On a background thread it no longer holds up writes, and 100 B iterations stay at 78,773.344–92,126.956 ops/s. What's left is the flush, still inside the write and under the lock: every 4 MiB (RocksDB every 64 MiB), which is every 1,022 puts at 4 KiB against every 38,836 at 100 B, and it most likely waits on a disk busy with the compaction's writes. One 4 KiB iteration fell to 5,599.510 ops/s. A 100 B put now averages 11.74–11.88 µs, down from 44.49–50.45 µs, against 3.907 µs with nothing to flush (FFM table).
- **Synced writes tie.** Both engines wait for one fsync per write, 1.285–1.506 ms here, which swamps everything else.
- **100 B scans, 3.68× slower.** Patina reads each value with its own seek and read behind a per-table mutex, about 3 µs per entry; RocksDB reads blocks of many entries. At 4 KiB the bytes dominate and the two tie. The boundary isn't the cost: Patina returns 100 entries in one call, while RocksDB's iterator takes four JNI calls per entry.
- **Gets are within error.** Every key is in memory, so a get is one map lookup in the memtable and one per table, then one read. That was at most 5 lookups when these rows ran, with compaction inline; tables flushed during a background merge can now push it past 5. The 48 MiB dataset most likely sat in the OS file cache.

## Limitations

- Single node, single writer: one lock, one fsync per synced write, no group commit (about 660–780 synced writes/s here).
- Every key is in memory (a full index per SSTable), and open reads every SSTable end to end.
- Compaction always merges every table, so its cost grows with the store, and it needs disk space for a second copy of the data. Flushes still run inline under the write lock.
- Tables flushed while a merge runs stay unmerged until it ends, so under steady writes the table count, and the lookups per get and scan, can grow well past 4. Nothing stalls writes to cap it.
- No lock file: two processes on one data dir corrupt each other's WAL.
- Power-loss safety is argued from fsync-before-ack and the torn-tail test, not tested. On Windows, directory fsyncs are skipped.
- Recovery assumes the only damage is a torn tail: corruption mid-log silently drops every later record.
- No bloom filters, block cache, compression or leveled compaction.

## Contact

Questions, partnership inquiries, or feedback about this project:

- LinkedIn: [Apostolos Kakarantzas](https://www.linkedin.com/in/akakarantzas/)
- X / Twitter: [@aevagnarly](https://x.com/aevagnarly)
- Email: [hello@akakarantzas.com](mailto:hello@akakarantzas.com)
