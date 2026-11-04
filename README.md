# Patina

A crash-safe key-value store: an LSM storage engine in Rust, embedded in a Java 25 Spring Boot service through the Foreign Function & Memory API (no JNI).

```
HTTP ──► service/ (Java, Spring Boot) ──FFM──► engine/ (Rust cdylib)
```

Rust owns all storage logic; Java is a thin HTTP layer. It's the same embedding model Kafka Streams and Flink use with RocksDB.

A test kills the writer process 120 times and no acknowledged write is lost. On one laptop it ties RocksDB on fsynced writes from one thread (1.2× slower from 8 threads), stays within 10% on gets, and is 1.2–1.6× faster on unsynced 100-byte writes, but 2.6–4.1× slower on average on unsynced 4 KiB writes and 3.6× slower on 100-byte scans. [Results](#results) explains why.

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

**Write-ahead log** ([wal.rs](engine/src/wal.rs)). Every put and delete is appended to the current WAL file, `<n>.wal`, and then applied to the memtable (a `BTreeMap`), both under one write lock. Records are little-endian:

```
[payload_len u32][crc32 u32][payload]
payload = [op u8 = 0][key_len u32][key][value_len u32][value]   put
        | [op u8 = 1][key_len u32][key]                         delete
```

A record goes out in one write and is fsynced before the call returns (unless sync-writes is off). Synced writes that arrive while an fsync is running queue up, and the first of them to wake writes them all at once and fsyncs once (group commit): a test has 8 threads write at once and counts 2 fsyncs. If the write or fsync fails, every call in that batch errors, the memtable is unchanged, and the WAL refuses further appends until reopened, because a partial record would hide everything written after it.

On open, recovery replays every WAL file left, oldest first, each up to its first incomplete or corrupt record, and truncates and syncs a file cut short. Tested by cutting a log at every byte offset.

**Flush** ([lib.rs](engine/src/lib.rs)). When the memtable passes 4 MiB, the write that crossed the limit seals it: under the lock it opens the next WAL file, swaps in a fresh memtable, keeps the full one as the immutable memtable and reserves the next seq for its table. A background thread then writes it out without the lock:

1. Write `<seq>.sst.tmp` and fsync it.
2. Rename it to `<seq>.sst`.
3. fsync the directory (Unix only: Rust's std can't open a directory on Windows).
4. Delete the WAL files that held the immutable memtable and fsync the directory again (Unix only).
5. Under a brief write lock, put the table in front of the others and drop the immutable memtable.

Meanwhile writes go to the fresh memtable and WAL, and gets and scans check both memtables before the tables. Only one memtable is sealed at a time: a write that fills the fresh one before the flush ends waits for it, without the lock, so reads go on. That caps memory at two memtables, plus one record per write that is waiting.

A crash before step 2 leaves at most a `.tmp`, which open deletes, and the WALs still have the data. A crash between steps 2 and 4 leaves the data in both places; replay rebuilds the same entries into the memtable, which shadows the table harmlessly. Step 4 ends before step 5 because only after step 5 can a newer table exist, and a WAL left behind would hide that table's values on replay. Reserving the seq at the seal keeps tables in recency order, in memory and after a reopen. A failed flush doesn't fail the write, which is already in the WAL: the memtable stays sealed and the next write that fills the fresh one retries the flush. Closing the engine waits for a running flush.

An SSTable is sorted entries plus a footer (entry count, `compacted_through`, crc32, magic). Open verifies the crc and loads every key with its value's offset; values are read on demand. A get checks the memtable, the immutable memtable, then the tables newest first, and a tombstone ends the search.

**Compaction.** When a flush leaves more than 4 tables, the flush thread starts another background thread that merges them all into one (newest wins, values streamed one at a time) and drops tombstones. At most one runs at a time. Tables are immutable, so the merge reads its inputs without the lock while reads, writes and flushes go on. The output's seq is reserved when the merge starts, so tables flushed meanwhile get higher seqs and stay in front of it, in memory and after a reopen; every table older than the newest input is an input, so dropping tombstones is safe. The output goes through the flush's tmp → fsync → rename → fsync-dir steps and records the highest input seq as `compacted_through`. Then a brief write lock swaps it in for the inputs, and only then are the inputs closed and deleted. A crash can leave inputs behind that still hold values whose tombstones were dropped, so open first deletes every table with a seq at or below the highest `compacted_through`. Deleted keys can't come back. Closing the engine waits for a running compaction.

**Boundary** ([lib.rs](engine/src/lib.rs), [PatinaEngine.java](service/src/main/java/dev/patina/engine/PatinaEngine.java)). Seven C functions: `patina_open`, `patina_close`, `patina_put`, `patina_get`, `patina_delete`, `patina_scan`, `patina_free`.

- Every entry point except `patina_free` (it only drops a buffer) wraps its body in `catch_unwind`, so a panic becomes an error code instead of unwinding into the JVM.
- Whoever allocates a buffer frees it. Java passes keys and values in a per-call `Arena`, and Rust copies what it keeps. Rust allocates the `get` and `scan` results; Java copies them out and calls `patina_free` in a `finally`.
- Java calls hold a read lock and check `closed`; `close()` takes the write lock. The handle is never freed mid-call, and calls after close throw `IllegalStateException`.

**Scans.** `patina_scan(start, limit)` merges both memtables and all tables under one read lock and returns up to `limit` live entries (at most 1000, capped near 4 MiB) plus a `more` flag, in one buffer. Nothing survives the call, so there's no iterator or cursor to leak. HTTP paging scans from `after + 0x00`, the smallest key after the cursor. Each page is consistent on its own; the pages together aren't a snapshot.

## Results

AMD Ryzen 7 5700U, 15.3 GiB RAM, Samsung MZVLB512HBJQ NVMe SSD, Windows 11 Home (plugged in), Temurin 25.0.5, rustc 1.99.1, rocksdbjni 11.8.1, JMH 1.37. Every row except the concurrent one comes from one run on commit `fce7797`; that one comes from a later run. One thread (8 in the concurrent row), one fork per row: `±` is the 99.9% interval over that row's 5 iterations, and separate runs have differed by up to about 20%. Numbers are rounded to two significant digits of their `±`; the exact values, the full conditions and two before/after comparisons are in [docs/benchmarks.md](docs/benchmarks.md). To reproduce: `cd service && ./gradlew jmh` (about 31 minutes; output in `service/build/results/jmh/results.txt`).

### Patina vs RocksDB

Patina runs its defaults: a 4 MiB memtable flushed on a background thread, and a full compaction above 4 tables on another. RocksDB runs with compression off and `WriteOptions.setSync` matching Patina, WAL on and everything else default (64 MiB write buffer, background leveled compaction, no bloom filter, default block cache). Keys are 8 bytes and values random. Writes start from an empty store and keep it from warmup (3 × 1 s) through measurement (5 × 2 s); each synced-write trial first waits 2 minutes, so it doesn't inherit the previous trial's disk work ([why](docs/benchmarks.md#why-synced-trials-pause-for-2-minutes)). Reads run on 48 MiB of preloaded data (466,033 keys at 100 B, 12,264 at 4 KiB) after reopening the engine.

ops/s, higher is better:

| Benchmark | Value | Sync | Patina | RocksDB |
|---|---|---|---:|---:|
| sequentialPut | 100 B | on | 872 ± 52 | 854.3 ± 9.7 |
| sequentialPut | 4 KiB | on | 811 ± 55 | 810 ± 85 |
| sequentialPut | 100 B | off | 241,000 ± 38,000 | 193,700 ± 2,100 |
| sequentialPut | 4 KiB | off | 16,000 ± 50,000 | 65,000 ± 28,000 |
| randomPut | 100 B | on | 854 ± 36 | 843 ± 55 |
| randomPut | 4 KiB | on | 814 ± 39 | 804 ± 35 |
| randomPut | 100 B | off | 225,000 ± 41,000 | 145,000 ± 13,000 |
| randomPut | 4 KiB | off | 18,000 ± 69,000 | 47,000 ± 33,000 |
| randomGet | 100 B | | 171,800 ± 3,300 | 167,100 ± 5,300 |
| randomGet | 4 KiB | | 155,900 ± 3,100 | 168,800 ± 6,500 |
| scan100 | 100 B | | 4,510 ± 160 | 16,210 ± 250 |
| scan100 | 4 KiB | | 2,623 ± 92 | 2,700 ± 140 |

Moving compaction to a background thread made unsynced writes 3.1–6.4× faster; moving the flush made no measurable difference ([details](docs/benchmarks.md#moving-compaction-off-the-write-path)).

### Concurrent synced writes

8 threads put random keys with 100 B values into one store, sync on, with the same settings, warmup, measurement and 2-minute pause as above. This is a later run, on the group-commit change; its single-threaded rows landed within error of the table above ([details](docs/benchmarks.md#group-commit)).

ops/s across all 8 threads, higher is better:

| Benchmark | Value | Sync | Patina | RocksDB |
|---|---|---|---:|---:|
| syncedPut8Threads | 100 B | on | 3,360 ± 110 | 4,120 ± 310 |

### FFM cost per call

One hot key in the memtable, read and written through `PatinaEngine` with sync off, against a `ConcurrentSkipListMap`. Warmup 10 × 2 s, measurement 5 × 2 s.

ns/op, lower is better:

| Operation | Value | Patina through FFM | `ConcurrentSkipListMap` |
|---|---|---:|---:|
| get | 100 B | 366.9 ± 5.9 | 7.22 ± 0.17 |
| get | 4 KiB | 819.9 ± 4.8 | 6.75 ± 0.16 |
| put, sync off | 100 B | 3,210 ± 420 | 10.70 ± 0.64 |
| put, sync off | 4 KiB | 12,700 ± 9,500 | 10.9 ± 1.2 |

A get crosses the boundary twice (`patina_get`, `patina_free`) and copies the value twice (out of the memtable, then into a `byte[]`): about 360 ns more than the skip list at 100 B and 810 ns at 4 KiB. A put adds a WAL write and, every 4 MiB of puts, a seal: a new WAL file, and the full memtable handed to the flush thread, after waiting for the last flush if it is still running.

### Why Patina loses

- **Unsynced 4 KiB writes, 2.6–4.1× slower.** Only one full memtable can wait for its flush, so writes go no faster than flushes, and a flush shares the disk with a full compaction that rewrites the whole store. Patina's iterations swing with it, from about 2,000 to 37,500 ops/s, so its error bars are wider than its scores. RocksDB fills a 64 MiB buffer before it flushes, 16× less often, and compacts level by level instead of rewriting everything. At 100 B Patina is 1.2× (sequential) to 1.6× (random) faster, outside error.
- **Single-threaded synced writes tie.** Both engines wait for one fsync per write, 1.1–1.2 ms here, which swamps everything else.
- **8-thread synced writes, 1.2× slower.** In the concurrent row's run, 8 threads gave Patina 3.9× its single-threaded randomPut rate and RocksDB 4.9×. This run doesn't show why.
- **100 B scans, 3.6× slower.** Patina reads each value with its own seek and read behind a per-table mutex, about 2.2 µs per entry; RocksDB reads blocks of many entries. The boundary isn't the cost: a scan crosses it twice, like a get, well under 1 µs (FFM table) of the 220 µs the scan takes. At 4 KiB the two tie.
- **Gets: tie at 100 B, 1.1× slower at 4 KiB.** Every key is in memory, so a get is one map lookup per memtable and one per table, then one read; the 48 MiB dataset most likely sat in the OS file cache. This run doesn't show why RocksDB pulls ahead at 4 KiB.

## Limitations

- Single node, one lock, held through each WAL fsync. Concurrent synced writers share an fsync, but a lone writer still pays one per write (about 800–880 synced writes/s here).
- Every key is in memory (a full index per SSTable), and open reads every SSTable end to end.
- Compaction always merges every table, so its cost grows with the store, and it needs disk space for a second copy of the data.
- One memtable at a time waits for its flush, so once flushes fall behind, for example while a full compaction shares the disk, writes run at flush speed.
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
