# Patina

A crash-safe key-value store: an LSM storage engine in Rust, embedded in a Java 25 Spring Boot service through the Foreign Function & Memory API (no JNI).

```
HTTP ──► service/ (Java, Spring Boot) ──FFM──► engine/ (Rust cdylib)
```

A test kills the writer process 120 times and no acknowledged write is lost. On one laptop, Patina is within error of RocksDB on fsynced writes and random gets, ranges from 1.34× faster (100-byte values) to 6.83× slower (4 KiB) on unsynced writes, and loses by 3.68× on 100-byte scans. [Results](#results) explains why.

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

**Write-ahead log** ([wal.rs](engine/src/wal.rs)). Every put and delete is appended to the current WAL file, `<n>.wal`, and then applied to the memtable (a `BTreeMap`), both under one write lock. Records are little-endian:

```
[payload_len u32][crc32 u32][payload]
payload = [op u8 = 0][key_len u32][key][value_len u32][value]   put
        | [op u8 = 1][key_len u32][key]                         delete
```

A record goes out in one write and is fsynced before the call returns (unless sync-writes is off). If the write or fsync fails, the call errors, the memtable is unchanged, and the WAL refuses further appends until reopened, because a partial record would hide everything written after it.

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

AMD Ryzen 7 5700U, 15.3 GiB RAM, Samsung MZVLB512HBJQ NVMe SSD, Windows 11 Home, Temurin 25.0.5, rustc 1.99.1, rocksdbjni 11.8.1, JMH 1.37. One thread, one fork per row: `±` is the 99.9% interval over the 5 iterations of a single run. Windows Defender real-time scanning was on. To reproduce: `cd service && ./gradlew jmh` (output in `service/build/results/jmh/results.txt`).

### Patina vs RocksDB

Patina runs its defaults: a 4 MiB memtable flushed on a background thread, and a full compaction above 4 tables on another. RocksDB runs with compression off and `WriteOptions.setSync` matching Patina, WAL on and everything else default (64 MiB write buffer, background leveled compaction, no bloom filter, default block cache). Keys are 8 bytes and values random. Writes start from an empty store and keep it from warmup (3 × 1 s) through measurement (5 × 2 s). Reads run on 48 MiB of preloaded data (466,033 keys at 100 B, 12,264 at 4 KiB) after reopening the engine.

The four sync-off write rows come from the latest session, after the flush moved to a background thread too ([below](#moving-the-flush-off-the-write-path)). The other rows come from the first session, when flush and compaction both ran inside the write, and weren't rerun. RocksDB ran 1.07–1.54× faster on the sync-off rows in the latest session, so compare within a row, not across sessions.

ops/s, higher is better:

| Benchmark | Value | Sync | Patina | RocksDB |
|---|---|---|---:|---:|
| sequentialPut | 100 B | on | 778.072 ± 119.367 | 760.180 ± 77.750 |
| sequentialPut | 4 KiB | on | 698.252 ± 271.792 | 736.886 ± 17.220 |
| sequentialPut | 100 B | off | 210,007.241 ± 78,657.850 | 185,487.377 ± 7,700.852 |
| sequentialPut | 4 KiB | off | 9,627.579 ± 28,738.826 | 65,753.715 ± 23,033.945 |
| randomPut | 100 B | on | 777.608 ± 34.341 | 719.605 ± 105.333 |
| randomPut | 4 KiB | on | 663.932 ± 317.690 | 724.062 ± 35.447 |
| randomPut | 100 B | off | 189,042.747 ± 95,124.994 | 141,006.824 ± 14,001.970 |
| randomPut | 4 KiB | off | 9,184.293 ± 31,091.089 | 45,818.784 ± 38,369.719 |
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

### Moving the flush off the write path

After that change the flush still ran inside the write that filled the memtable, under the lock. Its move was measured the same way, in a later session: the old engine, then the new one, back to back, with RocksDB as the control.

ops/s, higher is better:

| Benchmark | Value | Patina, inline flush | Patina, background flush | RocksDB, first run | RocksDB, second run |
|---|---|---:|---:|---:|---:|
| sequentialPut | 100 B | 168,188.591 ± 43,797.993 | 210,007.241 ± 78,657.850 | 183,646.657 ± 10,014.285 | 185,487.377 ± 7,700.852 |
| sequentialPut | 4 KiB | 13,823.364 ± 15,750.356 | 9,627.579 ± 28,738.826 | 63,597.134 ± 17,921.239 | 65,753.715 ± 23,033.945 |
| randomPut | 100 B | 162,636.775 ± 19,896.941 | 189,042.747 ± 95,124.994 | 126,685.903 ± 43,742.258 | 141,006.824 ± 14,001.970 |
| randomPut | 4 KiB | 15,688.221 ± 31,271.116 | 9,184.293 ± 31,091.089 | 42,414.805 ± 17,384.384 | 45,818.784 ± 38,369.719 |

At 100 B Patina got 16–25% faster, 4–24% with RocksDB's 1–11% drift divided out, inside the error bars. At 4 KiB the new engine scored 0.59–0.70× the old, but every 4 KiB error bar is wider than its score, and a run of the new engine just before the old one landed in between (10,694.497 and 12,351.587 ops/s): no measurable change.

To see where 4 KiB writes go, an instrumented copy of the engine (not in the repo) ran sequentialPut's puts directly from Rust for 13 s. Writes spent 9.3 s of it waiting for the previous memtable's flush. A memtable filled in about 35 ms at the 29,000 puts/s Patina reached between stalls, while a flush averaged 61.2 ms: 48.4 ms writing and fsyncing the table and 9.9 ms re-reading it. A full compaction ran during 195 of the 207 flushes, and its output grew from 21 MB to 842 MB. With at most one memtable sealed, writes can go no faster than flushes. At 100 B a memtable takes about 155 ms to fill and a flush averaged 53.8 ms, so writes waited 344 ms of the 13 s.

### FFM cost per call

One hot key in the memtable, read and written through `PatinaEngine` with sync off, against a `ConcurrentSkipListMap`. Warmup 10 × 2 s, measurement 5 × 2 s. Separate runs varied by up to about 20%, more than `±` shows.

ns/op, lower is better:

| Operation | Value | Patina through FFM | `ConcurrentSkipListMap` |
|---|---|---:|---:|
| get | 100 B | 445.889 ± 10.986 | 9.337 ± 1.325 |
| get | 4 KiB | 1,191.682 ± 62.078 | 9.096 ± 0.189 |
| put, sync off | 100 B | 3,907.308 ± 58.858 | 19.322 ± 0.286 |
| put, sync off | 4 KiB | 16,946.746 ± 896.255 | 19.533 ± 0.802 |

A get crosses the boundary twice (`patina_get`, `patina_free`) and copies the value twice (out of the memtable, then into a `byte[]`): 436.552 ns more than the skip list at 100 B, 1,182.586 ns at 4 KiB. A put adds a WAL write and, every 4 MiB of puts, a flush, which still ran inside the put when these rows were measured.

### Why Patina loses

- **Unsynced writes, 4.99–6.83× slower at 4 KiB.** At 100 B Patina is 1.13× faster (sequential, within error) to 1.34× faster (random). Flush and compaction both run on background threads now, but only one full memtable can wait for its flush, so writes go no faster than flushes. At 4 KiB the next memtable fills in about 35 ms while a flush takes 61.2 ms on average, sharing the disk with a full compaction that rewrites the whole store, 842 MB by the end of a 13 s run: writes spent 9.3 s of it waiting ([above](#moving-the-flush-off-the-write-path)). Iterations swing with the compaction, from 1,805.384 to 21,676.713 ops/s. RocksDB fills a 64 MiB buffer before it flushes, so it pays for a flush 16× less often; Patina's compaction rewrote 2,125 MB for the 869 MB written in that run. The gap was only 2.02–2.48× in the previous session: RocksDB has since run faster and Patina at 4 KiB hasn't. The engine with background compaction and the inline flush scored 13,211.166 and 13,502.171 ops/s then and 13,823.364 and 15,688.221 now, while RocksDB went from 32,726.657 and 27,271.705 to 63,597.134 and 42,414.805. A 100 B put now averages 4.76–5.29 µs, against 3.907 µs with nothing to flush (FFM table, measured in another session).
- **Synced writes tie.** Both engines wait for one fsync per write, 1.285–1.506 ms here, which swamps everything else.
- **100 B scans, 3.68× slower.** Patina reads each value with its own seek and read behind a per-table mutex, about 3 µs per entry; RocksDB reads blocks of many entries. At 4 KiB the bytes dominate and the two tie. The boundary isn't the cost: Patina returns 100 entries in one call, while RocksDB's iterator takes four JNI calls per entry.
- **Gets are within error.** Every key is in memory, so a get is one map lookup per memtable (two while a flush runs) and one per table, then one read. That was at most 5 lookups when these rows ran, with compaction inline; tables flushed during a background merge can now push it past 5. The 48 MiB dataset most likely sat in the OS file cache.

## Limitations

- Single node, single writer: one lock, one fsync per synced write, no group commit (about 660–780 synced writes/s here).
- Every key is in memory (a full index per SSTable), and open reads every SSTable end to end.
- Compaction always merges every table, so its cost grows with the store, and it needs disk space for a second copy of the data.
- One memtable at a time waits for its flush, so once flushes fall behind, as they do at 4 KiB while a compaction runs, writes run at flush speed.
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
