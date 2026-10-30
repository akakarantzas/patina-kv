# Patina

A key-value store: a storage engine in **Rust**, served by a **Java** (Spring Boot) API that calls the engine directly through Java's Foreign Function & Memory API (no JNI).
```
HTTP ──► service/ (Java 25, Spring Boot) ──FFM──► engine/ (Rust cdylib)
```

## Run

Needs Rust 1.85+ (`cargo`) and JDK 25. Gradle comes from the wrapper (`gradlew`). On Windows, Rust's default toolchain also needs the Visual Studio C++ Build Tools (the free "Build Tools" download, not the full IDE), and the commands below run in Git Bash.

```bash
cd service
./gradlew test      # builds the Rust engine, then runs Java tests against it
./gradlew bootRun   # API on http://localhost:8080
```

| Request | Response |
|---|---|
| `PUT /items/{key}` with the value as the body | `204`; `413` if the value is over 1 MiB |
| `GET /items/{key}` | `200` with the value's bytes, or `404` |
| `DELETE /items/{key}` | `204`, whether or not the key existed |
| `GET /items?after=&limit=` | one page of keys after `after`, in byte order |

```bash
curl -X PUT localhost:8080/items/user1 -H "Content-Type: text/plain" -d alice
curl -X PUT localhost:8080/items/user2 -H "Content-Type: text/plain" -d bob
curl localhost:8080/items/user1                    # alice
curl "localhost:8080/items?limit=1"                # {"items":[{"key":"user1","value":"YWxpY2U="}],"next":"user1"}
curl "localhost:8080/items?after=user1&limit=1"    # {"items":[{"key":"user2","value":"Ym9i"}],"next":null}
curl -X DELETE localhost:8080/items/user1
```

Keys are a single path segment, so no `/`. Values are raw bytes, so `GET /items` returns them in base64. `after` is exclusive and optional (omit it for the first page); `limit` is 1–1000, default 50, and anything else gets 400. When `next` isn't `null`, pass it back as `?after=` (URL-encoded) for the next page. A page stops early at about 4 MiB of keys and values, so keep paging until `next` is `null` rather than until a page is short.

Data lives in `service/data/` (override with `./gradlew bootRun --args='--patina.data-dir=...'`); delete it for a clean start.

Every write is fsynced before it is acknowledged. `--args='--patina.sync-writes=false'` skips that fsync: writes get much faster (see [Results](#results)), but an acknowledged write then survives only a crash of the process, not power loss or an OS crash.

Rust tests alone: `cd engine && cargo test`.

## Design

**Why two languages:** embed a native storage engine in the JVM, the way Kafka Streams and Flink embed RocksDB. Rust owns all storage logic. Java is a thin HTTP service that validates requests and calls the engine.

### Write-ahead log ([wal.rs](engine/src/wal.rs))

Every put and delete is appended to `wal.log` before it reaches the memtable (an in-memory `BTreeMap`). Each record, little-endian:

```
[payload_len u32][crc32 u32 of payload][payload]

payload = [op u8 = 0][key_len u32][key][value_len u32][value]   put
        | [op u8 = 1][key_len u32][key]                         delete
```

**fsync before ack.** The record is built in one buffer, written with one `write_all`, and, unless `patina.sync-writes` is off, `sync_data` (fsync) runs before put or delete returns. The WAL and the memtable sit behind the same write lock, so log order is memtable order. A record over 4 MiB is rejected before anything is written. If the write or the fsync fails, the memtable is unchanged, the write returns an error, and the WAL refuses every later append until the engine is reopened (`failed_append_blocks_later_appends`): the failure may have left a partial record, recovery stops there, so anything appended after it would be lost.

**Recovery.** On open, records are read until the first one that is incomplete or fails a check (length over 4 MiB, crc mismatch, unknown op, leftover bytes). The file is truncated there and synced, so new appends never land after garbage. `torn_tail_at_every_length` cuts a log at every byte offset and checks that exactly the complete records come back. The kill test ([crash.rs](engine/tests/crash.rs)) hard-kills a writer process 120 times (100 with sync on, 20 with sync off) and checks after each kill that every acknowledged write is still there.

### Flush ([lib.rs](engine/src/lib.rs) `flush`)

Once the key and value bytes written since the last flush pass 4 MiB, the write that crossed the limit flushes the memtable, still under the write lock, in this order:

1. Write `<seq>.sst.tmp` and fsync it.
2. Rename it to `<seq>.sst`.
3. fsync the directory, on Unix only (Rust's standard library can't open a directory on Windows).
4. Truncate `wal.log` to 0 bytes and fsync it.
5. Clear the memtable.

A crash at any point loses nothing:
- Before the rename, the WAL still holds every record, and the next open deletes the leftover `.tmp` (`stray_tmp_file_is_deleted_on_open_and_never_read`).
- Between the rename and the WAL truncate, the data is in both places. Replay rebuilds exactly the newest table's entries into the memtable, which is read first, so the duplicate is harmless (`crash_between_rename_and_wal_reset_replays_harmlessly`).
- After the truncate, the table alone holds the data, and it was fsynced in step 1.

A failed flush doesn't fail the write that triggered it, because that write is already in the WAL; the next write retries the flush (`failed_flush_keeps_the_write_and_retries_on_the_next_one`). The kill test uses a 100-byte memtable limit and asserts that at least 5 of its kills landed mid-flush or mid-compaction.

An SSTable ([sstable.rs](engine/src/sstable.rs)) holds sorted entries `[key_len u32][key][kind u8: value or tombstone][value_len u32][value]`, then a footer `[entry_count u64][compacted_through u64][crc32 u32][magic u32]`. Opening one reads it end to end, checks the magic, crc and count, and keeps every key in memory with its value's file offset; values are read from disk only when asked for. A corrupt table makes open fail with an error naming the file (tested with a flipped byte at every position and a cut at every length). A get checks the memtable, then the SSTables newest first, and the first entry found wins, so a tombstone hides any older value.

### Compaction ([lib.rs](engine/src/lib.rs) `compact`)

When a flush leaves more than 4 SSTables, the same write merges all of them into one: a k-way merge over the tables' sorted keys where the newest table wins per key, reading one value at a time so the values are never all in memory. Tombstones are dropped, which is safe because every table is an input, so no older value is left for them to hide. The memtable is not an input (it is still in the WAL) and is newer than every table. The output goes through the same `.tmp` → fsync → rename → fsync-directory steps as a flush, and its footer records `compacted_through`, the highest input seq. Only after that are the inputs closed and deleted.

**No resurrected deletes.** A crash after the rename but before every input is deleted leaves old tables behind that may still hold values whose tombstones the merge dropped. So open, before reading anything, finds the highest `compacted_through` and deletes every table whose seq is at or below it (`crash_before_compaction_deletes_never_resurrects_a_deleted_key`, which restores either all the inputs or only the oldest one). If an input fails to read mid-merge, the short output is never renamed and nothing is deleted (`read_error_mid_merge_renames_nothing_and_deletes_nothing`).

### The boundary ([lib.rs](engine/src/lib.rs), [PatinaEngine.java](service/src/main/java/dev/patina/engine/PatinaEngine.java))

Seven C functions:

| Function | Returns |
|---|---|
| `patina_open(path, path_len, sync_writes)` | an engine handle, or null with the reason on stderr |
| `patina_close(engine)` | nothing |
| `patina_put(engine, key, key_len, value, value_len)` | `OK` or `ERR` |
| `patina_get(engine, key, key_len, out, out_len)` | `OK`, `NOT_FOUND` or `ERR` |
| `patina_delete(engine, key, key_len)` | `OK` (also for a missing key) or `ERR` |
| `patina_scan(engine, start, start_len, limit, out, out_len)` | `OK` or `ERR` |
| `patina_free(buf, len)` | nothing |

- **No panics into the JVM.** Unwinding into Java is undefined behaviour, so every function above except `patina_free` runs its body inside `catch_unwind` and turns a panic into `ERR` (null from `patina_open`; `patina_close` swallows it). `patina_free` only drops a byte buffer, which can't panic.
- **Whoever allocates a buffer frees it.** Java copies the path, keys and values into a confined `Arena` that is freed when the call returns; Rust only reads them during the call and copies whatever it keeps. Rust allocates the result buffers of `get` and `scan`; Java copies them into Java memory and hands them back to `patina_free` in a `finally`. `patina_open` allocates the engine handle and `patina_close` frees it.
- **No use after close.** Every Java call holds a read lock and checks a `closed` flag; `close()` takes the write lock, so the handle is never freed under a running call, and a call after `close()` throws `IllegalStateException` instead of crashing the JVM (`callsAfterCloseThrowInsteadOfCrashingTheJvm`).
- **Validation at the edge.** The controller reads at most 1 MiB + 1 byte of a PUT body and answers 413 before anything reaches Rust (`oneByteOverIsRejectedBeforeReachingRust`).

### Stateless scans and paging

`patina_scan(start, limit)` returns the live keys at or after `start` in byte order, merged across the memtable and every SSTable (newest wins, tombstones skipped), in one buffer that the caller frees with `patina_free`: `[count u32][more u8]`, then `count` × `[key_len u32][key][value_len u32][value]`. It stops at `limit` entries (clamped to 1–1000) or at about 4 MiB of keys and values, but always returns at least one entry if one exists. `more` is true only if another live key follows the last one returned. The whole scan runs under one read lock and nothing survives the call: Rust keeps no state between calls except the engine handle, so there is no iterator or cursor to expire or leak.

The client does the paging. `GET /items?after=k` appends one `0x00` byte to `k` (the smallest key greater than `k`) and scans from there, and `next` is the last key returned. Each page is consistent on its own, but the pages together are not a snapshot: a key written between two pages shows up if it sorts after the cursor and not if it sorts before (`keysWrittenBetweenPagesAppearOnlyAfterTheCursor`).

## Results

One machine, one run per benchmark. Treat the numbers as rough.

**Machine:** AMD Ryzen 7 5700U, 15.3 GiB RAM, Samsung MZVLB512HBJQ NVMe SSD, Windows 11 Home. **Software:** Temurin JDK 25.0.5, rustc 1.99.1 (release build), rocksdbjni 11.8.1, JMH 1.37 through the me.champeau.jmh 0.7.4 Gradle plugin. Windows Defender real-time scanning of `%TEMP%`, where every benchmark directory lives, was left on. One thread; one fork (a fresh JVM) per row, so each row is a single run, and `±` is JMH's 99.9% confidence interval over that run's 5 measurement iterations.

To reproduce (it runs both benchmarks and writes `service/build/results/jmh/results.txt`):

```bash
cd service
./gradlew jmh
```

### Patina vs RocksDB ([PatinaVsRocksDb.java](service/src/jmh/java/dev/patina/bench/PatinaVsRocksDb.java))

Settings, as listed in the benchmark's header comment:
- **Patina:** `Options::default()`: a 4 MiB memtable limit, and a full compaction once a flush leaves more than 4 SSTables, both run inside the write that triggers them. `sync_writes` follows the Sync column. Patina has no compression, bloom filters or block cache.
- **RocksDB:** `setCreateIfMissing(true)`; `setCompressionType(NO_COMPRESSION)`, because Patina doesn't compress; `WriteOptions.setSync(sync)`, matching Patina, with the WAL on (the default), so both engines log every write before acking it. Everything else is the default, including a 64 MiB write buffer, leveled compaction on background threads, no bloom filter, the default block cache, and default `ReadOptions`.

Workloads: keys are 8-byte big-endian longs, values are random bytes. Each trial starts in a fresh temp dir, with warmup 3 × 1 s and measurement 5 × 2 s.
- `sequentialPut` writes increasing keys and `randomPut` random 64-bit keys, starting from an empty store. The store carries over from warmup into measurement, so later iterations write into a bigger store.
- `randomGet` reads a random existing key, and `scan100` reads 100 entries from one. Both run against 48 MiB of key and value bytes (12 × Patina's memtable limit: 466,033 keys at 100 B, 12,264 at 4 KiB), preloaded with sync off, after which the engine is closed and reopened, so reads start from disk. The benchmark checks that RocksDB's memtable is empty after the reopen; Patina's holds at most the last unflushed 4 MiB. Reads have no Sync column because sync only affects writes.

Throughput in ops/s, higher is better:

| Benchmark | Value | Sync | Patina | RocksDB |
|---|---|---|---:|---:|
| sequentialPut | 100 B | on | 778.072 ± 119.367 | 760.180 ± 77.750 |
| sequentialPut | 4 KiB | on | 698.252 ± 271.792 | 736.886 ± 17.220 |
| sequentialPut | 100 B | off | 35653.526 ± 40439.842 | 123009.580 ± 69722.154 |
| sequentialPut | 4 KiB | off | 1953.610 ± 1387.942 | 55704.503 ± 4679.584 |
| randomPut | 100 B | on | 777.608 ± 34.341 | 719.605 ± 105.333 |
| randomPut | 4 KiB | on | 663.932 ± 317.690 | 724.062 ± 35.447 |
| randomPut | 100 B | off | 32695.540 ± 46488.145 | 91641.273 ± 57680.752 |
| randomPut | 4 KiB | off | 1698.491 ± 2555.243 | 42955.454 ± 12979.818 |
| randomGet | 100 B | | 119601.057 ± 34748.958 | 130243.174 ± 5217.127 |
| randomGet | 4 KiB | | 117198.350 ± 13191.961 | 130032.778 ± 11034.726 |
| scan100 | 100 B | | 3271.111 ± 138.891 | 12030.384 ± 1346.870 |
| scan100 | 4 KiB | | 1971.409 ± 416.063 | 1980.491 ± 462.473 |

### Per-call FFM cost ([FfmOverhead.java](service/src/jmh/java/dev/patina/bench/FfmOverhead.java))

Get and put of one hot key held in Patina's memtable, through `PatinaEngine`, against a `ConcurrentSkipListMap` holding the same key and value in plain Java. Patina runs with sync off. Average time per call, warmup 10 × 2 s (the Patina get keeps getting faster for about 10 s), measurement 5 × 2 s.

Nanoseconds per call, lower is better:

| Operation | Value | Patina through FFM | `ConcurrentSkipListMap` |
|---|---|---:|---:|
| get | 100 B | 445.889 ± 10.986 | 9.337 ± 1.325 |
| get | 4 KiB | 1191.682 ± 62.078 | 9.096 ± 0.189 |
| put, sync off | 100 B | 3907.308 ± 58.858 | 19.322 ± 0.286 |
| put, sync off | 4 KiB | 16946.746 ± 896.255 | 19.533 ± 0.802 |

Separate runs of this benchmark differed by up to about 20%, more than `±` suggests: `±` only covers the iterations inside one run.

A Patina get crosses the boundary twice (`patina_get`, then `patina_free`), allocates a confined arena for the key, takes two read locks (Java's close guard and the engine's), and copies the value twice: Rust clones it out of the memtable, then Java copies it into a new `byte[]`. The skip list just returns the array it holds. So the difference, 436.552 ns at 100 B and 1182.586 ns at 4 KiB, approximates the boundary cost plus copying the value out, and it grows with the value.

A put also copies the key and value into native memory, takes the write lock, and writes one WAL record to the OS. Writes to one key count toward the 4 MiB limit too, so every ~38,800 puts of 100 B, or every ~1,020 of 4 KiB, also flush a one-entry SSTable and empty the WAL, with an fsync for each. How the extra 13 µs at 4 KiB splits between the bigger copies, the bigger WAL writes and the more frequent flushes was not measured.

### Why Patina loses where it loses

**Sync-off writes.** Comparing means, RocksDB is 2.80× and 3.45× faster at 100 B (random, sequential) and 25.29× and 28.51× faster at 4 KiB. Patina flushes every 4 MiB, where RocksDB's write buffer is 64 MiB, and Patina flushes and compacts inside the write that triggers it, under the write lock, where RocksDB does both on background threads. Each of Patina's full compactions rewrites every live entry, reading each value with its own seek and read. These workloads never write the same key twice, so the store grows all trial and each compaction is bigger than the last: in the per-iteration output, 100 B sequential put fell from about 50k to about 29k ops/s within its trial. That is why Patina's error bars are as wide as its scores, and why a longer run would give a lower number. Without that work a 100 B put costs 3907.308 ns (the FFM table above, where every flush and compaction holds one key); here the average is 28.05 to 30.59 µs per put (1 / 35653.526 and 1 / 32695.540 ops/s). RocksDB's 100 B sync-off results also have wide error bars; that wasn't investigated.

**Sync-on writes are a tie.** Every pair of intervals overlaps. Each write in both engines waits for one fsync of its WAL, and each one takes 1.285 to 1.506 ms here (1 / ops/s), against 3.907 µs for the same 100 B put without the fsync (FFM table). At 100 B a 13-second trial writes about 1 MiB, so neither engine even flushes. At 4 KiB it writes about 35 MiB, so Patina flushes about 8 times and compacts once inside writes, which likely explains its wider error bars at 4 KiB.

**Scans of 100 B entries: RocksDB is 3.68× faster; at 4 KiB they tie.** Patina's merge finds each next key by checking every source (the memtable and up to 4 tables), then reads the value of each key that lives in an SSTable with an index lookup, a seek and a read, behind a per-table mutex: two system calls per entry, about 3 µs per entry in total here. RocksDB's iterator walks data blocks that each hold many small entries. The boundary is not the cost: one Patina scan returns all 100 entries in one call, while the benchmark makes four JNI calls per entry into RocksDB's iterator (`isValid`, `key`, `value`, `next`). At 4 KiB, moving the value bytes likely outweighs the per-entry overhead for both engines.

**Random gets are within error.** The intervals overlap at both value sizes. Every SSTable keeps all its keys in memory, so a Patina get is at most 5 in-memory lookups (the memtable and up to 4 tables) plus one seek and read for the value. The 48 MiB dataset fits easily in 15.3 GiB of RAM, so these reads most likely came from the OS file cache, not the SSD. Patina's wide 100 B interval (± 34748.958) wasn't investigated.

## Limitations

Every `shortcut:` comment in the code is here, along with the limits of the design itself.

- **Single node.** No replication and nothing distributed.
- **Single writer.** One lock covers the WAL and the memtable, so writes run one at a time. With sync on, each waits for its own fsync (no group commit), which caps sync-on writes at about 660–780 per second on this machine.
- **Every key is in memory.** Each SSTable keeps a full key index ([sstable.rs](engine/src/sstable.rs); a sparse index would fix it), and opening the engine reads every SSTable end to end to build those indexes. Memory and startup time grow with the number of keys.
- **Full compaction blocks reads and writes.** It runs inside a write, under the write lock, and rewrites every live entry with one seek and read per value ([lib.rs](engine/src/lib.rs) `compact`; a background thread would fix the stall). It also needs disk space for a second copy of the live data until the inputs are deleted.
- **No multi-process locking.** There is no lock file, so two processes opening the same data dir corrupt each other's WAL ([lib.rs](engine/src/lib.rs) `Engine::open`).
- **Sync off only survives process crashes.** With `patina.sync-writes=false`, an acked write has reached the OS but not the disk, so power loss or an OS crash can lose it. The kill test runs 20 iterations this way.
- **Power loss is argued, not tested.** The kill test proves process crashes. Power-loss safety rests on fsync before ack, the torn-tail test, and the flush and compaction order. On Windows the directory fsyncs are skipped, so whether a new WAL file or an SSTable rename survives power loss there is up to NTFS.
- **WAL corruption mid-log drops the rest of the log.** Recovery truncates at the first bad record. That is right for a torn tail, the only damage a crash should leave, but a flipped bit in the middle would silently drop every later record too ([wal.rs](engine/src/wal.rs)).
- **A failed WAL write stops all writes** until the engine is reopened (see [Write-ahead log](#write-ahead-log-walrs)).
- **Reads of one SSTable take turns.** Each table's file sits behind a mutex for its seek and read ([sstable.rs](engine/src/sstable.rs); positional reads would fix it).
- **The merge checks every source for each key,** O(sources) per key. That's fine with at most 5 sources; more tables would need a heap ([lib.rs](engine/src/lib.rs) `merge`).
- **The WAL is read into memory whole on open.** Flushes keep it near 4 MiB ([wal.rs](engine/src/wal.rs)).
- **64-bit only.** Java maps Rust's `usize` to `long` ([PatinaEngine.java](service/src/main/java/dev/patina/engine/PatinaEngine.java)).
- **No bloom filters or leveled compaction** (both cut in [PLAN.md](PLAN.md)), **and no block cache or compression.**

## Contact

Questions, partnership inquiries, or feedback about this project:

- LinkedIn: [Apostolos Kakarantzas](https://www.linkedin.com/in/akakarantzas/)
- X / Twitter: [@aevagnarly](https://x.com/aevagnarly)
- Email: [hello@akakarantzas.com](mailto:hello@akakarantzas.com)
