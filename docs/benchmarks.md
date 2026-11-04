# Benchmarks

The raw numbers behind the README's [Results](../README.md#results), how they were measured, why synced-write trials pause, the group-commit run, and two before/after comparisons from the engine's history.

## Setup

- **Machine:** AMD Ryzen 7 5700U, 15.3 GiB RAM, Samsung MZVLB512HBJQ NVMe SSD, Windows 11 Home, plugged in, Balanced power plan. Windows Defender real-time scanning was on, and other apps were open. Windows' power log shows no sleep or standby during the run.
- **Software:** Temurin 25.0.5, rustc 1.99.1, rocksdbjni 11.8.1, JMH 1.37.
- **Code:** commit `fce7797`. The engine is unchanged since `9383766`; `fce7797` only adds the pause described below.
- **Run:** `cd service && ./gradlew jmh`, about 27 minutes. JMH writes `service/build/results/jmh/results.txt`.
- **Statistics:** one thread, one fork (a fresh JVM) per row. `±` is JMH's 99.9% confidence interval over the 5 measurement iterations of that fork. It doesn't cover differences between separate runs, which have reached about 20%.

Every engine setting is listed in the README and, one by one, in the header comment of [PatinaVsRocksDb.java](../service/src/jmh/java/dev/patina/bench/PatinaVsRocksDb.java).

## Exact results

[results-fce7797.txt](results-fce7797.txt) is JMH's own output for the run, unrounded: every Patina vs RocksDB row except the concurrent one, and the FFM table.

## Why synced trials pause for 2 minutes

Each synced-write trial waits 2 minutes before it starts. Without the pause the results depend on run order. A heavy trial writes and deletes gigabytes, and for tens of seconds after it ends the SSD is still absorbing them, so every fsync waits. JMH runs trials in a fixed order, and RocksDB's synced trials always come straight after Patina's 4 KiB unsynced trial, the heaviest one. A full run without the pause measured RocksDB's synced writes at 310.720–711.788 ops/s against Patina's 782.948–853.954.

Running a synced trial right after that heavy trial shows it hits both engines:

| Synced 100 B writes, ops/s | Warmup (3 × 1 s) | Measurement (5 × 2 s) |
|---|---|---|
| Patina | 809.622, 846.600, 849.135 | 462.617, 18.852, 517.677, 794.547, 838.095 |
| RocksDB | 313.741, 94.409, 16.191 | 17.939, 363.383, 825.854, 814.646, 806.375 |

With the pause, every synced iteration in the run, warmups included, landed between 777.210 and 893.933 ops/s.

## Group commit

Measured on the group-commit change (the commit after `d226628`) in one full `./gradlew jmh` run on 2026-11-04 from 02:14 to 02:45, on the same machine, plugged in, with the OneDrive client not running when the run started. Windows' power log has no sleep or standby events in that window. The run adds `syncedPut8Threads`, named so it sorts last and the other trials keep their order.

| syncedPut8Threads, 100 B, ops/s | Score | Iterations, warmups included |
|---|---:|---|
| Patina | 3,360.400 ± 105.312 | 3,320.819–3,493.373 |
| RocksDB | 4,119.725 ± 311.067 | 3,877.519–4,254.413 |

Every single-threaded synced iteration in the run, warmups included, landed between 767.010 and 905.926 ops/s. Patina's single-threaded synced rows landed within error of the `fce7797` run: sequentialPut 878.758 ± 54.644 (100 B) and 804.667 ± 65.639 (4 KiB), randomPut 862.082 ± 59.114 (100 B) and 811.543 ± 52.142 (4 KiB). A lone writer still pays one fsync per write.

## Moving compaction off the write path

Measured while developing [PR #1](https://github.com/akakarantzas/patina-kv/pull/1). Compaction used to run inside the write that triggered it, under the lock. To measure the change, the sync-off write benchmarks ran on the old engine and then on the new one, back to back on the same machine. RocksDB is the control: its code didn't change.

ops/s, higher is better:

| Benchmark | Value | Patina, inline compaction | Patina, background compaction | RocksDB, first run | RocksDB, second run |
|---|---|---:|---:|---:|---:|
| sequentialPut | 100 B | 19,820.125 ± 19,582.858 | 85,181.635 ± 21,522.764 | 63,945.807 ± 8,537.997 | 76,329.952 ± 5,476.861 |
| sequentialPut | 4 KiB | 2,007.568 ± 2,126.176 | 13,211.166 ± 18,218.807 | 29,080.694 ± 2,955.431 | 32,726.657 ± 1,554.723 |
| randomPut | 100 B | 22,478.147 ± 27,246.041 | 84,194.343 ± 12,744.043 | 47,279.888 ± 10,000.114 | 57,334.793 ± 11,277.550 |
| randomPut | 4 KiB | 1,913.188 ± 3,273.246 | 13,502.171 ± 11,700.699 | 24,686.308 ± 2,272.613 | 27,271.705 ± 4,094.875 |

Patina got 3.75–7.06× faster. RocksDB also ran 10–21% faster in the second run, so part of that is the machine; divided out, the gain is 3.09–6.39×. An earlier run of the new engine landed within error of these numbers. The inline numbers from the first benchmark run, on commit `274ed87` (35,653.526, 1,953.610, 32,695.540 and 1,698.491 ops/s), aren't comparable: RocksDB ran 1.74–1.94× faster in that run than in the first run here.

## Moving the flush off the write path

Measured while developing [PR #2](https://github.com/akakarantzas/patina-kv/pull/2). After that change the flush still ran inside the write that filled the memtable, under the lock. It was measured the same way: the old engine, then the new one, back to back, with RocksDB as the control.

ops/s, higher is better:

| Benchmark | Value | Patina, inline flush | Patina, background flush | RocksDB, first run | RocksDB, second run |
|---|---|---:|---:|---:|---:|
| sequentialPut | 100 B | 168,188.591 ± 43,797.993 | 210,007.241 ± 78,657.850 | 183,646.657 ± 10,014.285 | 185,487.377 ± 7,700.852 |
| sequentialPut | 4 KiB | 13,823.364 ± 15,750.356 | 9,627.579 ± 28,738.826 | 63,597.134 ± 17,921.239 | 65,753.715 ± 23,033.945 |
| randomPut | 100 B | 162,636.775 ± 19,896.941 | 189,042.747 ± 95,124.994 | 126,685.903 ± 43,742.258 | 141,006.824 ± 14,001.970 |
| randomPut | 4 KiB | 15,688.221 ± 31,271.116 | 9,184.293 ± 31,091.089 | 42,414.805 ± 17,384.384 | 45,818.784 ± 38,369.719 |

At 100 B Patina got 16–25% faster, 4–24% with RocksDB's 1–11% drift divided out, inside the error bars. At 4 KiB the new engine scored 0.59–0.70× the old, but every 4 KiB error bar is wider than its score, and a run of the new engine just before the old one landed in between (10,694.497 and 12,351.587 ops/s): no measurable change.
