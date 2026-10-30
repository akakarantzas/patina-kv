package dev.patina.bench;

import dev.patina.engine.PatinaEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

/**
 * Patina vs RocksDB, each through its Java API: one thread, a fresh JVM (fork) and a fresh temp dir per trial.
 * Keys are 8-byte big-endian longs, so byte order is numeric order; values are random bytes of valueSize.
 *
 * <p>Patina settings: Options::default() from engine/src/lib.rs, that is a 4 MiB memtable limit and a full
 * compaction, on a background thread the triggering write starts, once there are more than 4 SSTables; sync_writes = sync.
 * Patina has no compression, no bloom filters and no block cache.
 *
 * <p>RocksDB settings, every one this benchmark sets:
 * <ul>
 *   <li>Options.setCreateIfMissing(true)
 *   <li>Options.setCompressionType(NO_COMPRESSION), because Patina doesn't compress
 *   <li>WriteOptions.setSync(sync), the same as Patina's sync_writes. The WAL stays on (the default), so both
 *       engines log every write before acking it.
 * </ul>
 * Everything else is the RocksDB default, including the 64 MiB write buffer, leveled compaction on background
 * threads, no bloom filter, the default block cache, and default ReadOptions.
 *
 * <p>Writes run with sync on and off. Reads have no sync param: sync only affects writes.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
public class PatinaVsRocksDb {

    static final int KEY_BYTES = 8;
    static final int SCAN = 100;

    @State(Scope.Benchmark)
    public static class Writes {
        @Param({"patina", "rocksdb"}) public String engine;
        @Param({"true", "false"}) public boolean sync;
        @Param({"100", "4096"}) public int valueSize;
        Path dir;
        Store store;
        byte[] value;
        long next;

        @Setup(Level.Trial)
        public void open() throws Exception {
            dir = Files.createTempDirectory("patina-bench");
            store = Store.open(engine, dir, sync);
            value = randomBytes(valueSize);
        }

        @TearDown(Level.Trial)
        public void close() throws Exception {
            store.close();
            deleteRecursively(dir);
        }
    }

    @State(Scope.Benchmark)
    public static class Reads {
        @Param({"patina", "rocksdb"}) public String engine;
        @Param({"100", "4096"}) public int valueSize;
        /** Key and value bytes preloaded: 12x Patina's 4 MiB memtable limit. A param only so it prints next to the results. */
        @Param("48") public int datasetMiB;
        Path dir;
        Store store;
        long keys;

        @Setup(Level.Trial)
        public void preload() throws Exception {
            dir = Files.createTempDirectory("patina-bench");
            keys = ((long) datasetMiB << 20) / (KEY_BYTES + valueSize);
            byte[] value = randomBytes(valueSize);
            // Sync off: it only affects writes, and a sync-on preload takes minutes.
            try (Store load = Store.open(engine, dir, false)) {
                for (long i = 0; i < keys; i++) {
                    load.put(key(i), value);
                }
            }
            // Reopened, so reads start from disk: Patina replays at most one memtable from its WAL,
            // and RocksDB flushes its recovered WAL to an SSTable.
            store = Store.open(engine, dir, false);
            if (store instanceof Rocks rocks && !rocks.db.getProperty("rocksdb.num-entries-active-mem-table").equals("0")) {
                throw new IllegalStateException("RocksDB kept the preload in its memtable");
            }
        }

        @TearDown(Level.Trial)
        public void close() throws Exception {
            store.close();
            deleteRecursively(dir);
        }
    }

    @Benchmark
    public void sequentialPut(Writes w) throws Exception {
        w.store.put(key(w.next++), w.value);
    }

    @Benchmark
    public void randomPut(Writes w) throws Exception {
        w.store.put(key(ThreadLocalRandom.current().nextLong()), w.value);
    }

    @Benchmark
    public byte[] randomGet(Reads r) throws Exception {
        return r.store.get(key(ThreadLocalRandom.current().nextLong(r.keys)));
    }

    @Benchmark
    public void scan100(Reads r, Blackhole bh) throws Exception {
        int n = r.store.scan(key(ThreadLocalRandom.current().nextLong(r.keys - SCAN)), SCAN, bh);
        if (n != SCAN) {
            throw new IllegalStateException("scan returned " + n + " entries");
        }
    }

    interface Store extends AutoCloseable {
        void put(byte[] key, byte[] value) throws Exception;

        /** Throws if the key is missing, so a broken preload can't pass as fast misses. */
        byte[] get(byte[] key) throws Exception;

        /** Reads up to limit entries from start into bh and returns how many it read. */
        int scan(byte[] start, int limit, Blackhole bh) throws Exception;

        static Store open(String engine, Path dir, boolean sync) throws Exception {
            return switch (engine) {
                case "patina" -> new Patina(new PatinaEngine(Path.of(System.getProperty("patina.library")), dir, sync));
                case "rocksdb" -> new Rocks(dir, sync);
                default -> throw new IllegalArgumentException(engine);
            };
        }
    }

    record Patina(PatinaEngine engine) implements Store {
        public void put(byte[] key, byte[] value) {
            engine.put(key, value);
        }

        public byte[] get(byte[] key) {
            return engine.get(key).orElseThrow();
        }

        public int scan(byte[] start, int limit, Blackhole bh) {
            List<PatinaEngine.Entry> items = engine.scan(start, limit).items();
            for (PatinaEngine.Entry e : items) {
                bh.consume(e.key());
                bh.consume(e.value());
            }
            return items.size();
        }

        public void close() {
            engine.close();
        }
    }

    static final class Rocks implements Store {
        final Options options = new Options().setCreateIfMissing(true).setCompressionType(CompressionType.NO_COMPRESSION);
        final WriteOptions writeOptions;
        final RocksDB db;

        Rocks(Path dir, boolean sync) throws RocksDBException {
            writeOptions = new WriteOptions().setSync(sync);
            db = RocksDB.open(options, dir.toString());
        }

        public void put(byte[] key, byte[] value) throws RocksDBException {
            db.put(writeOptions, key, value);
        }

        public byte[] get(byte[] key) throws RocksDBException {
            byte[] value = db.get(key);
            if (value == null) {
                throw new IllegalStateException("missing key");
            }
            return value;
        }

        public int scan(byte[] start, int limit, Blackhole bh) throws RocksDBException {
            try (RocksIterator it = db.newIterator()) {
                int n = 0;
                for (it.seek(start); it.isValid() && n < limit; it.next(), n++) {
                    bh.consume(it.key());
                    bh.consume(it.value());
                }
                it.status();
                return n;
            }
        }

        public void close() {
            db.close();
            writeOptions.close();
            options.close();
        }
    }

    static byte[] key(long i) {
        return ByteBuffer.allocate(KEY_BYTES).putLong(i).array();
    }

    static byte[] randomBytes(int n) {
        byte[] bytes = new byte[n];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    static void deleteRecursively(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
