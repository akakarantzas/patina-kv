package dev.patina.bench;

import dev.patina.engine.PatinaEngine;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
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

/**
 * What one Java -> Rust call costs: get and put of one hot key held in Patina's memtable, through PatinaEngine,
 * against a ConcurrentSkipListMap holding the same key and value in plain Java. One thread, nanoseconds per call.
 *
 * <p>A Patina get crosses the boundary twice (patina_get, then patina_free for Rust's copy of the value) and
 * copies the value into a new byte[]; the skip list returns its stored array. So getPatina minus getSkipList
 * approximates the boundary cost plus copying the value out.
 *
 * <p>Patina put runs with sync_writes off: each put writes its WAL record to the OS, without fsync. Overwrites
 * count toward the 4 MiB memtable limit, so every ~4 MiB of puts also flushes a one-entry SSTable (that flush
 * does fsync), and once more than 4 SSTables exist the flush also compacts; that cost is amortized into the put number.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 2) // getPatina drops from ~630 to ~470 ns/op only after about 10 s
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class FfmOverhead {

    @Param({"100", "4096"}) public int valueSize;
    final byte[] key = PatinaVsRocksDb.key(42);
    final ConcurrentSkipListMap<byte[], byte[]> skipList = new ConcurrentSkipListMap<>(Arrays::compareUnsigned);
    byte[] value;
    Path dir;
    PatinaEngine patina;

    @Setup(Level.Trial)
    public void open() throws IOException {
        value = PatinaVsRocksDb.randomBytes(valueSize);
        dir = Files.createTempDirectory("patina-bench");
        patina = new PatinaEngine(Path.of(System.getProperty("patina.library")), dir, false);
        patina.put(key, value);
        // A copy: Arrays.compareUnsigned returns early when both arguments are the same array, which would flatter the skip list.
        skipList.put(key.clone(), value);
    }

    @TearDown(Level.Trial)
    public void close() throws IOException {
        patina.close();
        PatinaVsRocksDb.deleteRecursively(dir);
    }

    @Benchmark
    public void getPatina(Blackhole bh) {
        bh.consume(patina.get(key).orElseThrow());
    }

    @Benchmark
    public void getSkipList(Blackhole bh) {
        bh.consume(skipList.get(key));
    }

    @Benchmark
    public void putPatina() {
        patina.put(key, value);
    }

    @Benchmark
    public void putSkipList() {
        skipList.put(key, value);
    }
}
