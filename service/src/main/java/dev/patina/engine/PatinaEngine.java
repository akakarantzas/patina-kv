package dev.patina.engine;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Java side of the Rust engine's C ABI (engine/src/lib.rs), called through the Foreign Function & Memory API.
 * shortcut: maps Rust usize to JAVA_LONG, so 64-bit platforms only.
 */
public final class PatinaEngine implements AutoCloseable {

    // Status codes shared with engine/src/lib.rs.
    private static final int OK = 0;
    private static final int NOT_FOUND = 1;

    private final Arena libraryArena = Arena.ofShared();
    private final MethodHandle close;
    private final MethodHandle put;
    private final MethodHandle get;
    private final MethodHandle delete;
    private final MethodHandle scan;
    private final MethodHandle free;
    private final MemorySegment handle;
    // Calls hold the read lock for the whole native call; close takes the write lock, so the handle is never freed mid-call.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private boolean closed;

    public PatinaEngine(Path library, Path dataDir) {
        SymbolLookup lib = SymbolLookup.libraryLookup(library, libraryArena);
        MethodHandle open = bind(lib, "patina_open", FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG));
        close = bind(lib, "patina_close", FunctionDescriptor.ofVoid(ADDRESS));
        put = bind(lib, "patina_put", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG));
        get = bind(lib, "patina_get", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS));
        delete = bind(lib, "patina_delete", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG));
        scan = bind(lib, "patina_scan", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, JAVA_INT, ADDRESS, ADDRESS));
        free = bind(lib, "patina_free", FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG));
        byte[] path = dataDir.toString().getBytes(UTF_8);
        try (Arena call = Arena.ofConfined()) {
            handle = (MemorySegment) open.invokeExact(call.allocateFrom(JAVA_BYTE, path), (long) path.length);
        } catch (Throwable t) {
            throw rethrow(t);
        }
        if (handle.equals(MemorySegment.NULL)) {
            libraryArena.close();
            throw new IllegalStateException("patina_open failed for data dir " + dataDir + " (reason on stderr)");
        }
    }

    public void put(byte[] key, byte[] value) {
        lock.readLock().lock();
        try (Arena call = Arena.ofConfined()) {
            checkOpen();
            int rc = (int) put.invokeExact(handle,
                    call.allocateFrom(JAVA_BYTE, key), (long) key.length,
                    call.allocateFrom(JAVA_BYTE, value), (long) value.length);
            check(rc, "put");
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Succeeds whether or not the key exists. */
    public void delete(byte[] key) {
        lock.readLock().lock();
        try (Arena call = Arena.ofConfined()) {
            checkOpen();
            int rc = (int) delete.invokeExact(handle, call.allocateFrom(JAVA_BYTE, key), (long) key.length);
            check(rc, "delete");
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Optional<byte[]> get(byte[] key) {
        lock.readLock().lock();
        try (Arena call = Arena.ofConfined()) {
            checkOpen();
            MemorySegment out = call.allocate(ADDRESS);
            MemorySegment outLen = call.allocate(JAVA_LONG);
            int rc = (int) get.invokeExact(handle, call.allocateFrom(JAVA_BYTE, key), (long) key.length, out, outLen);
            if (rc == NOT_FOUND) {
                return Optional.empty();
            }
            check(rc, "get");
            long len = outLen.get(JAVA_LONG, 0);
            MemorySegment buf = out.get(ADDRESS, 0);
            try {
                return Optional.of(buf.reinterpret(len).toArray(JAVA_BYTE));
            } finally {
                free.invokeExact(buf, len); // Rust allocated it, so Rust frees it
            }
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            lock.readLock().unlock();
        }
    }

    public record Entry(byte[] key, byte[] value) {}

    public record ScanPage(List<Entry> items, boolean more) {}

    /** Live keys >= start in byte order, at most limit (Rust clamps it to 1..1000); more is true if another key follows. */
    public ScanPage scan(byte[] start, int limit) {
        lock.readLock().lock();
        try (Arena call = Arena.ofConfined()) {
            checkOpen();
            MemorySegment out = call.allocate(ADDRESS);
            MemorySegment outLen = call.allocate(JAVA_LONG);
            int rc = (int) scan.invokeExact(handle, call.allocateFrom(JAVA_BYTE, start), (long) start.length, limit, out, outLen);
            check(rc, "scan");
            long len = outLen.get(JAVA_LONG, 0);
            MemorySegment buf = out.get(ADDRESS, 0);
            try {
                // [count u32][more u8] then count x [key_len u32][key][value_len u32][value], little-endian
                ByteBuffer b = buf.reinterpret(len).asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                int count = b.getInt();
                boolean more = b.get() != 0;
                List<Entry> items = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    byte[] key = field(b);
                    items.add(new Entry(key, field(b)));
                }
                return new ScanPage(items, more);
            } finally {
                free.invokeExact(buf, len); // Rust allocated it, so Rust frees it
            }
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            lock.readLock().unlock();
        }
    }

    private static byte[] field(ByteBuffer b) {
        byte[] bytes = new byte[b.getInt()];
        b.get(bytes);
        return bytes;
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            try {
                close.invokeExact(handle);
            } finally {
                libraryArena.close();
            }
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("engine is closed");
        }
    }

    private static MethodHandle bind(SymbolLookup lib, String name, FunctionDescriptor descriptor) {
        MemorySegment symbol = lib.find(name).orElseThrow(() -> new IllegalStateException("missing symbol " + name));
        return Linker.nativeLinker().downcallHandle(symbol, descriptor);
    }

    private static void check(int rc, String op) {
        if (rc != OK) {
            throw new IllegalStateException("patina " + op + " failed with code " + rc);
        }
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException r) return r;
        if (t instanceof Error e) throw e;
        return new IllegalStateException(t);
    }
}
