package dev.patina.engine;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.Optional;

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
    private final MethodHandle free;
    private final MemorySegment handle;
    private boolean closed;

    public PatinaEngine(Path library) {
        SymbolLookup lib = SymbolLookup.libraryLookup(library, libraryArena);
        MethodHandle open = bind(lib, "patina_open", FunctionDescriptor.of(ADDRESS));
        close = bind(lib, "patina_close", FunctionDescriptor.ofVoid(ADDRESS));
        put = bind(lib, "patina_put", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG));
        get = bind(lib, "patina_get", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS));
        free = bind(lib, "patina_free", FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG));
        try {
            handle = (MemorySegment) open.invokeExact();
        } catch (Throwable t) {
            throw rethrow(t);
        }
        if (handle.equals(MemorySegment.NULL)) {
            throw new IllegalStateException("patina_open failed");
        }
    }

    public void put(byte[] key, byte[] value) {
        try (Arena call = Arena.ofConfined()) {
            int rc = (int) put.invokeExact(handle,
                    call.allocateFrom(JAVA_BYTE, key), (long) key.length,
                    call.allocateFrom(JAVA_BYTE, value), (long) value.length);
            check(rc, "put");
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    public Optional<byte[]> get(byte[] key) {
        try (Arena call = Arena.ofConfined()) {
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
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            close.invokeExact(handle);
        } catch (Throwable t) {
            throw rethrow(t);
        } finally {
            libraryArena.close();
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
