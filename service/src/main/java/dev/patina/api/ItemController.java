package dev.patina.api;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.patina.engine.PatinaEngine;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/items")
class ItemController {

    private static final int MAX_VALUE_BYTES = 1_048_576;
    private static final int MAX_SCAN_LIMIT = 1000;

    private final PatinaEngine engine;

    ItemController(PatinaEngine engine) {
        this.engine = engine;
    }

    // Read from the stream, not @RequestBody byte[], so an oversized body is never fully buffered.
    @PutMapping("/{key}")
    ResponseEntity<Void> put(@PathVariable String key, InputStream body) throws IOException {
        byte[] value = body.readNBytes(MAX_VALUE_BYTES + 1);
        if (value.length > MAX_VALUE_BYTES) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).build();
        }
        engine.put(key.getBytes(UTF_8), value);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/{key}")
    ResponseEntity<Void> delete(@PathVariable String key) {
        engine.delete(key.getBytes(UTF_8));
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{key}")
    ResponseEntity<byte[]> get(@PathVariable String key) {
        return engine.get(key.getBytes(UTF_8))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // Jackson writes byte[] as base64.
    record Item(String key, byte[] value) {}

    record Page(List<Item> items, String next) {}

    /** Pages through keys in byte order: pass the previous page's next back as ?after= (exclusive). */
    @GetMapping
    ResponseEntity<Page> scan(@RequestParam(required = false) String after,
                              @RequestParam(defaultValue = "50") int limit) {
        if (limit < 1 || limit > MAX_SCAN_LIMIT) {
            return ResponseEntity.badRequest().build();
        }
        // after + 0x00 is the smallest key greater than after, so the scan (inclusive) starts just past it.
        byte[] start = after == null ? new byte[0] : (after + "\0").getBytes(UTF_8);
        PatinaEngine.ScanPage page = engine.scan(start, limit);
        List<Item> items = page.items().stream().map(e -> new Item(new String(e.key(), UTF_8), e.value())).toList();
        return ResponseEntity.ok(new Page(items, page.more() ? items.getLast().key() : null));
    }
}
