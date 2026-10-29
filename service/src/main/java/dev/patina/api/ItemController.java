package dev.patina.api;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.patina.engine.PatinaEngine;
import java.io.IOException;
import java.io.InputStream;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/items")
class ItemController {

    private static final int MAX_VALUE_BYTES = 1_048_576;

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
}
