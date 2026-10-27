package dev.patina.api;

import static java.nio.charset.StandardCharsets.UTF_8;

import dev.patina.engine.PatinaEngine;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/items")
class ItemController {

    private final PatinaEngine engine;

    ItemController(PatinaEngine engine) {
        this.engine = engine;
    }

    @PutMapping("/{key}")
    ResponseEntity<Void> put(@PathVariable String key, @RequestBody byte[] value) {
        engine.put(key.getBytes(UTF_8), value);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{key}")
    ResponseEntity<byte[]> get(@PathVariable String key) {
        return engine.get(key.getBytes(UTF_8))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
