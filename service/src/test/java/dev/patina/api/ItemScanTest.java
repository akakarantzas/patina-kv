package dev.patina.api;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.patina.engine.PatinaEngine;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** GET /items paging against the real engine, so cursor semantics are checked through Rust. */
class ItemScanTest {

    private static final Path LIBRARY =
            Path.of("../engine/target/release", System.mapLibraryName("patina_engine")).toAbsolutePath();

    @TempDir Path dataDir;
    private PatinaEngine engine;
    private MockMvc mvc;

    @BeforeEach
    void open() {
        engine = new PatinaEngine(LIBRARY, dataDir);
        mvc = MockMvcBuilders.standaloneSetup(new ItemController(engine)).build();
    }

    @AfterEach
    void close() {
        engine.close();
    }

    @Test
    void pagingWithNextReturnsEveryKeyOnceInOrder() throws Exception {
        List<String> keys = IntStream.range(0, 250).mapToObj("k%03d"::formatted).toList();
        keys.forEach(this::put);

        List<String> seen = new ArrayList<>();
        List<Integer> pageSizes = new ArrayList<>();
        String after = null;
        do {
            JsonNode page = page(after, 100);
            pageSizes.add(page.get("items").size());
            for (JsonNode item : page.get("items")) {
                seen.add(item.get("key").asText());
                assertThat(Base64.getDecoder().decode(item.get("value").asText())).isEqualTo(value(item.get("key").asText()));
            }
            after = page.get("next").isNull() ? null : page.get("next").asText();
        } while (after != null && seen.size() <= keys.size()); // a cursor that never advances must fail, not hang

        assertThat(seen).isEqualTo(keys);
        assertThat(pageSizes).containsExactly(100, 100, 50);
    }

    @Test
    void keysWrittenBetweenPagesAppearOnlyAfterTheCursor() throws Exception {
        List.of("a", "c", "e").forEach(this::put);
        JsonNode first = page(null, 2);
        assertThat(keys(first)).containsExactly("a", "c");
        assertThat(first.get("next").asText()).isEqualTo("c");

        put("b"); // before the cursor
        put("d"); // after it
        JsonNode second = page("c", 2);

        assertThat(keys(second)).containsExactly("d", "e");
        assertThat(second.get("next").isNull()).isTrue();
    }

    @Test
    void afterExcludesThatKeyButNotLongerKeysStartingWithIt() throws Exception {
        List.of("a", "ab", "b").forEach(this::put);

        assertThat(keys(page("a", 10))).containsExactly("ab", "b");
    }

    @Test
    void emptyStoreReturnsNoItemsAndNoNext() throws Exception {
        JsonNode page = page(null, 50);

        assertThat(page.get("items").isArray()).isTrue();
        assertThat(page.get("items")).isEmpty();
        assertThat(page.get("next").isNull()).isTrue();
    }

    private JsonNode page(String after, int limit) throws Exception {
        MockHttpServletRequestBuilder request = get("/items").param("limit", String.valueOf(limit));
        if (after != null) {
            request.param("after", after);
        }
        String body = mvc.perform(request).andExpect(status().isOk()).andReturn().getResponse().getContentAsString(UTF_8);
        return new ObjectMapper().readTree(body);
    }

    private static List<String> keys(JsonNode page) {
        List<String> keys = new ArrayList<>();
        page.get("items").forEach(item -> keys.add(item.get("key").asText()));
        return keys;
    }

    private void put(String key) {
        engine.put(key.getBytes(UTF_8), value(key));
    }

    // A leading 0xFF byte is never valid UTF-8, so the value needs base64.
    private static byte[] value(String key) {
        return ("ÿ" + key).getBytes(ISO_8859_1);
    }
}
