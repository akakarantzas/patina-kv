package dev.patina.engine;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PatinaEngineTest {

    private static final Path LIBRARY =
            Path.of("../engine/target/release", System.mapLibraryName("patina_engine")).toAbsolutePath();

    @TempDir Path dataDir;

    @Test
    void putThenGetCrossesIntoRust() {
        try (PatinaEngine engine = new PatinaEngine(LIBRARY, dataDir)) {
            engine.put("user#1".getBytes(UTF_8), "alice".getBytes(UTF_8));

            assertThat(engine.get("user#1".getBytes(UTF_8))).hasValueSatisfying(v ->
                    assertThat(new String(v, UTF_8)).isEqualTo("alice"));
            assertThat(engine.get("missing".getBytes(UTF_8))).isEmpty();
        }
    }

    @Test
    void writesSurviveReopen() {
        try (PatinaEngine engine = new PatinaEngine(LIBRARY, dataDir)) {
            engine.put("user#1".getBytes(UTF_8), "alice".getBytes(UTF_8));
        }
        try (PatinaEngine engine = new PatinaEngine(LIBRARY, dataDir)) {
            assertThat(engine.get("user#1".getBytes(UTF_8))).hasValueSatisfying(v ->
                    assertThat(new String(v, UTF_8)).isEqualTo("alice"));
        }
    }

    @Test
    void deleteCrossesIntoRustAndSurvivesReopen() {
        byte[] k = "user#1".getBytes(UTF_8);
        try (PatinaEngine engine = new PatinaEngine(LIBRARY, dataDir)) {
            engine.put(k, "alice".getBytes(UTF_8));
            engine.delete(k);
            assertThat(engine.get(k)).isEmpty();
            engine.delete("missing".getBytes(UTF_8)); // not an error
        }
        try (PatinaEngine engine = new PatinaEngine(LIBRARY, dataDir)) {
            assertThat(engine.get(k)).isEmpty();
        }
    }

    @Test
    void dataDirThatIsAFileFailsToOpen() throws IOException {
        Path file = Files.createFile(dataDir.resolve("not-a-dir"));

        assertThatIllegalStateException().isThrownBy(() -> new PatinaEngine(LIBRARY, file))
                .withMessageContaining(file.toString());
    }

    @Test
    void callsAfterCloseThrowInsteadOfCrashingTheJvm() {
        PatinaEngine engine = new PatinaEngine(LIBRARY, dataDir);
        engine.close();
        engine.close(); // idempotent

        assertThatIllegalStateException().isThrownBy(() -> engine.put(new byte[] {1}, new byte[] {2}))
                .withMessage("engine is closed");
        assertThatIllegalStateException().isThrownBy(() -> engine.get(new byte[] {1}))
                .withMessage("engine is closed");
        assertThatIllegalStateException().isThrownBy(() -> engine.delete(new byte[] {1}))
                .withMessage("engine is closed");
    }

    @Test
    void emptyValueRoundTrips() {
        try (PatinaEngine engine = new PatinaEngine(LIBRARY, dataDir)) {
            engine.put("k".getBytes(UTF_8), new byte[0]);
            assertThat(engine.get("k".getBytes(UTF_8))).hasValueSatisfying(v -> assertThat(v).isEmpty());
        }
    }
}
