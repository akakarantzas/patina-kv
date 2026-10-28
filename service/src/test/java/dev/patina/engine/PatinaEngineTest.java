package dev.patina.engine;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class PatinaEngineTest {

    private static final Path LIBRARY =
            Path.of("../engine/target/release", System.mapLibraryName("patina_engine")).toAbsolutePath();

    @Test
    void putThenGetCrossesIntoRust() {
        try (PatinaEngine engine = new PatinaEngine(LIBRARY)) {
            engine.put("user#1".getBytes(UTF_8), "alice".getBytes(UTF_8));

            assertThat(engine.get("user#1".getBytes(UTF_8))).hasValueSatisfying(v ->
                    assertThat(new String(v, UTF_8)).isEqualTo("alice"));
            assertThat(engine.get("missing".getBytes(UTF_8))).isEmpty();
        }
    }

    @Test
    void callsAfterCloseThrowInsteadOfCrashingTheJvm() {
        PatinaEngine engine = new PatinaEngine(LIBRARY);
        engine.close();
        engine.close(); // idempotent

        assertThatIllegalStateException().isThrownBy(() -> engine.put(new byte[] {1}, new byte[] {2}))
                .withMessage("engine is closed");
        assertThatIllegalStateException().isThrownBy(() -> engine.get(new byte[] {1}))
                .withMessage("engine is closed");
    }

    @Test
    void emptyValueRoundTrips() {
        try (PatinaEngine engine = new PatinaEngine(LIBRARY)) {
            engine.put("k".getBytes(UTF_8), new byte[0]);
            assertThat(engine.get("k".getBytes(UTF_8))).hasValueSatisfying(v -> assertThat(v).isEmpty());
        }
    }
}
