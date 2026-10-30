package dev.patina;

import dev.patina.engine.PatinaEngine;
import java.nio.file.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class PatinaApplication {

    public static void main(String[] args) {
        SpringApplication.run(PatinaApplication.class, args);
    }

    /**
     * Defaults to the cargo release build next to this project and ./data in the working dir;
     * override with --patina.library=... and --patina.data-dir=...
     * --patina.sync-writes=false skips the WAL sync per write: acked writes then survive a process crash, not power loss.
     */
    @Bean(destroyMethod = "close")
    PatinaEngine patinaEngine(@Value("${patina.library:}") String library,
                              @Value("${patina.data-dir:data}") String dataDir,
                              @Value("${patina.sync-writes:true}") boolean syncWrites) {
        Path path = library.isBlank()
                ? Path.of("../engine/target/release", System.mapLibraryName("patina_engine"))
                : Path.of(library);
        return new PatinaEngine(path.toAbsolutePath(), Path.of(dataDir).toAbsolutePath(), syncWrites);
    }
}
