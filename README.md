# Patina

A key-value store: a storage engine in **Rust**, served by a **Java** (Spring Boot) API that calls the engine directly through Java's Foreign Function & Memory API (no JNI).
```
HTTP ──► service/ (Java 25, Spring Boot) ──FFM──► engine/ (Rust cdylib)
```

## Run

Needs Rust (`cargo`) and JDK 25. Gradle comes from the wrapper (`gradlew`). On Windows without Visual Studio, use Rust's GNU toolchain: `rustup default stable-x86_64-pc-windows-gnu`.

```bash
cd service
./gradlew test      # builds the Rust engine, then runs Java tests against it
./gradlew bootRun   # API on http://localhost:8080
```

```bash
curl -X PUT localhost:8080/items/user1 -H "Content-Type: text/plain" -d alice
curl localhost:8080/items/user1
```

Rust tests alone: `cd engine && cargo test`.

## Contact

Questions, partnership inquiries, or feedback about this project:

- LinkedIn: [Apostolos Kakarantzas](https://www.linkedin.com/in/akakarantzas/)
- X / Twitter: [@aevagnarly](https://x.com/aevagnarly)
- Email: [hello@akakarantzas.com](mailto:hello@akakarantzas.com)
