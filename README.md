# Patina

A key-value store: a storage engine in **Rust**, served by a **Java** (Spring Boot) API that calls the engine directly through Java's Foreign Function & Memory API (no JNI).
```
HTTP ──► service/ (Java 25, Spring Boot) ──FFM──► engine/ (Rust cdylib)
```

## Run

Needs Rust 1.85+ (`cargo`) and JDK 25. Gradle comes from the wrapper (`gradlew`). On Windows, Rust's default toolchain also needs the Visual Studio C++ Build Tools (the free "Build Tools" download, not the full IDE), and the commands below run in Git Bash.

```bash
cd service
./gradlew test      # builds the Rust engine, then runs Java tests against it
./gradlew bootRun   # API on http://localhost:8080
```

```bash
curl -X PUT localhost:8080/items/user1 -H "Content-Type: text/plain" -d alice
curl localhost:8080/items/user1
curl "localhost:8080/items?limit=50"
curl -X DELETE localhost:8080/items/user1
```

Keys are a single path segment, so no `/`; values can be up to 1 MiB, and bigger PUTs get 413. `GET /items` lists keys in byte order, up to `limit` per page (1–1000, default 50): `{"items":[{"key":"user1","value":"YWxpY2U="}],"next":null}`, with values in base64. When `next` isn't `null`, pass it back as `?after=` (URL-encoded) for the next page.

Data lives in `service/data/` (override with `./gradlew bootRun --args='--patina.data-dir=...'`); delete it for a clean start.

Rust tests alone: `cd engine && cargo test`.

## Contact

Questions, partnership inquiries, or feedback about this project:

- LinkedIn: [Apostolos Kakarantzas](https://www.linkedin.com/in/akakarantzas/)
- X / Twitter: [@aevagnarly](https://x.com/aevagnarly)
- Email: [hello@akakarantzas.com](mailto:hello@akakarantzas.com)
