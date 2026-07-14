# AGENTS.md — Library Events Producer v2

Instructions for AI agents working in this repository.

---

## Project Overview

A Spring Boot 4.1.0 REST API that publishes library events (`ADD`, `UPDATE`) to Apache Kafka.
- **Language:** Java 25
- **Build tool:** Gradle (wrapper: `./gradlew`)
- **Kafka:** 3-broker KRaft cluster via Docker Compose (`compose.yaml`)
- **Default port:** `8080`

---

## Build & Test Commands

```bash
# Compile the project
./gradlew build

# Run all tests (unit + integration)
./gradlew test

# Run the application (requires Kafka brokers to be running)
./gradlew bootRun

# Start Kafka brokers (required for integration tests and local run)
docker compose up -d

# Stop Kafka brokers
docker compose down
```

> Integration tests use `@EmbeddedKafka` — no running broker is needed for `./gradlew test`.

---

## Repository Structure

```
src/
├── main/java/com/arshad/
│   ├── controller/          # REST endpoints (LibraryEventController)
│   ├── service/             # Business logic (LibraryEventServiceImpl)
│   ├── producer/            # Kafka publisher (LibraryEventProducer)
│   ├── model/               # LibraryEvent, Book, EventType
│   ├── validator/           # Custom @ValidEventType annotation + validator
│   ├── exception/           # Global error handler, custom exceptions, ErrorResponse
│   └── config/              # OpenAPI/Swagger config
├── main/resources/
│   ├── application.yml      # Base config (active profile: dev)
│   ├── application-dev.yml  # Dev overrides (localhost Kafka)
│   ├── application-stage.yml
│   └── application-prod.yml
└── test/java/com/arshad/
    ├── controller/          # Controller unit tests + integration tests
    ├── service/             # Service unit tests
    ├── producer/            # Producer unit tests
    └── model/               # Validation and serialization tests

compose.yaml                 # 3-broker KRaft Kafka cluster
build.gradle                 # Dependencies and build config
docs/                        # PRD, implementation plan, test docs
```

---

## API Endpoints

| Method | Path | Description | Success |
|--------|------|-------------|---------|
| `POST` | `/v1/library-events` | Publish ADD event | `201 Created` |
| `PUT`  | `/v1/library-events` | Publish UPDATE event | `200 OK` |

---

## API Documentation (Swagger / OpenAPI)

This project uses [springdoc-openapi](https://springdoc.org/) (`springdoc-openapi-starter-webmvc-ui:2.8.9`) to auto-generate interactive API docs.

| Resource | URL |
|----------|-----|
| Swagger UI | `http://localhost:8080/swagger-ui.html` |
| OpenAPI JSON spec | `http://localhost:8080/v3/api-docs` |

**API metadata** (configured in `com.arshad.config.OpenApiConfig`):
- **Title:** Library Events Producer API
- **Version:** 1.0.0
- **Description:** API for publishing library events to Kafka.

**springdoc config** (in `application.yml`):
```yaml
springdoc:
  api-docs:
    path: /v3/api-docs
  swagger-ui:
    path: /swagger-ui.html
```

> The application must be running (`./gradlew bootRun`) to access the Swagger UI and API docs.
> To update API metadata, edit `src/main/java/com/arshad/config/OpenApiConfig.java`.

---

## Key Conventions

### Code Style
- Use **Lombok** (`@Slf4j`, `@RequiredArgsConstructor`, `@Data`, etc.) — avoid manual boilerplate.
- Validate inputs with **Bean Validation** (`@Valid`, `@NotNull`, `@Positive`, etc.).
- Log at `INFO` for successful publishes, `ERROR` for failures — always include `libraryEventId` and `eventType` in log messages.
- Controller methods return `CompletableFuture<ResponseEntity<T>>` for async Servlet dispatch.

### Kafka
- Topic name is read from `spring.kafka.topic.name` (default: `library-events`).
- Message key is `libraryEventId` (`Long`) to preserve per-event partition ordering.
- Use `sendLibraryEvent(...)` (async) for controller flows; `sendLibraryEventSynchronous(...)` (3s timeout) for synchronous needs.
- Producer is configured for idempotent delivery (`acks=all`, `enable.idempotence=true`).

### Error Handling
- All exceptions flow through `LibraryEventsControllerAdvice`.
- Invalid payloads → `400 Bad Request` with structured `ErrorResponse`.
- Kafka publish failures → `503 Service Unavailable`.
- Never swallow exceptions silently; always log with context.

### Testing
- **Unit tests:** Mock `KafkaTemplate` — no broker needed.
- **Integration tests:** Annotate with `@EmbeddedKafka` — assert actual messages in topic.
- Use `AssertJ` for assertions, `Mockito` for mocks.
- Place unit tests under `src/test/java/com/arshad/<package>/` mirroring the main package.

---

## What to Avoid

- Do **not** modify files under `build/`, `.gradle/`, `.idea/`.
- Do **not** commit secrets or real broker credentials.
- Do **not** add persistence (no database) — this service is intentionally stateless.
- Do **not** implement auth/authorization — out of scope.
- Do **not** change the `CLUSTER_ID` in `compose.yaml` — it must stay consistent across the 3 brokers.

---

## Adding New Features

1. Add/update model in `com.arshad.model`.
2. Add validation constraints directly on the model using Bean Validation annotations.
3. Update service interface (`LibraryEventService`) and implementation (`LibraryEventServiceImpl`).
4. Expose via controller, keeping the `CompletableFuture<ResponseEntity<T>>` return pattern.
5. Write unit test (mock `KafkaTemplate`) and integration test (`@EmbeddedKafka`).
6. Run `./gradlew test` before committing — all tests must pass.
