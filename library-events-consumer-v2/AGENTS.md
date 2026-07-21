# Agent Instructions — library-events-consumer-v2

## Project Overview

This is a **Spring Boot 4.1.0 / Java 25** Kafka consumer application. It consumes JSON events from the
Kafka topic `library-events`, validates them, and persists `LibraryEvent` and `Book` records to a
PostgreSQL database managed by Flyway.

There are **no REST controllers** — this is a pure event-driven consumer application.

---

## Build & Run

```bash
# Build (skipping tests)
./gradlew build -x test

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "com.learnkafka.consumer.LibraryEventsConsumerIntegrationTest"

# Start dependent services (PostgreSQL)
docker compose up -d

# Run the application
./gradlew bootRun
```

> Tests use **EmbeddedKafka** (no Docker needed for Kafka) and **Testcontainers** for PostgreSQL
> (Docker required). The `compose.yaml` is only needed for running the application locally.

---

## Architecture

```
Kafka topic: library-events
        │
        ▼
LibraryEventsConsumer          @KafkaListener — delegates immediately to service
        │
        ▼
LibraryEventService            Validates DTO (Bean Validation + conditional rules),
        │                      routes to handleAdd() or handleUpdate()
        ├── LibraryEventMapper  DTO ↔ Entity conversion
        └── LibraryEventRepository / BookRepository  → PostgreSQL
```

---

## Package Structure

```
com.learnkafka
├── config/         LibraryEventsConsumerConfig — @EnableKafka, ObjectMapper bean, KafkaListenerContainerFactory
├── consumer/       LibraryEventsConsumer       — @KafkaListener(topics = "library-events")
├── entity/         LibraryEvent, Book, LibraryEventType (enum)
├── mapper/         LibraryEventMapper          — toEntity(), updateEntity()
├── model/          LibraryEventDto, BookDto, EventType (enum)
├── repository/     LibraryEventRepository, BookRepository (JpaRepository)
└── service/        LibraryEventService         — @Transactional event processing
```

---

## Key Conventions

### Kafka Consumer
- The consumer class **only logs and delegates** to the service — no business logic.
- Use `ConsumerRecord<Long, LibraryEventDto>` as the listener method parameter.
- The `ConcurrentKafkaListenerContainerFactory` is configured in `LibraryEventsConsumerConfig`.
- Error handling is injected via `ObjectProvider<CommonErrorHandler>`.

### Service Layer
- `LibraryEventService.processEvent()` is the single entry point for all event types.
- Validate with `jakarta.validation.Validator` first, then apply conditional business rules.
- Use a `switch` expression on `EventType` to route `ADD` vs `UPDATE`.
- `ADD`: call `libraryEventMapper.toEntity()`, then `libraryEventRepository.save()`.
- `UPDATE`: look up the existing entity (throw `IllegalArgumentException` if not found), call `libraryEventMapper.updateEntity()`, then save.
- All processing is `@Transactional`.

### DTOs vs Entities
- **DTOs** live in `com.learnkafka.model` — used for Kafka deserialization and validation.
- **Entities** live in `com.learnkafka.entity` — mapped to PostgreSQL tables.
- Conversion is done exclusively in `LibraryEventMapper` — never convert inline in the service.
- `EventType` (DTO enum) mirrors `LibraryEventType` (entity enum); convert with `LibraryEventType.valueOf(dto.getEventType().name())`.

### Entities
- Use `@PrePersist` / `@PreUpdate` for `createdAt` / `updatedAt` audit columns — do not set them manually.
- `LibraryEvent` and `Book` have a **OneToOne** relationship:
  - `LibraryEvent` owns the inverse side (`mappedBy = "libraryEvent"`, `cascade = ALL`).
  - `Book` holds the foreign key column `library_event_id`.
- Entity IDs are `Integer`; DTO IDs are `Long` — cast with `.intValue()` in the mapper.

### Database Migrations (Flyway)
- All schema changes go in `src/main/resources/db/migration/` as `V{n}__{description}.sql`.
- Never use `ddl-auto: create` or `update` — only `validate`.
- Follow the existing migration chain: V1 (init schema) → V2 (remove serial) → V3 (audit columns).
- Next migration should be **V4**.

### Validation
- DTO fields use Bean Validation annotations (`@NotNull`, `@NotEmpty`, `@Positive`, `@Size`, `@Valid`).
- `LibraryEventService.validateDto()` runs the validator programmatically and throws `IllegalArgumentException` with a comma-separated violations message.
- Conditional rules (e.g., `UPDATE` requires non-null `libraryEventId`) are checked in `validateConditionalRules()` — add new business rules there.

### Lombok
- Entities and DTOs use `@Data`, `@Builder`, `@NoArgsConstructor`, `@AllArgsConstructor`.
- Service/consumer/mapper classes use constructor injection (no `@Autowired`).

---

## Testing Patterns

### Integration Tests
- Use `@SpringBootTest` + `@EmbeddedKafka` + `@Testcontainers`.
- Inject `KafkaTemplate<Long, LibraryEventDto>` to publish test messages.
- Use a polling helper (e.g., `await().atMost(...)`) to wait for async consumer processing before asserting on the database.
- Both `LibraryEventRepository` and `BookRepository` are available for assertions.

### Test Structure
- Place integration tests in `src/test/java/com/learnkafka/consumer/`.
- Test class naming: `*IntegrationTest.java`.
- Each test method should follow the pattern: **publish event → wait for DB record → assert**.

### What to Test
| Scenario | Expected outcome |
|---|---|
| ADD event with valid data | `LibraryEvent` + `Book` persisted |
| UPDATE event with valid data | Existing `LibraryEvent` + `Book` updated |
| ADD/UPDATE with null book | No record persisted; `IllegalArgumentException` thrown |
| UPDATE with null `libraryEventId` | No record persisted; `IllegalArgumentException` thrown |

---

## Dependencies Reference

| Category | Artifact |
|---|---|
| Kafka | `spring-boot-starter-kafka` |
| JPA | `spring-boot-starter-data-jpa` |
| Database | `org.postgresql:postgresql` |
| Migrations | `spring-boot-starter-flyway`, `flyway-database-postgresql` |
| Validation | `spring-boot-starter-validation` |
| Web | `spring-boot-starter-webmvc` |
| Lombok | `org.projectlombok:lombok` (compileOnly + annotationProcessor) |
| Jackson | `com.fasterxml.jackson.core:jackson-databind:2.20.2` |
| Test — Kafka | `spring-boot-starter-kafka-test` |
| Test — DB | `org.testcontainers:testcontainers-postgresql` |
| Test — Containers | `org.testcontainers:testcontainers-junit-jupiter` |

---

## Configuration

Key `application.yml` properties and their environment variable overrides:

| Property | Env var | Default |
|---|---|---|
| `server.port` | `SERVER_PORT` | `8081` |
| `spring.datasource.url` | `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/mydatabase` |
| `spring.datasource.username` | `SPRING_DATASOURCE_USERNAME` | `myuser` |
| `spring.datasource.password` | `SPRING_DATASOURCE_PASSWORD` | `secret` |
| `spring.kafka.consumer.bootstrap-servers` | — | `localhost:9092` |
| `spring.kafka.consumer.group-id` | `SPRING_KAFKA_CONSUMER_GROUP_ID` | `library-events-listener-group` |

Kafka value deserializer is `JsonDeserializer`. Trusted packages: `com.learnkafka.model`.
Default value type: `com.learnkafka.model.LibraryEventDto`. Type headers are disabled.

---

## Available Skills

This project has pre-configured Copilot skills — use `/skills` to activate them:

| Skill | Purpose |
|---|---|
| `kafka-consumer` | Adding new Kafka consumers, topics, and deserialization patterns |
| `testing` | Writing integration tests with EmbeddedKafka and Testcontainers |
| `db-migration` | Creating Flyway migration scripts |
| `controller` | Adding REST controllers (if the project ever expands to expose HTTP endpoints) |
