# Library Events Producer v2 - Implementation Plan

## Overview
Implement a Spring Boot 4 / Java 25 REST API that publishes library events (ADD/UPDATE) to Apache Kafka. 11 phases organized by architectural layers with clear dependencies.

## Architecture Layers
```
┌─────────────────────────────────────┐
│    REST Controller Layer             │  Phase 06: REST Endpoints
├─────────────────────────────────────┤
│    Error Handling Layer              │  Phase 07: Error Responses
├─────────────────────────────────────┤
│    Service/Business Logic Layer      │  Phase 04: LibraryEventService
├─────────────────────────────────────┤
│    Producer Wrapper Layer            │  Phase 05: LibraryEventProducer
├─────────────────────────────────────┤
│    Kafka Configuration Layer         │  Phase 02: KafkaProducerConfig
├─────────────────────────────────────┤
│    Validation Layer                  │  Phase 03: Input Validation
├─────────────────────────────────────┤
│    Domain/Model Layer                │  Phase 01: Models & DTOs
└─────────────────────────────────────┘
    + Observability (Phase 10)
    + Testing (Phases 08-09)
    + Documentation (Phase 11)
```

## Implementation Phases

### Phase 01: Domain Models & DTOs ✓ DEPENDENCY ANCHOR
**Status:** Pending  
**Duration:** ~1-2 hours  
**Dependencies:** None

**Deliverables:**
- `src/main/java/com/arshad/model/LibraryEvent.java` — Main event DTO with libraryEventId, eventType, book
- `src/main/java/com/arshad/model/Book.java` — Book DTO with bookId, bookName, bookAuthor
- `src/main/java/com/arshad/model/EventType.java` — Enum: ADD, UPDATE
- Include Lombok annotations (@Data, @AllArgsConstructor, @NoArgsConstructor) for reduced boilerplate

**Key Decisions:**
- Use DTOs (not JPA entities, no persistence needed)
- Enum for type-safety
- Lombok for cleaner code

**Files to Create:**
```
src/main/java/com/arshad/model/
├── LibraryEvent.java
├── Book.java
└── EventType.java
```

**Acceptance Criteria:**
- All classes compile with Lombok
- Serialization/deserialization to JSON works
- Enums are correctly defined

---

### Phase 02: Kafka Producer Configuration
**Status:** Pending  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 01 (models)

**Deliverables:**
- `src/main/java/com/arshad/config/KafkaProducerConfig.java` — KafkaTemplate bean, producer configs
- `src/main/resources/application.yml` — Kafka bootstrap servers, topic name, producer settings
- Configure idempotent producer (enable.idempotence=true, acks=all, retries=3)

**Key Decisions:**
- Use Spring KafkaTemplate for high-level abstraction
- Idempotent producer to prevent duplicates
- Externalize config via YAML properties file
- Topic name: `library-events`

**Files to Create/Modify:**
```
src/main/java/com/arshad/config/
└── KafkaProducerConfig.java

src/main/resources/
└── application.yml (update/create)
```

**Application.yml Configuration:**
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        linger.ms: 10

kafka:
  topic:
    name: library-events
```

**Acceptance Criteria:**
- KafkaTemplate bean available for injection
- Producer properties configured correctly
- Application starts without Kafka errors (graceful if broker unavailable for now)

---

### Phase 03: Input Validation
**Status:** Pending  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 01 (models)

**Deliverables:**
- Add `@NotNull`, `@NotEmpty`, `@Positive`, `@Size` annotations to models
- `src/main/java/com/arshad/validator/EventTypeValidator.java` — Custom validator for eventType enum
- Validation groups (if needed for different endpoints)

**Key Decisions:**
- Use Spring Validation (JSR-380) for declarative validation
- Custom validator for eventType to ensure ADD for POST, UPDATE for PUT
- Fail fast on validation errors

**Files to Create/Modify:**
```
src/main/java/com/arshad/model/
├── LibraryEvent.java (add @Valid annotations)
├── Book.java (add @Valid annotations)
└── EventType.java

src/main/java/com/arshad/validator/
└── EventTypeValidator.java (custom validator if needed)
```

**Validation Rules:**
- libraryEventId: @NotNull, @Positive
- eventType: @NotNull, valid enum (ADD or UPDATE)
- book: @NotNull, @Valid (cascade validation)
- bookId: @NotNull, @Positive
- bookName: @NotNull, @NotEmpty, @Size(max=255)
- bookAuthor: @NotNull, @NotEmpty, @Size(max=255)

**Acceptance Criteria:**
- Validation works on all fields
- Custom eventType validator enforces business rules
- Tests verify validation messages are clear

---

### Phase 04: LibraryEventService (Business Logic)
**Status:** Pending  
**Duration:** ~2-3 hours  
**Dependencies:** Phase 01 (models), Phase 02 (config)

**Deliverables:**
- `src/main/java/com/arshad/service/LibraryEventService.java` — Service interface with add() and update() methods
- `src/main/java/com/arshad/service/LibraryEventServiceImpl.java` — Implementation with Kafka publishing logic
- Inject KafkaTemplate and handle success/error cases

**Key Decisions:**
- Service layer orchestrates business logic
- Service calls producer to send events
- Service returns published event confirmation
- No database calls; pure event publishing

**Files to Create:**
```
src/main/java/com/arshad/service/
├── LibraryEventService.java (interface)
└── LibraryEventServiceImpl.java (implementation)
```

**Methods:**
```java
LibraryEvent publishAddEvent(LibraryEvent event) throws KafkaException;
LibraryEvent publishUpdateEvent(LibraryEvent event) throws KafkaException;
```

**Acceptance Criteria:**
- Service injects KafkaTemplate correctly
- Methods return published event
- Exceptions bubble up for controller handling
- Unit tests mock KafkaTemplate

---

### Phase 05: LibraryEventProducer Wrapper
**Status:** Pending  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 02 (config), Phase 04 (service)

**Deliverables:**
- `src/main/java/com/arshad/producer/LibraryEventProducer.java` — Wrapper around KafkaTemplate
- Handles serialization, error logging, callback handling
- Encapsulates idempotent producer semantics

**Key Decisions:**
- Producer wrapper decouples service from KafkaTemplate details
- Use SendResult callback to log success/failure
- Throw checked exception on publish failure for controller to catch
- Key: String (libraryEventId); Value: JSON (LibraryEvent)

**Files to Create:**
```
src/main/java/com/arshad/producer/
└── LibraryEventProducer.java
```

**Methods:**
```java
void sendLibraryEvent(LibraryEvent event) throws KafkaException;
```

**Acceptance Criteria:**
- Sends event to Kafka topic
- Logs success with event ID and topic
- Logs and throws exception on failure
- Callback captures success/error cases

---

### Phase 06: REST Controller (Endpoints)
**Status:** Pending  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 04 (service)

**Deliverables:**
- `src/main/java/com/arshad/controller/LibraryEventController.java` — REST endpoints
- POST /v1/library-events — Publish ADD event (returns 201 Created)
- PUT /v1/library-events — Publish UPDATE event (returns 200 OK)
- Proper HTTP status codes and Location header for 201

**Key Decisions:**
- Base path: /v1/library-events
- Use @PostMapping and @PutMapping
- Validate with @Valid annotation
- Return ResponseEntity with appropriate status and body
- No request ID param in URL (body-only)

**Files to Create:**
```
src/main/java/com/arshad/controller/
└── LibraryEventController.java
```

**Endpoints:**
```java
@PostMapping
ResponseEntity<LibraryEvent> postLibraryEvent(@Valid @RequestBody LibraryEvent event)

@PutMapping
ResponseEntity<LibraryEvent> putLibraryEvent(@Valid @RequestBody LibraryEvent event)
```

**Acceptance Criteria:**
- POST returns 201 Created with Location header
- PUT returns 200 OK
- Input validation triggered via @Valid
- Both endpoints call service and return published event

---

### Phase 07: Error Handling & Global Exception Handler
**Status:** Pending  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 03 (validation)

**Deliverables:**
- `src/main/java/com/arshad/exception/GlobalExceptionHandler.java` — @ControllerAdvice for exception handling
- `src/main/java/com/arshad/dto/ErrorResponse.java` — Standard error response DTO
- Handle validation errors (MethodArgumentNotValidException) → 400
- Handle Kafka errors (KafkaException) → 503
- Handle all other exceptions → 500

**Key Decisions:**
- Centralized exception handling via @ControllerAdvice
- Consistent error response format across API
- Include timestamp, status, error type, message, details
- No stack traces exposed to clients (log internally)

**Files to Create:**
```
src/main/java/com/arshad/exception/
└── GlobalExceptionHandler.java

src/main/java/com/arshad/dto/
└── ErrorResponse.java
```

**Error Response Format:**
```json
{
  "timestamp": "2026-07-04T00:58:28Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Validation failed",
  "details": ["field error 1", "field error 2"]
}
```

**Acceptance Criteria:**
- Validation errors return 400 with field details
- Kafka publish failures return 503 with message
- All other exceptions return 500
- Error format consistent across all paths

---

### Phase 08: Unit Tests
**Status:** Pending  
**Duration:** ~2-3 hours  
**Dependencies:** Phase 06 (controller), Phase 04 (service)

**Deliverables:**
- `src/test/java/com/arshad/controller/LibraryEventControllerTest.java` — Controller unit tests (mocked service)
- `src/test/java/com/arshad/service/LibraryEventServiceTest.java` — Service unit tests (mocked KafkaTemplate)
- `src/test/java/com/arshad/validator/ValidationTest.java` — Validation logic tests
- Use Mockito for mocking; AssertJ for assertions
- JUnit 5 (already in Spring Boot 4.1)

**Key Decisions:**
- Mock all external dependencies (KafkaTemplate, service)
- Test happy paths and error cases
- Verify correct status codes, response bodies, exception handling
- No Kafka broker required for unit tests

**Test Files:**
```
src/test/java/com/arshad/
├── controller/LibraryEventControllerTest.java
├── service/LibraryEventServiceTest.java
├── validator/ValidationTest.java
└── producer/LibraryEventProducerTest.java
```

**Test Scenarios:**
| Test | Endpoint | Input | Expected |
|------|----------|-------|----------|
| Valid ADD | POST | Valid ADD event | 201 Created |
| Valid UPDATE | PUT | Valid UPDATE event | 200 OK |
| Missing field | POST | Missing bookName | 400 Bad Request |
| Invalid eventType for POST | POST | eventType=UPDATE | 400 Bad Request |
| Service throws KafkaException | POST | Any valid | 503 Service Unavailable |

**Acceptance Criteria:**
- All unit tests pass
- >80% code coverage for service/controller
- Mocks verify correct method calls
- Assertions verify response status and body

---

### Phase 09: Integration Tests (EmbeddedKafka)
**Status:** Pending  
**Duration:** ~2-3 hours  
**Dependencies:** Phase 05 (producer)

**Deliverables:**
- `src/test/java/com/arshad/integration/LibraryEventIntegrationTest.java` — End-to-end tests with EmbeddedKafka
- Real KafkaTemplate, real message publishing to embedded broker
- Assertions verify messages appear in topic with correct key/value
- Test idempotency: duplicate publishes result in single message (with retries)

**Key Decisions:**
- Use @SpringBootTest + @EmbeddedKafka annotations
- Embedded Kafka broker runs during test
- Consumer group to read messages from topic
- Assert message payload, key, timestamp

**Test Files:**
```
src/test/java/com/arshad/integration/
└── LibraryEventIntegrationTest.java
```

**Test Scenarios:**
| Test | Action | Assertion |
|------|--------|-----------|
| Publish ADD event | POST valid event | 1 message in topic with eventType=ADD |
| Publish UPDATE event | PUT valid event | 1 message in topic with eventType=UPDATE |
| Message key | POST event | Message key = libraryEventId |
| Message value | POST event | Message value is correct JSON |
| Validation failure | POST invalid | No message in topic; 400 returned |
| Kafka failure retry | Simulate failure | Retries occur; success after retry |

**Acceptance Criteria:**
- Integration tests pass with EmbeddedKafka
- Messages published to correct topic
- Key and value correct
- Validation errors prevent publish (no message in topic)
- Error responses correct (400, 503)

---

### Phase 10: Observability (Metrics & Logging)
**Status:** Pending  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 06 (controller)

**Deliverables:**
- Update LibraryEventController and LibraryEventProducer with structured logging
- Add Micrometer metrics: published.total, publish.failed.total, publish.latency
- Enable Actuator: /actuator/health, /actuator/metrics
- Include correlation ID in logs (from request header X-Request-Id or generate)

**Key Decisions:**
- Use SLF4J for logging
- Structured logging with correlation ID
- Micrometer for metrics (Spring Boot 4.1 includes it)
- Actuator endpoints for ops visibility

**Files to Modify:**
```
src/main/java/com/arshad/
├── controller/LibraryEventController.java (add logging)
├── service/LibraryEventServiceImpl.java (add logging)
└── producer/LibraryEventProducer.java (add logging + metrics)

src/main/resources/
└── application.yml (add actuator config)
```

**Application.yml Updates:**
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus
  endpoint:
    health:
      show-details: always
```

**Logging Examples:**
```
INFO: Published ADD event: libraryEventId=123, bookId=1, correlationId=xyz
ERROR: Failed to publish UPDATE event: libraryEventId=123, cause=broker timeout, correlationId=xyz
```

**Metrics to Track:**
- `library.events.published` (Counter)
- `library.events.publish.failed` (Counter)
- `library.events.publish.latency` (Timer)

**Acceptance Criteria:**
- Logs include libraryEventId, eventType, correlation ID
- Metrics emitted for success and failure
- Actuator endpoints enabled and return data
- Health check includes Kafka status (if integration available)

---

### Phase 11: Documentation & README
**Status:** Pending  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 06 (controller)

**Deliverables:**
- `README.md` — Build, run, configuration, API examples
- Curl examples for POST and PUT
- Configuration properties table
- Architecture overview diagram
- How to run with Docker Compose (optional local Kafka setup)
- Troubleshooting section

**Files to Create/Modify:**
```
README.md
docker-compose.yaml (if adding local Kafka setup)
```

**README Sections:**
1. Project overview (what it does)
2. Prerequisites (Java 25, Gradle, Kafka)
3. Build: `gradle build`
4. Run: `gradle bootRun` or `docker run`
5. Configuration: properties table
6. API endpoints: curl examples (POST/PUT)
7. Testing: `gradle test`
8. Metrics: accessing /actuator/metrics
9. Troubleshooting: common issues

**Acceptance Criteria:**
- README is clear and complete
- Curl examples work as documented
- Build and run instructions tested
- Architecture diagram included

---

## Implementation Order (Dependency-Respecting)

1. **Phase 01** → Models & DTOs (no dependencies)
2. **Phase 02** → Kafka Config (depends on 01)
3. **Phase 03** → Validation (depends on 01)
4. **Phase 04** → Service (depends on 01, 02)
5. **Phase 05** → Producer (depends on 02, 04)
6. **Phase 06** → Controller (depends on 04)
7. **Phase 07** → Error Handler (depends on 03)
8. **Phase 08** → Unit Tests (depends on 06, 04)
9. **Phase 09** → Integration Tests (depends on 05)
10. **Phase 10** → Observability (depends on 06)
11. **Phase 11** → Documentation (depends on 06)

## Parallel Workstreams
- **Phase 03 (Validation)** can be done in parallel with **Phase 02 (Config)** — both depend only on Phase 01
- **Phase 08 (Unit Tests)** can be done in parallel with **Phase 07 (Error Handler)** once dependencies are satisfied

## Testing Strategy

### Unit Test Coverage
- Controller: request mapping, status codes, error handling
- Service: Kafka producer calls, success/failure handling
- Validation: field validation rules
- **Tools:** Mockito, JUnit 5, AssertJ

### Integration Test Coverage
- End-to-end POST/PUT flow
- Message correctness in Kafka topic
- Error scenarios (validation, Kafka failure)
- **Tools:** @SpringBootTest, @EmbeddedKafka, Kafka test containers

### Build & Test Commands
```bash
# Build
./gradlew clean build

# Unit tests only
./gradlew test

# Integration tests (includes unit)
./gradlew test --tests "*IntegrationTest"

# All tests
./gradlew test

# Run app
./gradlew bootRun
```

## Checklist

- [ ] Phase 01: Models created and compile
- [ ] Phase 02: Kafka config, application.yml set
- [ ] Phase 03: Validation annotations added
- [ ] Phase 04: Service layer implemented
- [ ] Phase 05: Producer wrapper implemented
- [ ] Phase 06: REST endpoints working (manual test)
- [ ] Phase 07: Error handler catches all exceptions
- [ ] Phase 08: Unit tests pass (>80% coverage)
- [ ] Phase 09: Integration tests pass with EmbeddedKafka
- [ ] Phase 10: Metrics and logging working
- [ ] Phase 11: README complete and accurate
- [ ] All tests passing in CI/CD
- [ ] Application starts and responds to requests
- [ ] Kafka messages verify on topic

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Kafka integration complexity | Phase 02 early; use Spring KafkaTemplate abstractions |
| Validation not working | Phase 03 with unit tests for each rule |
| Test setup slow | Use EmbeddedKafka; parallel test execution |
| Error handling incomplete | Phase 07 covers all exception types; manual testing |

---

**Plan Version:** 1.0  
**Created:** July 4, 2026  
**Status:** Ready for implementation
