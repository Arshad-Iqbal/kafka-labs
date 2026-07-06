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

### Phase 01: Domain Models & DTOs ✅ COMPLETED
**Status:** COMPLETED  
**Duration:** ~30 minutes  
**Dependencies:** None

**Deliverables:**
- ✅ `src/main/java/com/arshad/model/LibraryEvent.java` — Main event DTO with libraryEventId, eventType, book
- ✅ `src/main/java/com/arshad/model/Book.java` — Book DTO with bookId, bookName, bookAuthor
- ✅ `src/main/java/com/arshad/model/EventType.java` — Enum: ADD, UPDATE
- ✅ Lombok annotations (@Data, @AllArgsConstructor, @NoArgsConstructor) for reduced boilerplate

**Key Decisions:**
- Use DTOs (not JPA entities, no persistence needed)
- Enum for type-safety
- Lombok for cleaner code

**Files Created:**
```
src/main/java/com/arshad/model/
├── LibraryEvent.java
├── Book.java
└── EventType.java
```

**Acceptance Criteria:**
- ✅ All classes compile with Lombok
- ✅ Serialization/deserialization to JSON works
- ✅ Enums are correctly defined
- ✅ Tests: LibraryEventSerializationTest (10 tests passing)

---

### Phase 02: Kafka Producer Configuration ✅ COMPLETED
**Status:** COMPLETED  
**Duration:** ~30 minutes  
**Dependencies:** Phase 01 (models)

**Deliverables:**
- ✅ `src/main/java/com/arshad/config/KafkaProducerConfig.java` — KafkaTemplate<Long, LibraryEvent> bean, producer configs
- ✅ `src/main/resources/application.yml` — Kafka bootstrap servers, topic name, producer settings
- ✅ Idempotent producer configured (enable.idempotence=true, acks=all, retries=10)

**Key Decisions:**
- Use Spring KafkaTemplate for high-level abstraction
- Idempotent producer to prevent duplicates
- Externalize config via YAML properties file
- Topic name: `library-events`
- Key type: Long (libraryEventId), Value type: LibraryEvent (JSON)

**Files Created:**
```
src/main/java/com/arshad/config/
└── KafkaProducerConfig.java

src/main/resources/
└── application.yml
```

**Application.yml Configuration:**
```yaml
kafka:
  topic:
    library-events: library-events
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      acks: all
      retries: 10
      properties:
        enable.idempotence: true
        linger.ms: 10
```

**Acceptance Criteria:**
- ✅ KafkaTemplate<Long, LibraryEvent> bean available for injection
- ✅ Producer properties configured correctly
- ✅ Application starts without Kafka errors

---

### Phase 03: Input Validation ✅ COMPLETED
**Status:** COMPLETED  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 01 (models)

**Deliverables:**
- ✅ Added `@NotNull`, `@NotEmpty`, `@Positive`, `@Size` annotations to models
- ✅ `src/main/java/com/arshad/validator/ValidEventType.java` — Custom constraint annotation
- ✅ `src/main/java/com/arshad/validator/EventTypeValidator.java` — Custom validator for eventType enum

**Key Decisions:**
- Use Spring Validation (JSR-380) for declarative validation
- Custom validator for eventType to validate only ADD or UPDATE
- Fail fast on validation errors

**Files Created/Modified:**
```
src/main/java/com/arshad/model/
├── LibraryEvent.java (added @Valid, @NotNull, @Positive, @ValidEventType)
├── Book.java (added @Valid, @NotNull, @NotEmpty, @Size, @Positive)
└── EventType.java

src/main/java/com/arshad/validator/
├── ValidEventType.java (custom constraint annotation)
└── EventTypeValidator.java (validator implementation)
```

**Validation Rules Applied:**
- libraryEventId: @NotNull, @Positive
- eventType: @NotNull, @ValidEventType (custom validator for ADD/UPDATE)
- book: @NotNull, @Valid (cascade validation)
- bookId: @NotNull, @Positive
- bookName: @NotNull, @NotEmpty, @Size(max=255)
- bookAuthor: @NotNull, @NotEmpty, @Size(max=255)

**Acceptance Criteria:**
- ✅ Validation works on all fields
- ✅ Custom eventType validator enforces business rules (ADD/UPDATE only)
- ✅ Tests verify validation messages are clear
- ✅ Tests: LibraryEventValidationTest (40+ tests passing)

---

### Phase 04: LibraryEventService (Business Logic) ✅ COMPLETED with Enhancements
**Status:** COMPLETED  
**Duration:** ~2-3 hours (including refactoring)  
**Dependencies:** Phase 01 (models), Phase 02 (config)

**Deliverables:**
- ✅ `src/main/java/com/arshad/service/LibraryEventService.java` — Service interface with createLibraryEvent() and updateLibraryEvent() methods
- ✅ `src/main/java/com/arshad/service/LibraryEventServiceImpl.java` — Implementation with producer delegation
- ✅ `src/main/java/com/arshad/producer/LibraryEventProducer.java` — Producer component (extracted from service)
- ✅ `src/main/java/com/arshad/exception/LibraryEventPublishException.java` — Custom exception for publish failures

**Key Decisions & Changes:**
1. **Non-Blocking Async Publishing** (changed from blocking .get() to async callbacks)
   - Uses CompletableFuture<SendResult<...>> with whenComplete() callback
   - Service returns event immediately without waiting for broker ACK
   - Failure logging happens asynchronously

2. **Producer Component Extraction** (NEW)
   - Created separate LibraryEventProducer class owning all Kafka logic
   - Service delegates publishing: libraryEventProducer.sendLibraryEvent(event)
   - Improved separation of concerns and testability

3. **Exception Handling Strategy** (NEW)
   - Custom LibraryEventPublishException wraps Kafka failures
   - Producer catches immediate failures; service wraps with operation context
   - Async callback failures logged but not thrown (non-blocking contract)

4. **Method Naming** (RENAMED for semantic clarity)
   - publishAddEvent → createLibraryEvent
   - publishUpdateEvent → updateLibraryEvent

**Files Created/Modified:**
```
src/main/java/com/arshad/
├── service/
│   ├── LibraryEventService.java (interface, renamed methods)
│   └── LibraryEventServiceImpl.java (refactored to delegate pattern)
├── producer/
│   └── LibraryEventProducer.java (NEW - extracted component)
└── exception/
    └── LibraryEventPublishException.java (NEW - custom exception)
```

**Service Methods (Updated):**
```java
LibraryEvent createLibraryEvent(LibraryEvent event) throws LibraryEventPublishException;
LibraryEvent updateLibraryEvent(LibraryEvent event) throws LibraryEventPublishException;
```

**Producer Flow:**
```
sendLibraryEvent(event)
  ↓ Validate: event != null, libraryEventId != null, eventType != null
  ↓ Build Message with headers (TOPIC, messageKey, eventType)
  ↓ Send via KafkaTemplate (async)
  ↓ Register whenComplete() callback for logging
  ↓ Wrap immediate failures in LibraryEventPublishException
```

**Acceptance Criteria:**
- ✅ Service delegates to producer correctly
- ✅ Service sets event type (ADD/UPDATE) before delegating
- ✅ Methods return published event immediately (non-blocking)
- ✅ Service wraps producer exceptions with operation context
- ✅ Producer validates inputs and wraps immediate failures
- ✅ Async callback failures logged (not thrown)
- ✅ Tests: LibraryEventServiceImplTest (6 tests) + LibraryEventProducerTest (7 tests) = 13 tests passing

---

### Phase 05: LibraryEventProducer Wrapper ✅ COMPLETED (Integrated into Phase 04)
**Status:** COMPLETED  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 02 (config), Phase 04 (service)

**Deliverables:**
- ✅ `src/main/java/com/arshad/producer/LibraryEventProducer.java` — Producer component
- ✅ Handles async sending, error logging, callback handling
- ✅ Encapsulates idempotent producer semantics

**Key Decisions:**
- Producer owns all Kafka communication logic
- Uses async CompletableFuture.whenComplete() for non-blocking callbacks
- Throws LibraryEventPublishException on immediate failures
- Key: Long (libraryEventId); Value: JSON (LibraryEvent)
- Async callback failures logged but not thrown

**Files Created:**
```
src/main/java/com/arshad/producer/
└── LibraryEventProducer.java
```

**Methods:**
```java
void sendLibraryEvent(LibraryEvent event) throws LibraryEventPublishException;
```

**Acceptance Criteria:**
- ✅ Sends event to Kafka topic asynchronously
- ✅ Logs success with event ID and topic via callback
- ✅ Wraps and throws exception on immediate failures
- ✅ Callback captures async success/error cases
- ✅ Validated by: LibraryEventProducerTest (7 tests passing)

---

### Phase 06: REST Controller (Endpoints) ⏳ PENDING
**Status:** PENDING  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 04 (service)

**Planned Deliverables:**
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

**Planned Endpoints:**
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

### Phase 07: Error Handling & Global Exception Handler ⏳ PENDING
**Status:** PENDING  
**Duration:** ~1-2 hours  
**Dependencies:** Phase 03 (validation), Phase 04 (LibraryEventPublishException)

**Planned Deliverables:**
- `src/main/java/com/arshad/exception/GlobalExceptionHandler.java` — @ControllerAdvice for exception handling
- `src/main/java/com/arshad/dto/ErrorResponse.java` — Standard error response DTO
- Handle validation errors (MethodArgumentNotValidException) → 400
- Handle Kafka publish errors (LibraryEventPublishException) → 503
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
- LibraryEventPublishException return 503 with message
- All other exceptions return 500
- Error format consistent across all paths

---

### Phase 08: Unit Tests ✅ COMPLETED (Partial)
**Status:** PARTIALLY COMPLETED  
**Duration:** ~2 hours  
**Dependencies:** Phase 04 (service), Phase 05 (producer)

**Deliverables:**
- ✅ `src/test/java/com/arshad/service/LibraryEventServiceImplTest.java` — Service unit tests (mocked producer)
- ✅ `src/test/java/com/arshad/producer/LibraryEventProducerTest.java` — Producer unit tests (mocked KafkaTemplate)
- ✅ `src/test/java/com/arshad/model/LibraryEventValidationTest.java` — Validation logic tests
- ✅ `src/test/java/com/arshad/model/LibraryEventSerializationTest.java` — Serialization tests
- ✅ Use Mockito for mocking; AssertJ for assertions
- ✅ JUnit 5 (included in Spring Boot)

**Key Decisions:**
- Mock external dependencies (KafkaTemplate for producer, producer for service)
- Test happy paths and error cases
- Verify correct delegations, exception handling, and callback behavior
- No Kafka broker required for unit tests

**Test Files Created:**
```
src/test/java/com/arshad/
├── model/
│   ├── LibraryEventSerializationTest.java (10 tests)
│   └── LibraryEventValidationTest.java (40+ tests)
├── service/
│   └── LibraryEventServiceImplTest.java (6 tests)
└── producer/
    └── LibraryEventProducerTest.java (7 tests)
```

**Test Coverage:**
- ✅ Service delegation to producer
- ✅ Service exception wrapping with context
- ✅ Producer async send verification
- ✅ Producer immediate failure exception wrapping
- ✅ Validation on all model fields
- ✅ JSON serialization/deserialization

**Acceptance Criteria:**
- ✅ All 60+ unit tests passing
- ✅ >80% code coverage for service/producer/models
- ✅ Mocks verify correct method calls
- ✅ Assertions verify response status and behavior
- ✅ Controller tests: PENDING (depends on Phase 06)

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

1. **Phase 01** → Models & DTOs ✅ COMPLETED
2. **Phase 02** → Kafka Config ✅ COMPLETED
3. **Phase 03** → Validation ✅ COMPLETED
4. **Phase 04** → Service (with Producer extraction) ✅ COMPLETED
5. **Phase 05** → Producer (integrated into Phase 04) ✅ COMPLETED
6. **Phase 06** → Controller (depends on 04) ⏳ PENDING
7. **Phase 07** → Error Handler (depends on 03, 04) ⏳ PENDING
8. **Phase 08** → Unit Tests (partially complete, phase 06 still pending)
9. **Phase 09** → Integration Tests (depends on 05) ⏳ PENDING
10. **Phase 10** → Observability (depends on 06) ⏳ PENDING
11. **Phase 11** → Documentation (depends on 06) ⏳ PENDING

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

- [x] Phase 01: Models created and compile
- [x] Phase 02: Kafka config, application.yml set
- [x] Phase 03: Validation annotations added
- [x] Phase 04: Service layer implemented (with async non-blocking publishing)
- [x] Phase 04.1: Producer component extracted and integrated
- [x] Phase 04.2: LibraryEventPublishException created and error handling implemented
- [x] Phase 04.3: Method names refactored (createLibraryEvent/updateLibraryEvent)
- [x] Phase 08 (Partial): Model + Service + Producer unit tests passing (60+ tests)
- [ ] Phase 06: REST endpoints implemented
- [ ] Phase 07: Error handler catches all exceptions
- [ ] Phase 08 (Complete): Controller tests (depends on Phase 06)
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
