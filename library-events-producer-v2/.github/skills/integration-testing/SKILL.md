# Integration Testing Skill — Kafka Producer App

This skill describes how to write integration tests for a Spring Boot Kafka producer application.
Tests boot the **full application context**, drive HTTP endpoints via **MockMVC**, and verify
Kafka publishing behaviour with an **EmbeddedKafkaBroker** — no external broker or Docker Compose
is required.

---

## 1. Required Annotations

### Class-level annotations (always apply all four)

```java
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,   // (1)
        properties = "spring.docker.compose.enabled=false"     // (2)
)
@AutoConfigureMockMvc                                          // (3)
@EmbeddedKafka(                                                // (4)
        partitions = 1,
        topics = {"library-events"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DisplayName("MyComponent Integration Tests")
class MyIntegrationTest { ... }
```

| # | Annotation | Why |
|---|-----------|-----|
| 1 | `@SpringBootTest(webEnvironment = MOCK)` | Loads the full `ApplicationContext` with a mock Servlet environment so all layers (controller → service → producer) run in-process. |
| 2 | `properties = "spring.docker.compose.enabled=false"` | Prevents `DockerComposeLifecycleManager` from starting the real broker defined in `compose.yaml` during tests. |
| 3 | `@AutoConfigureMockMvc` | Auto-configures a `MockMvc` bean wired to the full context (not a slice), enabling end-to-end HTTP testing. |
| 4 | `@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers")` | Starts an in-process Kafka broker and overrides the `spring.kafka.bootstrap-servers` property so all producer/consumer beans connect to it automatically. |

---

## 2. Injected Beans

```java
@Autowired
private MockMvc mockMvc;

@Autowired
private ObjectMapper objectMapper;          // Jackson 2.x ObjectMapper (tools.jackson.databind)

@Autowired
private EmbeddedKafkaBroker embeddedKafkaBroker;

private Consumer<Long, LibraryEvent> consumer;   // created per-test in @BeforeEach
```

---

## 3. Consumer Setup & Teardown

Create a **fresh consumer per test** with a unique group ID so offset state never bleeds between
tests. Use `seekToEnd=true` when subscribing so the consumer only sees records produced
*during that specific test*, not replays from earlier tests.

```java
@BeforeEach
void setUpConsumer(TestInfo testInfo) {
    // Unique group per test – prevents offset bleed
    String groupId = "integration-test-" + testInfo.getDisplayName().hashCode();
    Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafkaBroker, groupId, false);
    props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");

    DefaultKafkaConsumerFactory<Long, LibraryEvent> factory = new DefaultKafkaConsumerFactory<>(
            props,
            new LongDeserializer(),
            new JacksonJsonDeserializer<>(LibraryEvent.class)
    );
    consumer = factory.createConsumer();

    // seekToEnd=true → only read records produced AFTER this point
    embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, true, "library-events");
}

@AfterEach
void tearDownConsumer() {
    consumer.close();
}
```

> **Why not `KafkaTestUtils.getSingleRecord`?**
> It issues a single `poll()` with a 5-second timeout. On a slow CI runner one window may expire
> before the broker delivers the record. Use a retry poll loop instead (see §5).

---

## 4. Async-Dispatch Pattern for CompletableFuture Controllers

The controller returns `CompletableFuture<ResponseEntity<...>>`, which triggers Spring MVC's
async dispatch. Tests must use a **two-step** approach:

```java
// Step 1 – perform the request and assert async processing started
MvcResult mvcResult = mockMvc.perform(post("/v1/library-events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(event)))
        .andExpect(request().asyncStarted())   // ← asserts async was triggered
        .andReturn();

// Step 2 – dispatch the async result and assert the final HTTP response
mockMvc.perform(asyncDispatch(mvcResult))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", "/v1/library-events/100"))
        .andExpect(jsonPath("$.libraryEventId").value(100))
        .andExpect(jsonPath("$.eventType").value("ADD"))
        .andExpect(jsonPath("$.book.bookName").value("Kafka in Action"));
```

For synchronous (`200 OK`) responses swap `status().isCreated()` for `status().isOk()` and
omit the `Location` header assertion.

---

## 5. Retry Poll Loop (Kafka Consumer)

```java
private ConsumerRecord<Long, LibraryEvent> pollForRecord(Long expectedKey) {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();

    while (System.nanoTime() < deadline) {
        ConsumerRecords<Long, LibraryEvent> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<Long, LibraryEvent> record : records.records("library-events")) {
            if (expectedKey.equals(record.key())) {
                return record;
            }
        }
    }

    throw new AssertionError(
            "No Kafka record with key=" + expectedKey + " was published within 10 seconds");
}
```

---

## 6. Full Test Example (POST — ADD event)

```java
@Test
@DisplayName("POST with valid ADD event returns 201 Created and publishes the record to Kafka")
void createLibraryEvent_validAddEvent_returns201AndPublishesToKafka() throws Exception {
    Book book = new Book(1L, "Kafka in Action", "Dylan Scott");
    LibraryEvent event = new LibraryEvent(100L, EventType.ADD, book);

    // Step 1 – send request, assert async started
    MvcResult mvcResult = mockMvc.perform(post("/v1/library-events")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(event)))
            .andExpect(request().asyncStarted())
            .andReturn();

    // Step 2 – dispatch async result, assert HTTP response
    mockMvc.perform(asyncDispatch(mvcResult))
            .andExpect(status().isCreated())
            .andExpect(header().string("Location", "/v1/library-events/100"))
            .andExpect(jsonPath("$.libraryEventId").value(100))
            .andExpect(jsonPath("$.eventType").value("ADD"))
            .andExpect(jsonPath("$.book.bookName").value("Kafka in Action"));

    // Step 3 – verify the Kafka record
    ConsumerRecord<Long, LibraryEvent> record = pollForRecord(100L);
    assertThat(record.key()).isEqualTo(100L);
    assertThat(record.value().getLibraryEventId()).isEqualTo(100L);
    assertThat(record.value().getEventType()).isEqualTo(EventType.ADD);
    assertThat(record.value().getBook().getBookId()).isEqualTo(1L);
    assertThat(record.value().getBook().getBookName()).isEqualTo("Kafka in Action");
    assertThat(record.value().getBook().getBookAuthor()).isEqualTo("Dylan Scott");
}
```

---

## 7. Full Test Example (PUT — UPDATE event)

```java
@Test
@DisplayName("PUT with valid UPDATE event returns 200 OK and publishes the record to Kafka")
void updateLibraryEvent_validUpdateEvent_returns200AndPublishesToKafka() throws Exception {
    Book book = new Book(1L, "Kafka in Action", "Dylan Scott");
    LibraryEvent event = new LibraryEvent(100L, EventType.UPDATE, book);

    MvcResult mvcResult = mockMvc.perform(put("/v1/library-events")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(objectMapper.writeValueAsString(event)))
            .andExpect(request().asyncStarted())
            .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.libraryEventId").value(100))
            .andExpect(jsonPath("$.eventType").value("UPDATE"))
            .andExpect(jsonPath("$.book.bookName").value("Kafka in Action"));

    ConsumerRecord<Long, LibraryEvent> record = pollForRecord(100L);
    assertThat(record.key()).isEqualTo(100L);
    assertThat(record.value().getEventType()).isEqualTo(EventType.UPDATE);
}
```

---

## 8. Required Imports

```java
// Spring Boot test
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;

// Embedded Kafka
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

// Kafka consumer / deserializer
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

// MockMVC
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

// Jackson (Spring Boot 4.x uses Jackson 3.x / tools.jackson namespace)
import tools.jackson.databind.ObjectMapper;

// JUnit 5
import org.junit.jupiter.api.*;

// AssertJ
import static org.assertj.core.api.Assertions.assertThat;
```

---

## 9. Required Test Dependencies (`build.gradle`)

```groovy
testImplementation 'org.springframework.boot:spring-boot-starter-test'
testImplementation 'org.springframework.kafka:spring-kafka-test'
testImplementation 'org.springframework.boot:spring-boot-webmvc-test'
testImplementation 'org.springframework:spring-test'
testRuntimeOnly    'org.junit.platform:junit-platform-launcher'
```

---

## 10. Key Pitfalls

| Pitfall | Fix |
|---------|-----|
| Consumer replays records from previous tests | Always pass `seekToEnd=true` to `consumeFromAnEmbeddedTopic` |
| Different tests interfere via shared consumer group offsets | Use a unique group ID per test (e.g. derived from `testInfo.getDisplayName().hashCode()`) |
| `asyncStarted()` assertion fails | Ensure `WebEnvironment.MOCK` is used — `RANDOM_PORT` / `DEFINED_PORT` do not use MockMvc |
| Docker Compose tries to start during tests | Set `spring.docker.compose.enabled=false` in `@SpringBootTest(properties = ...)` |
| `KafkaTestUtils.getSingleRecord` flakes on CI | Replace with a retry poll loop capped at 10 seconds (see §5) |
| Wrong `ObjectMapper` namespace | Spring Boot 4.x uses `tools.jackson.databind.ObjectMapper` (Jackson 3.x), not `com.fasterxml` |
