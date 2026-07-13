package com.arshad.controller;

import com.arshad.model.Book;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for {@link LibraryEventController}'s POST endpoint.
 *
 * <p>Boots the full application context against an {@link EmbeddedKafkaBroker}, so
 * every layer — HTTP, service, producer, and Kafka — runs in-process without
 * requiring an external broker. {@code spring.docker.compose.enabled=false} prevents
 * Docker Compose from starting during the test run.
 *
 * <p>Because the controller returns a {@link java.util.concurrent.CompletableFuture},
 * each test follows the two-step async-dispatch pattern:
 * <ol>
 *   <li>Perform the request and assert that async processing started.</li>
 *   <li>Dispatch the async result and assert the final HTTP response.</li>
 * </ol>
 *
 * <h2>Consumer isolation between tests</h2>
 * <p>All tests share a single embedded Kafka topic. Two measures are taken to ensure
 * each test's consumer only sees the record published by that specific test:
 * <ul>
 *   <li><b>Unique consumer group per test</b> — a group ID derived from the test's
 *       display name is used so group offsets never bleed between tests.</li>
 *   <li><b>{@code seekToEnd=true}</b> — {@link EmbeddedKafkaBroker#consumeFromAnEmbeddedTopic}
 *       is called with {@code seekToEnd=true} in {@code @BeforeEach}. The no-argument
 *       overload silently calls {@code seekToBeginning}, which would cause every consumer
 *       to replay all records ever published to the topic across all tests. Seeking to
 *       the end before the test publishes its record ensures each consumer only ever
 *       sees that one record.</li>
 * </ul>
 *
 * <h2>Why a poll loop instead of KafkaTestUtils.getSingleRecord</h2>
 * <p>{@link KafkaTestUtils#getSingleRecord} issues a single {@code consumer.poll()} call
 * with a fixed 5-second timeout. If the record has not arrived within that one window —
 * because the broker is slow, the JVM paused for GC, or a CI machine is under load —
 * the call returns empty and the test fails, even though the record would have arrived
 * moments later.
 *
 * <p>{@link #pollForRecord} instead polls in short 500 ms bursts and retries until either
 * a matching record is found or a 10-second deadline elapses. This makes the tests
 * deterministic under variable latency without increasing the median wait time on a
 * fast machine.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = "spring.docker.compose.enabled=false"
)
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = {LibraryEventsControllerIntegrationTest.TOPIC},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DisplayName("LibraryEventController Integration Tests")
class LibraryEventsControllerIntegrationTest {

    static final String TOPIC = "library-events";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Long, LibraryEvent> consumer;

    @BeforeEach
    void setUpConsumer(TestInfo testInfo) {
        // Unique group per test so offset state never bleeds between tests
        String groupId = "integration-test-" + testInfo.getDisplayName().hashCode();
        Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafkaBroker, groupId, false);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "*");

        DefaultKafkaConsumerFactory<Long, LibraryEvent> factory = new DefaultKafkaConsumerFactory<>(
                props,
                new LongDeserializer(),
                new JacksonJsonDeserializer<>(LibraryEvent.class)
        );
        consumer = factory.createConsumer();
        // seekToEnd=true positions this consumer at the topic tail before the test runs,
        // so it only ever reads records produced during this test and not from prior tests.
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, true, TOPIC);
    }

    @AfterEach
    void tearDownConsumer() {
        consumer.close();
    }

    @Test
    @DisplayName("POST with valid ADD event returns 201 Created and publishes the record to Kafka")
    void createLibraryEvent_validAddEvent_returns201AndPublishesToKafka() throws Exception {
        Book book = new Book(1L, "Kafka in Action", "Dylan Scott");
        LibraryEvent event = new LibraryEvent(100L, EventType.ADD, book);

        MvcResult mvcResult = mockMvc.perform(post("/v1/library-events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(event)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/v1/library-events/100"))
                .andExpect(jsonPath("$.libraryEventId").value(100))
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookName").value("Kafka in Action"));

        ConsumerRecord<Long, LibraryEvent> record = pollForRecord(100L);
        assertThat(record.key()).isEqualTo(100L);
        assertThat(record.value().getLibraryEventId()).isEqualTo(100L);
        assertThat(record.value().getEventType()).isEqualTo(EventType.ADD);
        assertThat(record.value().getBook().getBookId()).isEqualTo(1L);
        assertThat(record.value().getBook().getBookName()).isEqualTo("Kafka in Action");
        assertThat(record.value().getBook().getBookAuthor()).isEqualTo("Dylan Scott");
    }

    @Test
    @DisplayName("POST with a different valid ADD event returns 201 Created and publishes the record to Kafka")
    void createLibraryEvent_anotherValidAddEvent_returns201AndPublishesToKafka() throws Exception {
        Book book = new Book(42L, "Designing Data-Intensive Applications", "Martin Kleppmann");
        LibraryEvent event = new LibraryEvent(200L, EventType.ADD, book);

        MvcResult mvcResult = mockMvc.perform(post("/v1/library-events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(event)))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/v1/library-events/200"))
                .andExpect(jsonPath("$.libraryEventId").value(200))
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookName").value("Designing Data-Intensive Applications"));

        ConsumerRecord<Long, LibraryEvent> record = pollForRecord(200L);
        assertThat(record.key()).isEqualTo(200L);
        assertThat(record.value().getLibraryEventId()).isEqualTo(200L);
        assertThat(record.value().getEventType()).isEqualTo(EventType.ADD);
        assertThat(record.value().getBook().getBookId()).isEqualTo(42L);
        assertThat(record.value().getBook().getBookName()).isEqualTo("Designing Data-Intensive Applications");
        assertThat(record.value().getBook().getBookAuthor()).isEqualTo("Martin Kleppmann");
    }

    /**
     * Polls the embedded Kafka topic in short bursts until a record matching the given
     * {@code expectedKey} is found, or a 10-second deadline elapses.
     *
     * <p>This is preferred over {@link KafkaTestUtils#getSingleRecord}, which issues only
     * a single {@code poll()} call. On a slow CI machine that single window may expire
     * before the broker delivers the record, causing a spurious failure even though the
     * record arrives moments later. The retry loop here tolerates variable broker latency
     * without increasing the median wait time on a fast machine.
     *
     * @param expectedKey the Kafka record key to match
     * @return the first matching {@link ConsumerRecord}
     * @throws AssertionError if no matching record is found within 10 seconds
     */
    private ConsumerRecord<Long, LibraryEvent> pollForRecord(Long expectedKey) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();

        while (System.nanoTime() < deadline) {
            ConsumerRecords<Long, LibraryEvent> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<Long, LibraryEvent> record : records.records(TOPIC)) {
                if (expectedKey.equals(record.key())) {
                    return record;
                }
            }
        }

        throw new AssertionError(
                "No Kafka record with key=" + expectedKey + " was published to topic '" + TOPIC + "' within 10 seconds");
    }
}
