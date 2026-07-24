package com.learnkafka.consumer;

import com.learnkafka.TestcontainersConfiguration;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventConsumerFailure;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.model.BookDto;
import com.learnkafka.model.EventType;
import com.learnkafka.model.LibraryEventDto;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventConsumerFailureRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"library-events", "library-events.DLT"},
        bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers"
)
@TestPropertySource(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.recovery.mode=both"
})
@ImportTestcontainers(TestcontainersConfiguration.class)
class LibraryEventsConsumerFailureIntegrationTest {

    /**
     * Override the auto-configured KafkaTemplate<Object, Object> with one that targets the
     * embedded broker. The main application.yml hard-codes spring.kafka.producer.bootstrap-servers
     * to localhost:9092, so without this override the DeadLetterPublishingRecoverer would try
     * to publish to a real Kafka cluster that is not available during tests.
     */
    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        KafkaTemplate<Object, Object> kafkaTemplate(EmbeddedKafkaBroker broker) {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
            return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
        }
    }

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    BookRepository bookRepository;

    @Autowired
    LibraryEventConsumerFailureRepository failureRepository;

    KafkaTemplate<Long, LibraryEventDto> kafkaTemplate;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
        failureRepository.deleteAll();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @Test
    void consumeLibraryEvent_UPDATE_withNonExistentId_shouldPublishToDltAndPersistFailureRecord() throws Exception {
        // given — UPDATE event for a library event ID that does not exist in the DB.
        // IllegalArgumentException is non-retryable, so the error handler skips all retries
        // and immediately invokes the "both" recovery: DLT publish + failure table persist.
        BookDto bookDto = new BookDto(999L, "Non-Existent Book", "Unknown Author");
        LibraryEventDto dto = new LibraryEventDto(9999L, EventType.UPDATE, bookDto);

        KafkaConsumer<byte[], String> dltConsumer = buildDltConsumer();
        dltConsumer.subscribe(List.of("library-events.DLT"));

        // when
        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

        // then — wait for the recovery block to persist the failure record
        waitForCondition(
                () -> failureRepository.count() >= 1,
                30,
                "Timed out waiting for a record in library_event_consumer_failure"
        );

        // verify exactly one message landed on the DLT
        ConsumerRecords<byte[], String> dltRecords = pollUntilNonEmpty(dltConsumer, 10);
        dltConsumer.close();

        assertEquals(1, dltRecords.count(), "Expected exactly one record on library-events.DLT");

        // verify the failure table entry
        List<LibraryEventConsumerFailure> failures = failureRepository.findAll();
        assertEquals(1, failures.size());

        LibraryEventConsumerFailure failure = failures.getFirst();
        assertEquals("library-events", failure.getTopic());
        assertEquals(0, failure.getPartition());
        // Spring Kafka wraps listener exceptions in ListenerExecutionFailedException before
        // passing to the recoverer; the original IllegalArgumentException is the cause.
        assertTrue(failure.getExceptionClass().contains("ListenerExecutionFailedException"),
                "Expected ListenerExecutionFailedException wrapper, got: " + failure.getExceptionClass());
        assertNotNull(failure.getExceptionMessage());
        // The record value is the Lombok toString() of LibraryEventDto — contains libraryEventId=9999
        assertNotNull(failure.getRecordValue());
        assertTrue(failure.getRecordValue().contains("9999"),
                "Expected record value to contain libraryEventId 9999, got: " + failure.getRecordValue());
        assertNotNull(failure.getFailedAt());

        // verify nothing was persisted to the main tables
        assertEquals(0, libraryEventRepository.count());
        assertEquals(0, bookRepository.count());
    }

    @Test
    void consumeLibraryEvent_ADD_shouldPersistLibraryEventAndBook() throws Exception {
        // given — use id=300 to avoid conflicts with other tests' async processing
        BookDto bookDto = new BookDto(100L, "Clean Code", "Robert C. Martin");
        LibraryEventDto dto = new LibraryEventDto(300L, EventType.ADD, bookDto);

        // when
        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);
        waitForRecordCount(1, 10);

        // then
        List<LibraryEvent> events = libraryEventRepository.findAll();
        List<Book> books = bookRepository.findAll();

        assertEquals(1, events.size());
        assertEquals(1, books.size());

        LibraryEvent savedEvent = events.getFirst();
        Book savedBook = books.getFirst();

        assertEquals(300, savedEvent.getLibraryEventId());
        assertEquals(LibraryEventType.ADD, savedEvent.getEventType());
        assertNotNull(savedEvent.getCreatedAt());
        assertNotNull(savedEvent.getUpdatedAt());

        assertEquals(100, savedBook.getBookId());
        assertEquals("Clean Code", savedBook.getBookName());
        assertEquals("Robert C. Martin", savedBook.getBookAuthor());
        assertNotNull(savedBook.getLibraryEvent());
        assertEquals(savedEvent.getLibraryEventId(), savedBook.getLibraryEvent().getLibraryEventId());
        assertNotNull(savedBook.getCreatedAt());
        assertNotNull(savedBook.getUpdatedAt());
    }

    @Test
    void consumeLibraryEvent_UPDATE_shouldUpdateExistingLibraryEventAndBook() throws Exception {
        // given — ADD first, then UPDATE the same event
        BookDto addBookDto = new BookDto(101L, "Kafka Basics", "Alice");
        LibraryEventDto addDto = new LibraryEventDto(301L, EventType.ADD, addBookDto);
        kafkaTemplate.send("library-events", addDto).get(10, TimeUnit.SECONDS);
        waitForRecordCount(1, 10);

        // when — UPDATE the book details
        BookDto updateBookDto = new BookDto(101L, "Kafka in Action", "Bob");
        LibraryEventDto updateDto = new LibraryEventDto(301L, EventType.UPDATE, updateBookDto);
        kafkaTemplate.send("library-events", updateDto).get(10, TimeUnit.SECONDS);

        waitForCondition(
                () -> bookRepository.findById(101)
                        .map(b -> "Kafka in Action".equals(b.getBookName()) && "Bob".equals(b.getBookAuthor()))
                        .orElse(false),
                10,
                "Timed out waiting for UPDATE to be applied to book 101"
        );

        // then — still exactly one event and one book; event type flipped to UPDATE
        assertEquals(1, libraryEventRepository.count());
        assertEquals(1, bookRepository.count());

        LibraryEvent updatedEvent = libraryEventRepository.findById(301).orElseThrow();
        assertEquals(LibraryEventType.UPDATE, updatedEvent.getEventType());

        Book updatedBook = bookRepository.findById(101).orElseThrow();
        assertEquals("Kafka in Action", updatedBook.getBookName());
        assertEquals("Bob", updatedBook.getBookAuthor());
    }

    @Test
    void consumeLibraryEvent_invalid_withNullBook_shouldNotPersistAndShouldWriteFailureRecord() throws Exception {
        // given — null book violates @NotNull on LibraryEventDto.book → IllegalArgumentException (non-retryable)
        // → recovery (both): DLT + failure table
        LibraryEventDto invalidDto = new LibraryEventDto(1L, EventType.ADD, null);

        // when
        kafkaTemplate.send("library-events", invalidDto).get(10, TimeUnit.SECONDS);

        // then — nothing persisted in the main tables throughout the 3-second window
        assertConditionRemainsTrue(
                () -> libraryEventRepository.count() == 0 && bookRepository.count() == 0,
                3,
                "Unexpected persistence: ADD event with null book should not persist any record"
        );

        // and a failure record is eventually written
        waitForCondition(
                () -> failureRepository.count() == 1,
                10,
                "Timed out waiting for failure-table record for invalid ADD event with null book"
        );

        LibraryEventConsumerFailure failure = failureRepository.findAll().getFirst();
        assertEquals("library-events", failure.getTopic());
        assertTrue(failure.getExceptionClass().contains("ListenerExecutionFailedException"),
                "Expected ListenerExecutionFailedException, got: " + failure.getExceptionClass());
        // The full stack trace includes the cause chain, which contains "Validation failed:"
        assertNotNull(failure.getStackTrace());
        assertTrue(failure.getStackTrace().contains("Validation failed:"),
                "Expected stack trace to contain 'Validation failed:', got: " + failure.getStackTrace());
        assertNotNull(failure.getFailedAt());
    }

    @Test
    void consumeLibraryEvent_UPDATE_withNullLibraryEventId_shouldNotUpdateExistingRecord() throws Exception {
        // given — ADD a valid event first, then send an UPDATE with null libraryEventId
        BookDto addBookDto = new BookDto(102L, "Refactoring", "Martin Fowler");
        LibraryEventDto addDto = new LibraryEventDto(302L, EventType.ADD, addBookDto);
        kafkaTemplate.send("library-events", addDto).get(10, TimeUnit.SECONDS);
        waitForRecordCount(1, 10);

        String originalBookName = bookRepository.findById(102).orElseThrow().getBookName();

        // when — send invalid UPDATE: libraryEventId is null → fails @NotNull → non-retryable
        BookDto invalidUpdateBookDto = new BookDto(102L, "Refactoring 2nd Edition", "Martin Fowler");
        LibraryEventDto invalidUpdateDto = new LibraryEventDto(null, EventType.UPDATE, invalidUpdateBookDto);
        kafkaTemplate.send("library-events", invalidUpdateDto).get(10, TimeUnit.SECONDS);

        // then — data must remain unchanged throughout the 3-second observation window
        assertConditionRemainsTrue(
                () -> libraryEventRepository.count() == 1
                        && bookRepository.count() == 1
                        && libraryEventRepository.findById(302)
                        .map(e -> e.getEventType() == LibraryEventType.ADD)
                        .orElse(false)
                        && originalBookName.equals(
                                bookRepository.findById(102).map(Book::getBookName).orElse(null)),
                3,
                "Invalid UPDATE with null libraryEventId unexpectedly changed persisted data"
        );
    }

    // --- Helpers ---

    private void waitForRecordCount(long expectedCount, int timeoutSeconds) throws InterruptedException {
        waitForCondition(
                () -> libraryEventRepository.count() >= expectedCount,
                timeoutSeconds,
                "Timed out waiting for " + expectedCount + " record(s), found " + libraryEventRepository.count()
        );
    }

    /**
     * Asserts that {@code condition} remains {@code true} for the entire {@code durationSeconds}
     * window, polling every 100 ms. Fails immediately if the condition is violated.
     */
    private void assertConditionRemainsTrue(BooleanSupplier condition,
                                            int durationSeconds,
                                            String failureMessage) throws InterruptedException {
        long deadline = System.currentTimeMillis() + (long) durationSeconds * 1000;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(condition.getAsBoolean(), failureMessage);
            Thread.sleep(100);
        }
    }

    private KafkaConsumer<byte[], String> buildDltConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-verification-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(props);
    }

    private ConsumerRecords<byte[], String> pollUntilNonEmpty(KafkaConsumer<byte[], String> consumer,
                                                              int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + (long) timeoutSeconds * 1000;
        ConsumerRecords<byte[], String> records;
        do {
            records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) return records;
        } while (System.currentTimeMillis() < deadline);
        return records;
    }

    private void waitForCondition(BooleanSupplier condition,
                                  int timeoutSeconds,
                                  String failureMessage) throws InterruptedException {
        for (int i = 0; i < timeoutSeconds * 10; i++) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(100);
        }
        fail(failureMessage);
    }
}
