package com.learnkafka.consumer;

import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.model.BookDto;
import com.learnkafka.model.EventType;
import com.learnkafka.model.LibraryEventDto;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.util.backoff.FixedBackOff;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"library-events"},
        bootstrapServersProperty = "spring.kafka.consumer.bootstrap-servers"
)
@ImportTestcontainers
class LibraryEventsConsumerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @TestConfiguration
    static class TestConfig {
        // Skip failed records immediately (no retries) — keeps tests fast and prevents
        // failed messages from blocking the consumer between tests
        @Bean
        CommonErrorHandler testErrorHandler() {
            return new DefaultErrorHandler(new FixedBackOff(0L, 0L));
        }
    }

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    BookRepository bookRepository;

    KafkaTemplate<Long, LibraryEventDto> kafkaTemplate;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    @Test
    void consumeLibraryEvent_ADD_shouldPersistLibraryEventAndBook() throws Exception {
        // given — use id=100 to avoid conflicts with other tests' async Kafka processing
        BookDto bookDto = new BookDto(10L, "Kafka Fundamentals", "Arshad Iqbal");
        LibraryEventDto dto = new LibraryEventDto(100L, EventType.ADD, bookDto);

        // when
        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

        // then — allow up to 30s for async consumer to process and persist
        waitForRecordCount(1, 30);

        List<LibraryEvent> events = libraryEventRepository.findAll();
        assertEquals(1, events.size());
        LibraryEvent savedEvent = events.get(0);
        assertEquals(100, savedEvent.getLibraryEventId());
        assertEquals(LibraryEventType.ADD, savedEvent.getEventType());
        assertNotNull(savedEvent.getCreatedAt());
        assertNotNull(savedEvent.getUpdatedAt());

        List<Book> books = bookRepository.findAll();
        assertEquals(1, books.size());
        Book savedBook = books.get(0);
        assertEquals(10, savedBook.getBookId());
        assertEquals("Kafka Fundamentals", savedBook.getBookName());
        assertEquals("Arshad Iqbal", savedBook.getBookAuthor());
        assertEquals(savedEvent.getLibraryEventId(), savedBook.getLibraryEvent().getLibraryEventId());
    }

    @Test
    void consumeLibraryEvent_UPDATE_shouldUpdateExistingRecord() throws Exception {
        // given — use id=200 to avoid conflicts with other tests' async Kafka processing
        LibraryEvent existingEvent = LibraryEvent.builder()
                .libraryEventId(200)
                .eventType(LibraryEventType.ADD)
                .build();
        Book existingBook = Book.builder()
                .bookId(20)
                .bookName("Old Book Name")
                .bookAuthor("Old Author")
                .libraryEvent(existingEvent)
                .build();
        existingEvent.setBook(existingBook);
        libraryEventRepository.save(existingEvent);

        // when — send UPDATE event
        BookDto updatedBookDto = new BookDto(20L, "Kafka in Action", "Updated Author");
        LibraryEventDto dto = new LibraryEventDto(200L, EventType.UPDATE, updatedBookDto);
        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

        // then — wait for update to be applied
        Book updatedBook = waitForBookUpdate(20, "Kafka in Action", 30);

        assertEquals("Kafka in Action", updatedBook.getBookName());
        assertEquals("Updated Author", updatedBook.getBookAuthor());
        assertEquals(1, libraryEventRepository.count()); // no new event created
        assertEquals(1, bookRepository.count());         // no new book created
    }

    @Test
    void consumeLibraryEvent_withNullBook_shouldNotPersistAnyRecord() throws Exception {
        // given — book is null (invalid event)
        LibraryEventDto dto = new LibraryEventDto(1L, EventType.ADD, null);

        // when
        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

        // then — allow time for consumer to attempt processing, assert nothing persisted
        Thread.sleep(3000);

        assertEquals(0, libraryEventRepository.count());
        assertEquals(0, bookRepository.count());
    }

    @Test
    void consumeLibraryEvent_UPDATE_withNullLibraryEventId_shouldNotPersistAnyRecord() throws Exception {
        // given — libraryEventId is null for UPDATE (invalid event)
        BookDto bookDto = new BookDto(10L, "Kafka Fundamentals", "Arshad Iqbal");
        LibraryEventDto dto = new LibraryEventDto(null, EventType.UPDATE, bookDto);

        // when
        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

        // then — allow time for consumer to attempt processing, assert nothing persisted
        Thread.sleep(3000);

        assertEquals(0, libraryEventRepository.count());
        assertEquals(0, bookRepository.count());
    }

    // --- Helpers ---

    private void waitForRecordCount(long expectedCount, int timeoutSeconds) throws InterruptedException {
        waitForCondition(
                () -> libraryEventRepository.count() >= expectedCount,
                timeoutSeconds,
                "Timed out waiting for " + expectedCount + " record(s), found " + libraryEventRepository.count()
        );
    }

    private Book waitForBookUpdate(Integer bookId, String expectedBookName, int timeoutSeconds)
            throws InterruptedException {
        waitForCondition(
                () -> bookRepository.findById(bookId)
                        .map(b -> expectedBookName.equals(b.getBookName()))
                        .orElse(false),
                timeoutSeconds,
                "Timed out waiting for book " + bookId + " to have name '" + expectedBookName + "'"
        );
        return bookRepository.findById(bookId).orElse(null);
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
