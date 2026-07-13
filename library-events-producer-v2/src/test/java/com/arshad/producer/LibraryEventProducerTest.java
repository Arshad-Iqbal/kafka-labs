package com.arshad.producer;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.Book;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("LibraryEventProducer Tests")
class LibraryEventProducerTest {

    @Mock
    private KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    @InjectMocks
    private LibraryEventProducer libraryEventProducer;

    private LibraryEvent event;
    private Book book;

    @BeforeEach
    void setUp() {
        book = new Book(1L, "Effective Java", "Joshua Bloch");
        event = new LibraryEvent(101L, EventType.ADD, book);
        ReflectionTestUtils.setField(libraryEventProducer, "topicName", "test-library-events");
    }

    @Nested
    @DisplayName("Asynchronous Publishing Tests")
    class AsyncPublishingTests {

        /**
         * Verifies that sendLibraryEvent() successfully enqueues the message and returns a completed,
         * non-exceptional CompletableFuture when the broker acknowledges the send.
         */
        @Test
        @DisplayName("should send message asynchronously and return completed future")
        void sendLibraryEvent_sendsMessageAsync() {
            when(kafkaTemplate.send(any(Message.class))).thenReturn(successFuture());

            CompletableFuture<SendResult<Long, LibraryEvent>> result = libraryEventProducer.sendLibraryEvent(event);

            assertNotNull(result);
            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEvent() returns a failed CompletableFuture when the broker rejects
         * the message, allowing callers to handle the failure asynchronously without blocking.
         */
        @Test
        @DisplayName("should handle async broker rejection with failed future")
        void sendLibraryEvent_asyncFailureReturnsFailedFuture() {
            when(kafkaTemplate.send(any(Message.class))).thenReturn(failedFuture());

            CompletableFuture<SendResult<Long, LibraryEvent>> result = libraryEventProducer.sendLibraryEvent(event);

            assertNotNull(result);
            assertTrue(result.isCompletedExceptionally());
            assertThrows(CompletionException.class, result::join);
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEvent() wraps immediate KafkaTemplate failures (e.g., serialization errors)
         * in a LibraryEventPublishException before the message reaches the broker.
         */
        @Test
        @DisplayName("should wrap immediate producer failure in LibraryEventPublishException")
        void sendLibraryEvent_wrapsImmediateProducerFailure() {
            when(kafkaTemplate.send(any(Message.class))).thenThrow(new RuntimeException("kafka down"));

            LibraryEventPublishException exception = assertThrows(
                    LibraryEventPublishException.class,
                    () -> libraryEventProducer.sendLibraryEvent(event)
            );

            assertTrue(exception.getMessage().contains("Failed to enqueue ADD event"));
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEvent() correctly sets all Kafka message headers (topic, message key,
         * and event type) before publishing, ensuring proper message routing and identification.
         */
        @Test
        @DisplayName("should set correct message headers for async publishing")
        void sendLibraryEvent_setsCorrectMessageHeaders() {
            when(kafkaTemplate.send(any(Message.class))).thenReturn(successFuture());
            ArgumentCaptor<Message<LibraryEvent>> messageCaptor = ArgumentCaptor.forClass(Message.class);

            libraryEventProducer.sendLibraryEvent(event);

            verify(kafkaTemplate).send(messageCaptor.capture());
            Message<LibraryEvent> capturedMessage = messageCaptor.getValue();
            
            assertEquals("test-library-events", capturedMessage.getHeaders().get("kafka_topic"));
            assertEquals(101L, capturedMessage.getHeaders().get("kafka_messageKey"));
            assertEquals("ADD", capturedMessage.getHeaders().get("eventType"));
            assertEquals(event, capturedMessage.getPayload());
        }

        /**
         * Verifies that sendLibraryEvent() correctly handles UPDATE event types asynchronously,
         * ensuring both ADD and UPDATE events are supported equally.
         */
        @Test
        @DisplayName("should publish UPDATE event type asynchronously")
        void sendLibraryEvent_publishesUpdateEvent() {
            LibraryEvent updateEvent = new LibraryEvent(102L, EventType.UPDATE, 
                    new Book(2L, "Java Concurrency", "Brian Goetz"));
            when(kafkaTemplate.send(any(Message.class))).thenReturn(successFuture());

            CompletableFuture<SendResult<Long, LibraryEvent>> result = libraryEventProducer.sendLibraryEvent(updateEvent);

            assertTrue(result.isDone());
            assertFalse(result.isCompletedExceptionally());
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }
    }

    @Nested
    @DisplayName("Synchronous Publishing Tests")
    class SyncPublishingTests {

        /**
         * Verifies that sendLibraryEventSynchronous() successfully publishes a message and returns
         * the SendResult with partition and offset metadata from the broker.
         */
        @Test
        @DisplayName("should publish message synchronously and return SendResult")
        void sendLibraryEventSynchronous_succeeds() throws Exception {
            SendResult<Long, LibraryEvent> expectedResult = createMockSendResult(0, 100L);
            when(kafkaTemplate.send(any(Message.class))).thenReturn(completedFuture(expectedResult));

            SendResult<Long, LibraryEvent> result = libraryEventProducer.sendLibraryEventSynchronous(event);

            assertNotNull(result);
            assertEquals(0, result.getRecordMetadata().partition());
            assertEquals(100L, result.getRecordMetadata().offset());
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEventSynchronous() validates that the event parameter is not null
         * before attempting to publish, preventing null pointer exceptions downstream.
         */
        @Test
        @DisplayName("should throw exception when event is null")
        void sendLibraryEventSynchronous_throwsExceptionForNullEvent() {
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> libraryEventProducer.sendLibraryEventSynchronous(null)
            );

            assertEquals("Event cannot be null", exception.getMessage());
            verify(kafkaTemplate, never()).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEventSynchronous() validates that libraryEventId is not null,
         * rejecting events without unique identifiers before message construction.
         */
        @Test
        @DisplayName("should throw exception when libraryEventId is null")
        void sendLibraryEventSynchronous_throwsExceptionForNullLibraryEventId() {
            event.setLibraryEventId(null);

            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> libraryEventProducer.sendLibraryEventSynchronous(event)
            );

            assertEquals("Library event ID cannot be null", exception.getMessage());
            verify(kafkaTemplate, never()).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEventSynchronous() validates that eventType is not null,
         * ensuring only well-defined event types (ADD/UPDATE) are published to Kafka.
         */
        @Test
        @DisplayName("should throw exception when eventType is null")
        void sendLibraryEventSynchronous_throwsExceptionForNullEventType() {
            event.setEventType(null);

            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> libraryEventProducer.sendLibraryEventSynchronous(event)
            );

            assertEquals("Event type cannot be null", exception.getMessage());
            verify(kafkaTemplate, never()).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEventSynchronous() catches ExecutionException from the blocking
         * get() call and wraps it in LibraryEventPublishException for consistent error handling.
         */
        @Test
        @DisplayName("should wrap ExecutionException as LibraryEventPublishException")
        void sendLibraryEventSynchronous_wrapsExecutionException() throws Exception {
            ExecutionException executionException = new ExecutionException("Broker error", new RuntimeException("Unexpected error"));
            CompletableFuture<SendResult<Long, LibraryEvent>> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(executionException);
            when(kafkaTemplate.send(any(Message.class))).thenReturn(failedFuture);

            LibraryEventPublishException exception = assertThrows(
                    LibraryEventPublishException.class,
                    () -> libraryEventProducer.sendLibraryEventSynchronous(event)
            );

            assertTrue(exception.getMessage().contains("Failed to publish ADD event synchronously"));
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEventSynchronous() properly handles thread interruption by
         * wrapping the exception and restoring the interrupt flag for downstream cleanup.
         */
        @Test
        @DisplayName("should wrap InterruptedException and restore interrupt flag")
        void sendLibraryEventSynchronous_wrapsInterruptedException() throws Exception {
            CompletableFuture<SendResult<Long, LibraryEvent>> future = new CompletableFuture<>();
            when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

            // Simulate what happens when .get() is interrupted
            Thread testThread = Thread.currentThread();
            new Thread(() -> {
                try {
                    Thread.sleep(100);
                    testThread.interrupt();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();

            LibraryEventPublishException exception = assertThrows(
                    LibraryEventPublishException.class,
                    () -> libraryEventProducer.sendLibraryEventSynchronous(event)
            );

            assertTrue(exception.getMessage().contains("Interrupted while publishing ADD event"));
            assertTrue(Thread.currentThread().isInterrupted());
            Thread.interrupted();
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEventSynchronous() times out gracefully when the broker does not
         * acknowledge within 3 seconds, wrapping the timeout in LibraryEventPublishException.
         */
        @Test
        @DisplayName("should wrap TimeoutException when 3-second timeout is exceeded")
        void sendLibraryEventSynchronous_wrapsTimeoutException() throws Exception {
            CompletableFuture<SendResult<Long, LibraryEvent>> future = new CompletableFuture<>();
            // Never complete the future to simulate timeout
            when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

            LibraryEventPublishException exception = assertThrows(
                    LibraryEventPublishException.class,
                    () -> libraryEventProducer.sendLibraryEventSynchronous(event)
            );

            assertTrue(exception.getMessage().contains("Timed out publishing ADD event"));
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }

        /**
         * Verifies that sendLibraryEventSynchronous() correctly sets all Kafka message headers
         * (topic, message key, and event type) before the blocking send, ensuring proper message routing.
         */
        @Test
        @DisplayName("should set correct message headers for synchronous publishing")
        void sendLibraryEventSynchronous_setsCorrectMessageHeaders() throws Exception {
            SendResult<Long, LibraryEvent> expectedResult = createMockSendResult(1, 200L);
            when(kafkaTemplate.send(any(Message.class))).thenReturn(completedFuture(expectedResult));
            ArgumentCaptor<Message<LibraryEvent>> messageCaptor = ArgumentCaptor.forClass(Message.class);

            libraryEventProducer.sendLibraryEventSynchronous(event);

            verify(kafkaTemplate).send(messageCaptor.capture());
            Message<LibraryEvent> capturedMessage = messageCaptor.getValue();
            
            assertEquals("test-library-events", capturedMessage.getHeaders().get("kafka_topic"));
            assertEquals(101L, capturedMessage.getHeaders().get("kafka_messageKey"));
            assertEquals("ADD", capturedMessage.getHeaders().get("eventType"));
            assertEquals(event, capturedMessage.getPayload());
        }

        /**
         * Verifies that sendLibraryEventSynchronous() correctly handles UPDATE event types synchronously,
         * ensuring both ADD and UPDATE events are supported equally in blocking mode.
         */
        @Test
        @DisplayName("should publish UPDATE event type synchronously")
        void sendLibraryEventSynchronous_publishesUpdateEvent() throws Exception {
            LibraryEvent updateEvent = new LibraryEvent(103L, EventType.UPDATE, 
                    new Book(3L, "The Pragmatic Programmer", "David Thomas"));
            SendResult<Long, LibraryEvent> expectedResult = createMockSendResult(2, 300L);
            when(kafkaTemplate.send(any(Message.class))).thenReturn(completedFuture(expectedResult));

            SendResult<Long, LibraryEvent> result = libraryEventProducer.sendLibraryEventSynchronous(updateEvent);

            assertNotNull(result);
            assertEquals(2, result.getRecordMetadata().partition());
            assertEquals(300L, result.getRecordMetadata().offset());
            verify(kafkaTemplate, times(1)).send(any(Message.class));
        }
    }

    private CompletableFuture<SendResult<Long, LibraryEvent>> successFuture() {
        CompletableFuture<SendResult<Long, LibraryEvent>> future = new CompletableFuture<>();
        future.complete(null);
        return future;
    }

    private CompletableFuture<SendResult<Long, LibraryEvent>> failedFuture() {
        CompletableFuture<SendResult<Long, LibraryEvent>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Kafka publish failed"));
        return future;
    }

    private <T> CompletableFuture<T> completedFuture(T value) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.complete(value);
        return future;
    }

    private SendResult<Long, LibraryEvent> createMockSendResult(int partition, long offset) {
        TopicPartition topicPartition = new TopicPartition("test-library-events", partition);
        RecordMetadata metadata = new RecordMetadata(
                topicPartition,
                offset,
                0,
                System.currentTimeMillis(),
                0,
                0
        );
        ProducerRecord<Long, LibraryEvent> producerRecord = new ProducerRecord<>(
                "test-library-events",
                101L,
                event
        );
        return new SendResult<>(producerRecord, metadata);
    }
}
