package com.arshad.producer;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.Book;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @Mock
    private KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    @InjectMocks
    private LibraryEventProducer libraryEventProducer;

    private LibraryEvent event;

    @BeforeEach
    void setUp() {
        event = new LibraryEvent(101L, EventType.ADD, new Book(1L, "Book", "Author"));
        ReflectionTestUtils.setField(libraryEventProducer, "topicName", "test-library-events");
    }

    @Test
    void sendLibraryEvent_sendsMessageAsync() {
        when(kafkaTemplate.send(any(Message.class))).thenReturn(successFuture());

        CompletableFuture<SendResult<Long, LibraryEvent>> result = libraryEventProducer.sendLibraryEvent(event);

        assertNotNull(result);
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        verify(kafkaTemplate, times(1)).send(any(Message.class));
    }

    @Test
    void sendLibraryEvent_asyncFailureReturnsFailedFuture() {
        when(kafkaTemplate.send(any(Message.class))).thenReturn(failedFuture());

        CompletableFuture<SendResult<Long, LibraryEvent>> result = libraryEventProducer.sendLibraryEvent(event);

        assertNotNull(result);
        assertTrue(result.isCompletedExceptionally());
        assertThrows(CompletionException.class, result::join);
        verify(kafkaTemplate, times(1)).send(any(Message.class));
    }

    @Test
    void sendLibraryEvent_throwsForNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> libraryEventProducer.sendLibraryEvent(null));
        verify(kafkaTemplate, never()).send(any(Message.class));
    }

    @Test
    void sendLibraryEvent_throwsForNullLibraryEventId() {
        LibraryEvent invalidEvent = new LibraryEvent(null, EventType.ADD, new Book(1L, "Book", "Author"));

        assertThrows(IllegalArgumentException.class, () -> libraryEventProducer.sendLibraryEvent(invalidEvent));
        verify(kafkaTemplate, never()).send(any(Message.class));
    }

    @Test
    void sendLibraryEvent_throwsForNullEventType() {
        LibraryEvent invalidEvent = new LibraryEvent(101L, null, new Book(1L, "Book", "Author"));

        assertThrows(IllegalArgumentException.class, () -> libraryEventProducer.sendLibraryEvent(invalidEvent));
        verify(kafkaTemplate, never()).send(any(Message.class));
    }

    @Test
    void sendLibraryEvent_wrapsImmediateProducerFailure() {
        when(kafkaTemplate.send(any(Message.class))).thenThrow(new RuntimeException("kafka down"));

        assertThrows(LibraryEventPublishException.class, () -> libraryEventProducer.sendLibraryEvent(event));
        verify(kafkaTemplate, times(1)).send(any(Message.class));
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
}
