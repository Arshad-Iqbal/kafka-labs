package com.arshad.service;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.Book;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import com.arshad.producer.LibraryEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("LibraryEventService delegation tests")
class LibraryEventServiceImplTest {

    @Mock
    private LibraryEventProducer libraryEventProducer;

    @InjectMocks
    private LibraryEventServiceImpl libraryEventService;

    private LibraryEvent validAddEvent;
    private LibraryEvent validUpdateEvent;

    @BeforeEach
    void setUp() {
        validAddEvent = new LibraryEvent(101L, EventType.ADD, new Book(1L, "Test Book", "Test Author"));
        validUpdateEvent = new LibraryEvent(102L, EventType.UPDATE, new Book(2L, "Updated Book", "Updated Author"));
    }

    @Test
    void createLibraryEvent_setsEventTypeDelegatesAndReturnsEvent() {
        when(libraryEventProducer.sendLibraryEvent(validAddEvent)).thenReturn(successFuture());

        LibraryEvent result = libraryEventService.createLibraryEvent(validAddEvent).join();

        assertSame(validAddEvent, result);
        assertEquals(EventType.ADD, result.getEventType());
        verify(libraryEventProducer, times(1)).sendLibraryEvent(validAddEvent);
    }

    @Test
    void updateLibraryEvent_setsEventTypeDelegatesAndReturnsEvent() {
        when(libraryEventProducer.sendLibraryEvent(validUpdateEvent)).thenReturn(successFuture());

        LibraryEvent result = libraryEventService.updateLibraryEvent(validUpdateEvent).join();

        assertSame(validUpdateEvent, result);
        assertEquals(EventType.UPDATE, result.getEventType());
        verify(libraryEventProducer, times(1)).sendLibraryEvent(validUpdateEvent);
    }

    @Test
    void createLibraryEvent_throwsForNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> libraryEventService.createLibraryEvent(null));
        verify(libraryEventProducer, never()).sendLibraryEvent(any());
    }

    @Test
    void updateLibraryEvent_throwsForNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> libraryEventService.updateLibraryEvent(null));
        verify(libraryEventProducer, never()).sendLibraryEvent(any());
    }

    @Test
    void createLibraryEvent_wrapsProducerPublishException() {
        when(libraryEventProducer.sendLibraryEvent(validAddEvent))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("producer failed")));

        CompletableFuture<LibraryEvent> result = libraryEventService.createLibraryEvent(validAddEvent);

        CompletionException ex = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(LibraryEventPublishException.class, ex.getCause());
        verify(libraryEventProducer, times(1)).sendLibraryEvent(validAddEvent);
    }

    @Test
    void updateLibraryEvent_wrapsProducerPublishException() {
        when(libraryEventProducer.sendLibraryEvent(validUpdateEvent))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("producer failed")));

        CompletableFuture<LibraryEvent> result = libraryEventService.updateLibraryEvent(validUpdateEvent);

        CompletionException ex = assertThrows(CompletionException.class, result::join);
        assertInstanceOf(LibraryEventPublishException.class, ex.getCause());
        verify(libraryEventProducer, times(1)).sendLibraryEvent(validUpdateEvent);
    }

    private CompletableFuture<SendResult<Long, LibraryEvent>> successFuture() {
        return CompletableFuture.completedFuture(null);
    }
}
