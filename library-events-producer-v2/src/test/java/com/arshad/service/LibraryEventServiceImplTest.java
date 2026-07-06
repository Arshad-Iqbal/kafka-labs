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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

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
        LibraryEvent result = libraryEventService.createLibraryEvent(validAddEvent);

        assertSame(validAddEvent, result);
        assertEquals(EventType.ADD, result.getEventType());
        verify(libraryEventProducer, times(1)).sendLibraryEvent(validAddEvent);
    }

    @Test
    void updateLibraryEvent_setsEventTypeDelegatesAndReturnsEvent() {
        LibraryEvent result = libraryEventService.updateLibraryEvent(validUpdateEvent);

        assertSame(validUpdateEvent, result);
        assertEquals(EventType.UPDATE, result.getEventType());
        verify(libraryEventProducer, times(1)).sendLibraryEvent(validUpdateEvent);
    }

    @Test
    void createLibraryEvent_throwsForNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> libraryEventService.createLibraryEvent(null));
        verify(libraryEventProducer, never()).sendLibraryEvent(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void updateLibraryEvent_throwsForNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> libraryEventService.updateLibraryEvent(null));
        verify(libraryEventProducer, never()).sendLibraryEvent(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void createLibraryEvent_wrapsProducerPublishException() {
        doThrow(new LibraryEventPublishException("producer failed"))
                .when(libraryEventProducer).sendLibraryEvent(validAddEvent);

        assertThrows(LibraryEventPublishException.class, () -> libraryEventService.createLibraryEvent(validAddEvent));
        verify(libraryEventProducer, times(1)).sendLibraryEvent(validAddEvent);
    }

    @Test
    void updateLibraryEvent_wrapsProducerPublishException() {
        doThrow(new LibraryEventPublishException("producer failed"))
                .when(libraryEventProducer).sendLibraryEvent(validUpdateEvent);

        assertThrows(LibraryEventPublishException.class, () -> libraryEventService.updateLibraryEvent(validUpdateEvent));
        verify(libraryEventProducer, times(1)).sendLibraryEvent(validUpdateEvent);
    }
}
