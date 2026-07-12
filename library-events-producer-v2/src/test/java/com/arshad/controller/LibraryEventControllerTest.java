package com.arshad.controller;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.Book;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import com.arshad.service.LibraryEventService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("LibraryEventController Unit Tests")
class LibraryEventControllerTest {

    @Mock
    private LibraryEventService libraryEventService;

    @InjectMocks
    private LibraryEventController libraryEventController;

    private LibraryEvent validLibraryEvent;
    private Book validBook;

    @BeforeEach
    void setUp() {
        validBook = new Book(1L, "Test Book", "Test Author");
        validLibraryEvent = new LibraryEvent(100L, EventType.ADD, validBook);
    }

    @Test
    @DisplayName("POST with valid ADD event returns 201 Created")
    void testCreateLibraryEventValid() {
        validLibraryEvent.setEventType(EventType.ADD);
        when(libraryEventService.createLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(validLibraryEvent));

        ResponseEntity<LibraryEvent> response = libraryEventController
                .createLibraryEvent(validLibraryEvent).join();

        assertNotNull(response);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getHeaders().getLocation());
        assertEquals("/v1/library-events/100", response.getHeaders().getLocation().toString());
        assertEquals(validLibraryEvent, response.getBody());
        verify(libraryEventService, times(1)).createLibraryEvent(any(LibraryEvent.class));
    }

    @Test
    @DisplayName("POST delegates to service.createLibraryEvent")
    void testCreateLibraryEventDelegation() {
        when(libraryEventService.createLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(validLibraryEvent));

        libraryEventController.createLibraryEvent(validLibraryEvent).join();

        verify(libraryEventService, times(1)).createLibraryEvent(validLibraryEvent);
    }

    @Test
    @DisplayName("POST with service failure propagates exception via future")
    void testCreateLibraryEventServiceException() {
        when(libraryEventService.createLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new LibraryEventPublishException("Failed to publish ADD event", new RuntimeException("Kafka error"))));

        CompletableFuture<ResponseEntity<LibraryEvent>> future = libraryEventController.createLibraryEvent(validLibraryEvent);

        CompletionException ex = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(LibraryEventPublishException.class, ex.getCause());
        verify(libraryEventService, times(1)).createLibraryEvent(any(LibraryEvent.class));
    }

    @Test
    @DisplayName("PUT with valid UPDATE event returns 200 OK")
    void testUpdateLibraryEventValid() {
        validLibraryEvent.setEventType(EventType.UPDATE);
        when(libraryEventService.updateLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(validLibraryEvent));

        ResponseEntity<LibraryEvent> response = libraryEventController
                .updateLibraryEvent(validLibraryEvent).join();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(validLibraryEvent, response.getBody());
        verify(libraryEventService, times(1)).updateLibraryEvent(any(LibraryEvent.class));
    }

    @Test
    @DisplayName("PUT delegates to service.updateLibraryEvent")
    void testUpdateLibraryEventDelegation() {
        validLibraryEvent.setEventType(EventType.UPDATE);
        when(libraryEventService.updateLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(validLibraryEvent));

        libraryEventController.updateLibraryEvent(validLibraryEvent).join();

        verify(libraryEventService, times(1)).updateLibraryEvent(validLibraryEvent);
    }

    @Test
    @DisplayName("PUT with service failure propagates exception via future")
    void testUpdateLibraryEventServiceException() {
        validLibraryEvent.setEventType(EventType.UPDATE);
        when(libraryEventService.updateLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new LibraryEventPublishException("Failed to publish UPDATE event", new RuntimeException("Kafka error"))));

        CompletableFuture<ResponseEntity<LibraryEvent>> future = libraryEventController.updateLibraryEvent(validLibraryEvent);

        CompletionException ex = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(LibraryEventPublishException.class, ex.getCause());
        verify(libraryEventService, times(1)).updateLibraryEvent(any(LibraryEvent.class));
    }

    @Test
    @DisplayName("POST returns event in response body")
    void testCreateLibraryEventResponseBody() {
        when(libraryEventService.createLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(validLibraryEvent));

        ResponseEntity<LibraryEvent> response = libraryEventController
                .createLibraryEvent(validLibraryEvent).join();

        assertNotNull(response.getBody());
        assertEquals(100L, response.getBody().getLibraryEventId());
        assertEquals(EventType.ADD, response.getBody().getEventType());
        assertEquals(1L, response.getBody().getBook().getBookId());
        assertEquals("Test Book", response.getBody().getBook().getBookName());
        assertEquals("Test Author", response.getBody().getBook().getBookAuthor());
    }

    @Test
    @DisplayName("PUT returns event in response body")
    void testUpdateLibraryEventResponseBody() {
        validLibraryEvent.setEventType(EventType.UPDATE);
        when(libraryEventService.updateLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(validLibraryEvent));

        ResponseEntity<LibraryEvent> response = libraryEventController
                .updateLibraryEvent(validLibraryEvent).join();

        assertNotNull(response.getBody());
        assertEquals(100L, response.getBody().getLibraryEventId());
        assertEquals(EventType.UPDATE, response.getBody().getEventType());
        assertEquals(1L, response.getBody().getBook().getBookId());
    }

    @Test
    @DisplayName("POST with UPDATE event type throws IllegalArgumentException")
    void testCreateLibraryEvent_rejectsUpdateEventType() {
        validLibraryEvent.setEventType(EventType.UPDATE);

        assertThrows(IllegalArgumentException.class,
                () -> libraryEventController.createLibraryEvent(validLibraryEvent));
        verify(libraryEventService, never()).createLibraryEvent(any());
    }

    @Test
    @DisplayName("PUT with ADD event type throws IllegalArgumentException")
    void testUpdateLibraryEvent_rejectsAddEventType() {
        validLibraryEvent.setEventType(EventType.ADD);

        assertThrows(IllegalArgumentException.class,
                () -> libraryEventController.updateLibraryEvent(validLibraryEvent));
        verify(libraryEventService, never()).updateLibraryEvent(any());
    }
}