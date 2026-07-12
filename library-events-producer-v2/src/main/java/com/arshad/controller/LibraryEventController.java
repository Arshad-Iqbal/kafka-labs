package com.arshad.controller;

import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import com.arshad.service.LibraryEventService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * REST controller exposing endpoints to create and update library events.
 *
 * <p>Both endpoints return a {@code CompletableFuture}, enabling Spring MVC's async dispatch:
 * the Servlet thread is released immediately after the future is returned, and the HTTP response
 * is written once the Kafka broker acknowledges the message on the producer callback thread.
 *
 * <p>All requests are validated via Bean Validation ({@code @Valid}). Validation failures
 * and publish errors are handled globally by {@link com.arshad.exception.LibraryEventsControllerAdvice}.
 *
 * <p>Base path: {@code /v1/library-events}
 */
@RestController
@RequestMapping("/v1/library-events")
@RequiredArgsConstructor
@Slf4j
public class LibraryEventController {

    private final LibraryEventService libraryEventService;

    /**
     * Creates a new library event and publishes it to Kafka with event type {@code ADD}.
     *
     * @param event the library event to create; must be a valid {@link LibraryEvent}
     * @return a {@code CompletableFuture} that resolves to {@code 201 Created} with the
     *         published event and a {@code Location} header on success
     */
    @PostMapping
    public CompletableFuture<ResponseEntity<LibraryEvent>> createLibraryEvent(@Valid @RequestBody LibraryEvent event) {
        log.info("POST request received to create library event: {}", event);
        if (event.getEventType() != EventType.ADD) {
            throw new IllegalArgumentException(
                    "POST endpoint only accepts ADD events, but received: " + event.getEventType());
        }
        return libraryEventService.createLibraryEvent(event)
                .thenApply(publishedEvent -> {
                    URI location = URI.create("/v1/library-events/" + publishedEvent.getLibraryEventId());
                    log.info("Library event created successfully with ID: {}", publishedEvent.getLibraryEventId());
                    return ResponseEntity.created(location).body(publishedEvent);
                });
    }

    /**
     * Updates an existing library event and publishes it to Kafka with event type {@code UPDATE}.
     *
     * @param event the library event to update; must be a valid {@link LibraryEvent}
     * @return a {@code CompletableFuture} that resolves to {@code 200 OK} with the published event
     */
    @PutMapping
    public CompletableFuture<ResponseEntity<LibraryEvent>> updateLibraryEvent(@Valid @RequestBody LibraryEvent event) {
        log.info("PUT request received to update library event: {}", event);
        if (event.getEventType() != EventType.UPDATE) {
            throw new IllegalArgumentException(
                    "PUT endpoint only accepts UPDATE events, but received: " + event.getEventType());
        }
        return libraryEventService.updateLibraryEvent(event)
                .thenApply(publishedEvent -> {
                    log.info("Library event updated successfully with ID: {}", publishedEvent.getLibraryEventId());
                    return ResponseEntity.ok(publishedEvent);
                });
    }
}
