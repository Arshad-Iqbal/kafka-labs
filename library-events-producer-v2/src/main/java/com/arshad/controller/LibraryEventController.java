package com.arshad.controller;

import com.arshad.model.LibraryEvent;
import com.arshad.service.LibraryEventService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;

/**
 * REST controller exposing endpoints to create and update library events.
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
     * @return {@code 201 Created} with the published event and a {@code Location} header
     */
    @PostMapping
    public ResponseEntity<LibraryEvent> postLibraryEvent(@Valid @RequestBody LibraryEvent event) {
        log.info("POST request received to create library event: {}", event);
        LibraryEvent publishedEvent = libraryEventService.createLibraryEvent(event);
        
        URI location = URI.create("/v1/library-events/" + publishedEvent.getLibraryEventId());
        log.info("Library event created successfully with ID: {}", publishedEvent.getLibraryEventId());
        
        return ResponseEntity.created(location).body(publishedEvent);
    }

    /**
     * Updates an existing library event and publishes it to Kafka with event type {@code UPDATE}.
     *
     * @param event the library event to update; must be a valid {@link LibraryEvent}
     * @return {@code 200 OK} with the published event
     */
    @PutMapping
    public ResponseEntity<LibraryEvent> putLibraryEvent(@Valid @RequestBody LibraryEvent event) {
        log.info("PUT request received to update library event: {}", event);
        LibraryEvent publishedEvent = libraryEventService.updateLibraryEvent(event);
        
        log.info("Library event updated successfully with ID: {}", publishedEvent.getLibraryEventId());
        
        return ResponseEntity.ok(publishedEvent);
    }
}
