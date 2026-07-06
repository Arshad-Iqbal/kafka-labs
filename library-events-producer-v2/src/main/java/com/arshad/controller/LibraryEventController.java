package com.arshad.controller;

import com.arshad.model.LibraryEvent;
import com.arshad.service.LibraryEventService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;

@RestController
@RequestMapping("/v1/library-events")
@RequiredArgsConstructor
@Slf4j
public class LibraryEventController {

    private final LibraryEventService libraryEventService;

    @PostMapping
    public ResponseEntity<LibraryEvent> postLibraryEvent(@Valid @RequestBody LibraryEvent event) {
        log.info("POST request received to create library event: {}", event);
        LibraryEvent publishedEvent = libraryEventService.createLibraryEvent(event);
        
        URI location = URI.create("/v1/library-events/" + publishedEvent.getLibraryEventId());
        log.info("Library event created successfully with ID: {}", publishedEvent.getLibraryEventId());
        
        return ResponseEntity.created(location).body(publishedEvent);
    }

    @PutMapping
    public ResponseEntity<LibraryEvent> putLibraryEvent(@Valid @RequestBody LibraryEvent event) {
        log.info("PUT request received to update library event: {}", event);
        LibraryEvent publishedEvent = libraryEventService.updateLibraryEvent(event);
        
        log.info("Library event updated successfully with ID: {}", publishedEvent.getLibraryEventId());
        
        return ResponseEntity.ok(publishedEvent);
    }
}
