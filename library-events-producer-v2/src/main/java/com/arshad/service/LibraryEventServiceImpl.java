package com.arshad.service;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import com.arshad.producer.LibraryEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link LibraryEventService}.
 *
 * <p>Sets the appropriate {@link com.arshad.model.EventType} on the event before delegating
 * to {@link LibraryEventProducer} for asynchronous Kafka publishing. The {@code CompletableFuture}
 * returned by the producer is propagated to callers, keeping the entire pipeline non-blocking.
 */
@Service
@RequiredArgsConstructor
public class LibraryEventServiceImpl implements LibraryEventService {

    private final LibraryEventProducer libraryEventProducer;

    /**
     * {@inheritDoc}
     *
     * <p>Sets the event type to {@code ADD} before publishing.
     */
    @Override
    public CompletableFuture<LibraryEvent> createLibraryEvent(LibraryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        event.setEventType(EventType.ADD);
        return libraryEventProducer.sendLibraryEvent(event)
                .thenApply(result -> event)
                .exceptionally(ex -> {
                    throw new LibraryEventPublishException("Unable to publish ADD library event", ex.getCause());
                });
    }

    /**
     * {@inheritDoc}
     *
     * <p>Sets the event type to {@code UPDATE} before publishing.
     */
    @Override
    public CompletableFuture<LibraryEvent> updateLibraryEvent(LibraryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        event.setEventType(EventType.UPDATE);
        return libraryEventProducer.sendLibraryEvent(event)
                .thenApply(result -> event)
                .exceptionally(ex -> {
                    throw new LibraryEventPublishException("Unable to publish UPDATE library event", ex.getCause());
                });
    }
}
