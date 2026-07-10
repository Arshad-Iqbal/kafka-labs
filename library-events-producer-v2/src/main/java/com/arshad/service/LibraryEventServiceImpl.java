package com.arshad.service;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import com.arshad.producer.LibraryEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * Default implementation of {@link LibraryEventService}.
 *
 * <p>Sets the appropriate {@link com.arshad.model.EventType} on the event before delegating
 * to {@link LibraryEventProducer} for asynchronous Kafka publishing.
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
    public LibraryEvent createLibraryEvent(LibraryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        event.setEventType(EventType.ADD);
        try {
            libraryEventProducer.sendLibraryEvent(event);
        } catch (LibraryEventPublishException ex) {
            throw new LibraryEventPublishException("Unable to publish ADD library event", ex);
        }
        return event;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Sets the event type to {@code UPDATE} before publishing.
     */
    @Override
    public LibraryEvent updateLibraryEvent(LibraryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        event.setEventType(EventType.UPDATE);
        try {
            libraryEventProducer.sendLibraryEvent(event);
        } catch (LibraryEventPublishException ex) {
            throw new LibraryEventPublishException("Unable to publish UPDATE library event", ex);
        }
        return event;
    }
}
