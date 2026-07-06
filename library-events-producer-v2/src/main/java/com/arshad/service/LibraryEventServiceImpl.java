package com.arshad.service;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import com.arshad.producer.LibraryEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LibraryEventServiceImpl implements LibraryEventService {
    
    private final LibraryEventProducer libraryEventProducer;

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
