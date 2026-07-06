package com.arshad.service;

import com.arshad.model.LibraryEvent;

public interface LibraryEventService {
    
    /**
     * Publishes an ADD event for a library event.
     * 
     * @param event the library event to publish
     * @return the published library event
     */
    LibraryEvent createLibraryEvent(LibraryEvent event);
    
    /**
     * Publishes an UPDATE event for a library event.
     * 
     * @param event the library event to publish
     * @return the published library event
     */
    LibraryEvent updateLibraryEvent(LibraryEvent event);
}
