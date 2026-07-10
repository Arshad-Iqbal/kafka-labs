package com.arshad.service;

import com.arshad.model.LibraryEvent;

import java.util.concurrent.CompletableFuture;

public interface LibraryEventService {

    /**
     * Publishes an ADD event for a library event.
     *
     * @param event the library event to publish
     * @return a {@code CompletableFuture} that completes with the published event on success,
     *         or completes exceptionally if publishing fails
     */
    CompletableFuture<LibraryEvent> createLibraryEvent(LibraryEvent event);

    /**
     * Publishes an UPDATE event for a library event.
     *
     * @param event the library event to publish
     * @return a {@code CompletableFuture} that completes with the published event on success,
     *         or completes exceptionally if publishing fails
     */
    CompletableFuture<LibraryEvent> updateLibraryEvent(LibraryEvent event);
}
