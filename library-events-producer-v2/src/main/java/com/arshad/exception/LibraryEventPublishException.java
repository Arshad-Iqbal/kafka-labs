package com.arshad.exception;

/**
 * Unchecked exception thrown when a {@link LibraryEvent} cannot be published to Kafka.
 *
 * <p>Wraps lower-level exceptions (e.g. {@link java.util.concurrent.ExecutionException},
 * {@link java.util.concurrent.TimeoutException}) so callers deal with a single, domain-specific
 * exception type regardless of the underlying failure cause.
 */
public class LibraryEventPublishException extends RuntimeException {

    /**
     * Constructs a new exception with the given detail message and no cause.
     *
     * @param message human-readable description of the failure
     */
    public LibraryEventPublishException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the given detail message and root cause.
     *
     * @param message human-readable description of the failure
     * @param cause   the underlying exception that triggered this failure
     */
    public LibraryEventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
