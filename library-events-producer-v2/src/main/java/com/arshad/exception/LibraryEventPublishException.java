package com.arshad.exception;

public class LibraryEventPublishException extends RuntimeException {

    public LibraryEventPublishException(String message) {
        super(message);
    }

    public LibraryEventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
